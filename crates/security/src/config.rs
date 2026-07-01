//! JSON security config → an assembled [`SecurityContext`].
//!
//! (The spec used TOML; JSON is used because `serde_json` is already a workspace
//! dependency. The schema is otherwise identical.)

use crate::credential::{CredentialConfig, CredentialHash};
use crate::identity::StaticIdentityProvider;
use crate::permission::{ActionSet, Permission, PermissionConfig, ResourceType};
use crate::principal::Principal;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Deserialize)]
struct SecurityConfigJson {
    principals: Vec<PrincipalJson>,
}

#[derive(Deserialize)]
struct PrincipalJson {
    name: String,
    #[serde(default)]
    admin: bool,
    credential: CredentialConfig,
    #[serde(default)]
    permissions: Vec<PermissionConfig>,
}

/// Errors from parsing/validating a security config.
#[derive(Debug)]
pub enum ConfigError {
    Json(String),
    BadCredential(String),
    BadPermission(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Json(s) => write!(f, "invalid security config JSON: {s}"),
            ConfigError::BadCredential(s) => write!(f, "invalid credential for {s}"),
            ConfigError::BadPermission(s) => write!(f, "invalid permission: {s}"),
        }
    }
}

/// Build a [`StaticIdentityProvider`] from JSON config text. Requires
/// authentication (there is no anonymous access once principals are defined).
pub fn load_provider(json: &str) -> Result<StaticIdentityProvider, ConfigError> {
    let cfg: SecurityConfigJson =
        serde_json::from_str(json).map_err(|e| ConfigError::Json(e.to_string()))?;

    let mut principals: HashMap<String, (CredentialHash, Arc<Principal>)> = HashMap::new();
    for pj in cfg.principals {
        let cred = pj
            .credential
            .resolve()
            .ok_or_else(|| ConfigError::BadCredential(pj.name.clone()))?;

        let mut grants = Vec::with_capacity(pj.permissions.len());
        for perm in &pj.permissions {
            let rt = ResourceType::parse(&perm.resource_type)
                .ok_or_else(|| ConfigError::BadPermission(perm.resource_type.clone()))?;
            let actions = ActionSet::parse_list(&perm.actions)
                .ok_or_else(|| ConfigError::BadPermission(format!("{:?}", perm.actions)))?;
            grants.push(Permission {
                resource_type: rt,
                name: perm.name.clone(),
                actions,
            });
        }

        let principal = Arc::new(Principal {
            name: pj.name.clone(),
            grants,
            is_admin: pj.admin,
        });
        principals.insert(pj.name.clone(), (cred, principal));
    }

    // With principals configured, unauthenticated access resolves to an empty
    // (deny-everything) anonymous principal, and auth is required.
    Ok(StaticIdentityProvider::new(
        principals,
        Arc::new(Principal::empty("anonymous")),
        true,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::{bytes_to_hex, hash_password, DEFAULT_ITERATIONS};
    use crate::identity::IdentityProvider;
    use crate::permission::{Action, ResourceType};

    fn cred_json(pw: &[u8]) -> String {
        let salt = [4u8; 16];
        let hash = hash_password(pw, &salt, DEFAULT_ITERATIONS);
        format!(
            r#"{{"salt_hex":"{}","hash_hex":"{}","iterations":{}}}"#,
            bytes_to_hex(&salt),
            bytes_to_hex(&hash),
            DEFAULT_ITERATIONS
        )
    }

    #[test]
    fn parses_principals_and_permissions() {
        let json = format!(
            r#"{{"principals":[
                {{"name":"app","credential":{app},
                  "permissions":[{{"resource_type":"map","name":"orders*","actions":["read"]}}]}},
                {{"name":"ops","admin":true,"credential":{ops}}}
            ]}}"#,
            app = cred_json(b"apppw"),
            ops = cred_json(b"opspw"),
        );
        let provider = load_provider(&json).unwrap();

        let app = provider.authenticate(Some("app"), Some("apppw")).unwrap();
        assert!(app.authorize(ResourceType::Map, "orders1", Action::Read));
        assert!(!app.authorize(ResourceType::Map, "orders1", Action::Put));
        assert!(!app.authorize(ResourceType::Map, "cart1", Action::Read));

        let ops = provider.authenticate(Some("ops"), Some("opspw")).unwrap();
        assert!(ops.is_admin);
        assert!(ops.authorize(ResourceType::Queue, "anything", Action::Put));

        assert!(provider.authenticate(Some("app"), Some("wrong")).is_none());
        assert!(provider.requires_auth());
    }

    #[test]
    fn rejects_bad_resource_type() {
        let json = format!(
            r#"{{"principals":[{{"name":"x","credential":{c},
                "permissions":[{{"resource_type":"nonsense","name":"*","actions":["read"]}}]}}]}}"#,
            c = cred_json(b"pw")
        );
        assert!(matches!(
            load_provider(&json),
            Err(ConfigError::BadPermission(_))
        ));
    }
}
