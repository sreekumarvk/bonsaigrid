//! Authentication: turn (username, password) into an authorized [`Principal`].
//!
//! The [`IdentityProvider`] trait is the seam for future backends (LDAP/JAAS);
//! v1 ships only [`StaticIdentityProvider`] built from config.

use crate::credential::CredentialHash;
use crate::principal::Principal;
use std::collections::HashMap;
use std::sync::Arc;

/// Resolves credentials to principals. `Send + Sync` so a single instance can be
/// shared immutably across the per-core reactor threads.
pub trait IdentityProvider: Send + Sync {
    /// Authenticate; `None` means rejected.
    fn authenticate(&self, user: Option<&str>, pass: Option<&str>) -> Option<Arc<Principal>>;
    /// The principal used when authentication is not required (dev/no-config).
    fn anonymous(&self) -> Arc<Principal>;
    /// Whether credentials are mandatory (a real principal must authenticate).
    fn requires_auth(&self) -> bool;
}

/// Config-backed provider: a fixed table of `name → (credential, principal)`.
pub struct StaticIdentityProvider {
    principals: HashMap<String, (CredentialHash, Arc<Principal>)>,
    anonymous: Arc<Principal>,
    require_auth: bool,
}

impl StaticIdentityProvider {
    pub fn new(
        principals: HashMap<String, (CredentialHash, Arc<Principal>)>,
        anonymous: Arc<Principal>,
        require_auth: bool,
    ) -> StaticIdentityProvider {
        StaticIdentityProvider {
            principals,
            anonymous,
            require_auth,
        }
    }

    /// The permissive open provider: no principals, anonymous authorizes all,
    /// auth not required. Used when no security config is present.
    pub fn open() -> StaticIdentityProvider {
        StaticIdentityProvider {
            principals: HashMap::new(),
            anonymous: Arc::new(Principal::anonymous_full()),
            require_auth: false,
        }
    }
}

impl IdentityProvider for StaticIdentityProvider {
    fn authenticate(&self, user: Option<&str>, pass: Option<&str>) -> Option<Arc<Principal>> {
        match user {
            // No username presented: allowed only when auth is not required,
            // and resolves to the anonymous principal.
            None => {
                if self.require_auth {
                    None
                } else {
                    Some(self.anonymous.clone())
                }
            }
            Some(name) => {
                let (cred, principal) = self.principals.get(name)?;
                if cred.verify(pass.unwrap_or("").as_bytes()) {
                    Some(principal.clone())
                } else {
                    None
                }
            }
        }
    }

    fn anonymous(&self) -> Arc<Principal> {
        self.anonymous.clone()
    }

    fn requires_auth(&self) -> bool {
        self.require_auth
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permission::{Action, ActionSet, Permission, ResourceType};

    fn provider() -> StaticIdentityProvider {
        let cred = CredentialHash::new(b"s3cret", [9u8; 16], 10_000);
        let principal = Arc::new(Principal {
            name: "app".into(),
            grants: vec![Permission {
                resource_type: ResourceType::Map,
                name: "orders*".into(),
                actions: ActionSet::of(Action::Read),
            }],
            is_admin: false,
        });
        let mut m = HashMap::new();
        m.insert("app".to_string(), (cred, principal));
        StaticIdentityProvider::new(m, Arc::new(Principal::empty("anonymous")), true)
    }

    #[test]
    fn correct_credentials_authenticate() {
        let p = provider();
        let princ = p.authenticate(Some("app"), Some("s3cret")).unwrap();
        assert_eq!(princ.name, "app");
        assert!(princ.authorize(ResourceType::Map, "orders1", Action::Read));
    }

    #[test]
    fn wrong_password_rejected() {
        assert!(provider().authenticate(Some("app"), Some("nope")).is_none());
    }

    #[test]
    fn unknown_user_rejected() {
        assert!(provider().authenticate(Some("ghost"), Some("x")).is_none());
    }

    #[test]
    fn missing_user_rejected_when_auth_required() {
        assert!(provider().authenticate(None, None).is_none());
    }

    #[test]
    fn open_provider_lets_anyone_in_as_anonymous() {
        let p = StaticIdentityProvider::open();
        let princ = p.authenticate(None, None).unwrap();
        assert!(princ.authorize(ResourceType::Map, "any", Action::Put)); // anonymous is full
        assert!(!p.requires_auth());
    }
}
