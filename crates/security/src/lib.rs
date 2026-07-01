//! BonsaiGrid security: authentication (hashed credentials + identity provider),
//! authorization (Hazelcast-parity resource+action RBAC), and (later) TLS config.
//!
//! Pure logic — no sockets, no io_uring. The server binds a [`Principal`] to each
//! connection at authentication and calls [`optable::classify`] +
//! [`Principal::authorize`] in the dispatch path (allocation-free).

pub mod config;
pub mod credential;
pub mod identity;
pub mod optable;
pub mod permission;
pub mod principal;

pub use identity::{IdentityProvider, StaticIdentityProvider};
pub use optable::{classify, resource_name, Decision};
pub use permission::{Action, ResourceType};
pub use principal::Principal;

use std::sync::Arc;

/// The assembled, immutable per-node security state. Shared across the per-core
/// reactor threads by clone/`Arc` — read-only, so no locking.
pub struct SecurityContext {
    provider: StaticIdentityProvider,
}

impl SecurityContext {
    /// The permissive default used when no security config is present:
    /// authentication is not required and everyone is the full `anonymous`
    /// principal. Preserves today's no-auth behavior.
    pub fn open() -> SecurityContext {
        SecurityContext {
            provider: StaticIdentityProvider::open(),
        }
    }

    /// Load from JSON config text (principals + credentials + permissions).
    pub fn from_json(json: &str) -> Result<SecurityContext, config::ConfigError> {
        Ok(SecurityContext {
            provider: config::load_provider(json)?,
        })
    }

    /// Authenticate a client. `None` = rejected.
    pub fn authenticate(&self, user: Option<&str>, pass: Option<&str>) -> Option<Arc<Principal>> {
        self.provider.authenticate(user, pass)
    }

    /// The principal to use before/without authentication.
    pub fn anonymous(&self) -> Arc<Principal> {
        self.provider.anonymous()
    }

    /// Whether a client must present valid credentials.
    pub fn requires_auth(&self) -> bool {
        self.provider.requires_auth()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_context_authorizes_everything() {
        let ctx = SecurityContext::open();
        assert!(!ctx.requires_auth());
        let p = ctx.authenticate(None, None).unwrap();
        assert!(p.authorize(ResourceType::Map, "any", Action::Put));
    }
}
