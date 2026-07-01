//! An authenticated identity and its authorization decision.

use crate::permission::{name_matches, Action, Permission, ResourceType};

/// An authenticated principal with its compiled permission grants. Built once at
/// authentication and bound to the connection; `authorize` runs per request and
/// allocates nothing.
#[derive(Clone, Debug)]
pub struct Principal {
    pub name: String,
    pub grants: Vec<Permission>,
    /// Admin bypasses the grant list and authorizes every operation (including
    /// otherwise-unmapped/administrative ones).
    pub is_admin: bool,
}

impl Principal {
    /// A principal with no grants and no admin — denied everything.
    pub fn empty(name: impl Into<String>) -> Principal {
        Principal {
            name: name.into(),
            grants: Vec::new(),
            is_admin: false,
        }
    }

    /// The permissive `anonymous` principal used when no security config is
    /// present — authorizes everything, preserving today's no-auth behavior.
    pub fn anonymous_full() -> Principal {
        Principal {
            name: "anonymous".into(),
            grants: Vec::new(),
            is_admin: true,
        }
    }

    /// Authorize `action` on a resource of `rt` named `name`. Admin always
    /// passes; otherwise any grant whose type matches, whose glob matches the
    /// name, and whose action set contains `action` authorizes. Allocation-free.
    pub fn authorize(&self, rt: ResourceType, name: &str, action: Action) -> bool {
        if self.is_admin {
            return true;
        }
        self.grants.iter().any(|g| {
            g.resource_type == rt && g.actions.contains(action) && name_matches(&g.name, name)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permission::ActionSet;

    fn read_only_on_orders() -> Principal {
        Principal {
            name: "app".into(),
            grants: vec![Permission {
                resource_type: ResourceType::Map,
                name: "orders*".into(),
                actions: ActionSet::of(Action::Read),
            }],
            is_admin: false,
        }
    }

    #[test]
    fn read_only_principal() {
        let p = read_only_on_orders();
        assert!(p.authorize(ResourceType::Map, "orders9", Action::Read));
        assert!(!p.authorize(ResourceType::Map, "orders9", Action::Put));
        assert!(!p.authorize(ResourceType::Map, "cart9", Action::Read)); // name mismatch
        assert!(!p.authorize(ResourceType::Queue, "orders9", Action::Read)); // type mismatch
    }

    #[test]
    fn admin_authorizes_everything() {
        let p = Principal {
            name: "ops".into(),
            grants: vec![],
            is_admin: true,
        };
        assert!(p.authorize(ResourceType::Map, "any", Action::Put));
        assert!(p.authorize(ResourceType::Cluster, "x", Action::Admin));
    }

    #[test]
    fn anonymous_full_authorizes_everything() {
        let p = Principal::anonymous_full();
        assert!(p.authorize(ResourceType::Map, "any", Action::Remove));
    }

    #[test]
    fn empty_principal_denies_everything() {
        let p = Principal::empty("nobody");
        assert!(!p.authorize(ResourceType::Map, "any", Action::Read));
    }
}
