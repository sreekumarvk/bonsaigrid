//! The RBAC permission model: resource types, actions, and a single grant.
//!
//! A [`Permission`] is `(resource_type, name_pattern, actions)` — Hazelcast's
//! `MapPermission("cart*", "read","put")` shape. Matching is allocation-free so
//! it can run on the request hot path.

use serde::Deserialize;

/// A protected resource category. Mirrors Hazelcast's per-structure permission
/// classes (`MapPermission`, `QueuePermission`, …).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceType {
    Map,
    MultiMap,
    Queue,
    List,
    Set,
    Ringbuffer,
    Topic,
    PnCounter,
    Lock,
    Sql,
    Job,
    Cluster,
}

impl ResourceType {
    /// Parse a config token (case-insensitive).
    pub fn parse(s: &str) -> Option<ResourceType> {
        Some(match s.to_ascii_lowercase().as_str() {
            "map" => ResourceType::Map,
            "multimap" => ResourceType::MultiMap,
            "queue" => ResourceType::Queue,
            "list" => ResourceType::List,
            "set" => ResourceType::Set,
            "ringbuffer" => ResourceType::Ringbuffer,
            "topic" => ResourceType::Topic,
            "pncounter" => ResourceType::PnCounter,
            "lock" => ResourceType::Lock,
            "sql" => ResourceType::Sql,
            "job" => ResourceType::Job,
            "cluster" => ResourceType::Cluster,
            _ => return None,
        })
    }
}

/// A permitted action. `Admin` is the catch-all that also authorizes
/// otherwise-unmapped/administrative operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Create,
    Destroy,
    Read,
    Put,
    Remove,
    Listen,
    Lock,
    Offer,
    Poll,
    Admin,
}

impl Action {
    fn bit(self) -> u32 {
        1 << (self as u32)
    }
    /// Parse a config token (case-insensitive). `"all"` expands to every action.
    fn parse(s: &str) -> Option<ActionSet> {
        Some(match s.to_ascii_lowercase().as_str() {
            "all" => ActionSet::all(),
            "create" => ActionSet::of(Action::Create),
            "destroy" => ActionSet::of(Action::Destroy),
            "read" => ActionSet::of(Action::Read),
            "put" => ActionSet::of(Action::Put),
            "remove" => ActionSet::of(Action::Remove),
            "listen" => ActionSet::of(Action::Listen),
            "lock" => ActionSet::of(Action::Lock),
            "offer" => ActionSet::of(Action::Offer),
            "poll" => ActionSet::of(Action::Poll),
            "admin" => ActionSet::of(Action::Admin),
            _ => return None,
        })
    }
}

/// A compact bitset of [`Action`]s.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ActionSet(u32);

impl ActionSet {
    pub fn empty() -> ActionSet {
        ActionSet(0)
    }
    pub fn of(a: Action) -> ActionSet {
        ActionSet(a.bit())
    }
    pub fn all() -> ActionSet {
        // Every action bit set (Admin implies everything anyway).
        ActionSet(
            Action::Create.bit()
                | Action::Destroy.bit()
                | Action::Read.bit()
                | Action::Put.bit()
                | Action::Remove.bit()
                | Action::Listen.bit()
                | Action::Lock.bit()
                | Action::Offer.bit()
                | Action::Poll.bit()
                | Action::Admin.bit(),
        )
    }
    pub fn with(self, a: Action) -> ActionSet {
        ActionSet(self.0 | a.bit())
    }
    pub fn union(self, other: ActionSet) -> ActionSet {
        ActionSet(self.0 | other.0)
    }
    /// True if `a` is present, or `Admin` is (admin authorizes any action).
    pub fn contains(self, a: Action) -> bool {
        self.0 & a.bit() != 0 || self.0 & Action::Admin.bit() != 0
    }
    /// Parse a list of action tokens into a set.
    pub fn parse_list(tokens: &[String]) -> Option<ActionSet> {
        let mut set = ActionSet::empty();
        for t in tokens {
            set = set.union(Action::parse(t)?);
        }
        Some(set)
    }
}

/// One grant: which actions on which named resources of a given type.
#[derive(Clone, Debug)]
pub struct Permission {
    pub resource_type: ResourceType,
    /// Glob pattern: exact, or a trailing-`*` prefix, or `"*"` for all.
    pub name: String,
    pub actions: ActionSet,
}

/// The wire/JSON form of a permission (parsed in `config.rs`).
#[derive(Deserialize)]
pub struct PermissionConfig {
    pub resource_type: String,
    pub name: String,
    pub actions: Vec<String>,
}

/// Allocation-free glob match: exact string, trailing-`*` prefix, or `"*"`.
pub fn name_matches(pattern: &str, name: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return name.starts_with(prefix);
    }
    pattern == name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_matching() {
        assert!(name_matches("cart*", "cart42"));
        assert!(name_matches("cart*", "cart"));
        assert!(!name_matches("cart*", "order"));
        assert!(name_matches("*", "anything"));
        assert!(name_matches("exact", "exact"));
        assert!(!name_matches("exact", "other"));
    }

    #[test]
    fn action_set_membership() {
        let all = ActionSet::all();
        for a in [
            Action::Create,
            Action::Destroy,
            Action::Read,
            Action::Put,
            Action::Remove,
            Action::Listen,
            Action::Lock,
            Action::Offer,
            Action::Poll,
            Action::Admin,
        ] {
            assert!(all.contains(a), "all should contain {a:?}");
        }
        let rw = ActionSet::of(Action::Read).with(Action::Put);
        assert!(rw.contains(Action::Read));
        assert!(rw.contains(Action::Put));
        assert!(!rw.contains(Action::Remove));
    }

    #[test]
    fn admin_action_implies_all() {
        let admin = ActionSet::of(Action::Admin);
        assert!(admin.contains(Action::Read));
        assert!(admin.contains(Action::Remove));
        assert!(admin.contains(Action::Admin));
    }

    #[test]
    fn parse_tokens() {
        assert_eq!(ResourceType::parse("Map"), Some(ResourceType::Map));
        assert_eq!(ResourceType::parse("queue"), Some(ResourceType::Queue));
        assert_eq!(ResourceType::parse("bogus"), None);
        let set = ActionSet::parse_list(&["read".into(), "put".into()]).unwrap();
        assert!(
            set.contains(Action::Read)
                && set.contains(Action::Put)
                && !set.contains(Action::Remove)
        );
        assert_eq!(
            ActionSet::parse_list(&["all".into()]),
            Some(ActionSet::all())
        );
        assert_eq!(ActionSet::parse_list(&["nope".into()]), None);
    }
}
