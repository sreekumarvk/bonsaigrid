//! Maps a client message type to the authorization decision the dispatcher must
//! make before executing it.
//!
//! Hazelcast encodes the owning *service* in the high bits of the message type
//! (`service = msg_type >> 16`): 1=Map, 2=MultiMap, 3=Queue, 4=Topic, 5=List,
//! 6=Set, 8=Executor, 13=ReplicatedMap, 14=TransactionalMap, 19=Cache,
//! 21=Transaction, 23=Ringbuffer, 28=FlakeId, 29=PNCounter, 32=MgmtCenter,
//! 33=SQL, 0=Client. The Map service is gated per operation (correct read/write/
//! lock/listen action); other data structures are gated at the resource level
//! (v1 uses a conservative write-level default — per-op actions for non-Map
//! structures are a follow-up). Connection/session/management ops are governed by
//! authentication (and admin for Management Center), not per-resource permission.

use crate::permission::{Action, ResourceType};

/// What the dispatcher should do with an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Gate on `authorize(resource_type, resource_name, action)`.
    Data(ResourceType, Action),
    /// Allowed for any authenticated principal (connection/session control).
    Infra,
    /// Requires an admin principal (e.g. Management Center).
    AdminOnly,
    /// Not classified — fail closed (default-deny). No dispatched op should map
    /// here; the coverage test guards that.
    Unknown,
}

/// Classify a message type into an authorization [`Decision`].
pub fn classify(msg_type: i32) -> Decision {
    let service = msg_type >> 16;
    match service {
        1 => Decision::Data(ResourceType::Map, map_action(msg_type)),
        // ReplicatedMap / TransactionalMap / Cache are all map-shaped resources.
        13 | 14 | 19 => Decision::Data(ResourceType::Map, coarse_action()),
        2 => Decision::Data(ResourceType::MultiMap, coarse_action()),
        3 => Decision::Data(ResourceType::Queue, coarse_action()),
        4 => Decision::Data(ResourceType::Topic, coarse_action()),
        5 => Decision::Data(ResourceType::List, coarse_action()),
        6 => Decision::Data(ResourceType::Set, coarse_action()),
        23 => Decision::Data(ResourceType::Ringbuffer, coarse_action()),
        29 => Decision::Data(ResourceType::PnCounter, coarse_action()),
        33 => Decision::Data(ResourceType::Sql, coarse_action()),
        8 => Decision::Data(ResourceType::Job, coarse_action()),
        // Connection/session/id-generation control — authentication-gated only.
        0 | 21 | 28 => Decision::Infra,
        // Management Center — administrative surface.
        32 => Decision::AdminOnly,
        _ => Decision::Unknown,
    }
}

/// Conservative default action for non-Map data structures (v1): write-level, so
/// only principals with write/admin on that resource pass. Fail-closed.
fn coarse_action() -> Action {
    Action::Put
}

/// Precise per-operation action for the Map service (`service == 1`).
fn map_action(msg_type: i32) -> Action {
    match msg_type {
        66048 | 67072 | 67328 | 76288 | 76544 | 70144 | 74496 | 74240 | 74752 | 75008 | 75264
        | 75520 | 75776 | 80640 | 80896 | 87552 | 87808 | 81152 => Action::Read,
        65792 | 69120 | 66560 | 76800 | 76032 | 77312 => Action::Put,
        66304 | 67840 | 77056 => Action::Remove,
        69632 | 69888 | 70400 | 78592 => Action::Lock,
        71936 | 71424 | 72448 | 81664 => Action::Listen,
        // Any other Map op we haven't tabulated: require write (fail-closed).
        _ => Action::Put,
    }
}

/// Extract the resource name (the first string frame — the object name, by
/// Hazelcast convention) as a borrowed slice. Allocation-free.
pub fn resource_name<'a>(req_frames: &'a [&'a [u8]]) -> Option<&'a str> {
    // frame[0] is the initial fixed frame; frame[1] is the object name.
    req_frames.get(1).and_then(|c| std::str::from_utf8(c).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_ops_classified_precisely() {
        assert_eq!(
            classify(65792),
            Decision::Data(ResourceType::Map, Action::Put)
        ); // MapPut
        assert_eq!(
            classify(66048),
            Decision::Data(ResourceType::Map, Action::Read)
        ); // MapGet
        assert_eq!(
            classify(66304),
            Decision::Data(ResourceType::Map, Action::Remove)
        ); // MapRemove
        assert_eq!(
            classify(69632),
            Decision::Data(ResourceType::Map, Action::Lock)
        ); // MapLock
        assert_eq!(
            classify(71936),
            Decision::Data(ResourceType::Map, Action::Listen)
        ); // AddEntryListener
    }

    #[test]
    fn other_services_classified() {
        assert_eq!(
            classify(131328),
            Decision::Data(ResourceType::MultiMap, Action::Put)
        );
        assert_eq!(
            classify(196864),
            Decision::Data(ResourceType::Queue, Action::Put)
        );
        assert_eq!(
            classify(330240),
            Decision::Data(ResourceType::List, Action::Put)
        );
        assert_eq!(
            classify(395776),
            Decision::Data(ResourceType::Set, Action::Put)
        );
        assert_eq!(
            classify(1509120),
            Decision::Data(ResourceType::Ringbuffer, Action::Put)
        );
        assert_eq!(
            classify(1901312),
            Decision::Data(ResourceType::PnCounter, Action::Put)
        );
        assert_eq!(
            classify(2163712),
            Decision::Data(ResourceType::Sql, Action::Put)
        );
        assert_eq!(
            classify(262400),
            Decision::Data(ResourceType::Topic, Action::Put)
        );
        assert_eq!(
            classify(852224),
            Decision::Data(ResourceType::Map, Action::Put)
        ); // ReplicatedMap
    }

    #[test]
    fn infra_and_admin() {
        assert_eq!(classify(1024), Decision::Infra); // ClientCreateProxy
        assert_eq!(classify(1376512), Decision::Infra); // TransactionCommit
        assert_eq!(classify(1835264), Decision::Infra); // FlakeIdGenerator
        assert_eq!(classify(2099968), Decision::AdminOnly); // MCGetTimedMemberState
    }

    #[test]
    fn resource_name_is_first_string_frame() {
        let initial: &[u8] = &[0u8; 24];
        let name: &[u8] = b"orders";
        let frames: Vec<&[u8]> = vec![initial, name];
        assert_eq!(resource_name(&frames), Some("orders"));
    }

    /// COVERAGE GUARD: every message type the server dispatches must classify to
    /// something other than `Unknown`, so no operation silently bypasses authz.
    /// This list is the set of numeric dispatch arms in `handlers.rs`.
    #[test]
    fn every_dispatched_op_is_classified() {
        const DISPATCHED: &[i32] = &[
            1024, 3072, 3840, 4864, 5120, 5376, 5632, 65792, 66048, 66304, 66560, 67072, 67328,
            67840, 69120, 69632, 69888, 70144, 70400, 71424, 71936, 72448, 74240, 74496, 74752,
            75008, 75264, 75520, 75776, 76032, 76288, 76544, 76800, 77056, 77312, 78592, 80640,
            80896, 81152, 81664, 87552, 87808, 131328, 131584, 131840, 133632, 134144, 196864,
            197376, 197632, 197888, 198400, 199424, 200448, 201728, 262400, 262656, 327936, 328192,
            328704, 328960, 329984, 330240, 331008, 331520, 393472, 393728, 394240, 394496, 395520,
            395776, 525568, 525824, 852224, 852480, 852736, 852992, 853248, 853504, 853760, 854272,
            855808, 856064, 856320, 919040, 1248512, 1250048, 1250816, 1376512, 1377024, 1507584,
            1507840, 1508096, 1508352, 1508864, 1509120, 1835264, 1900800, 1901056, 1901312,
            2099968, 2163456, 2163712, 2163968,
        ];
        for &id in DISPATCHED {
            assert_ne!(
                classify(id),
                Decision::Unknown,
                "msg_type {id} (service {}) is unclassified — it would bypass or wrongly deny authz",
                id >> 16
            );
        }
    }
}
