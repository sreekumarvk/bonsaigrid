//! Hazelcast client-protocol message and custom-type codecs.
//!
//! Conventions (from the reference codecs):
//! - Custom composite types (Address, MemberInfo, MemberVersion) and collections
//!   of composites are wrapped in BEGIN/END data-structure marker frames.
//! - Fixed-size scalar lists (List<Integer>, List<UUID>) are a single frame of
//!   packed little-endian elements.

pub mod address;
pub mod atomiclong;
pub mod atomicref;
pub mod auth;
pub mod cache;
pub mod cluster_view;
pub mod executor;
pub mod map;
pub mod mc;
pub mod member_info;
pub mod partition_table;
pub mod schema;
pub mod sql;
pub mod txn;

use protocol::frame::{Frame, BEGIN_DS, END_DS};

pub fn begin_frame() -> Frame {
    Frame {
        flags: BEGIN_DS,
        content: Vec::new(),
    }
}
pub fn end_frame() -> Frame {
    Frame {
        flags: END_DS,
        content: Vec::new(),
    }
}
