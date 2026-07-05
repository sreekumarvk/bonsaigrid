//! Geo/WAN replication: capture local IMap mutations, buffer them durably, ship
//! them to a remote cluster, and apply inbound updates with the HLC merge.
//!
//! Off the hot path: capture pushes to an SPSC ring and a WAN thread owns all the
//! disk and socket work. Active-active is loop-free because inbound records apply via
//! [`store::Store::apply_wan`] (persist yes, re-publish no); concurrent writes
//! converge through the store's HLC `put_merge`.
pub mod consumer;
pub mod publisher;
pub mod queue;
pub mod record;
pub mod wire;

pub use consumer::apply_batch;
pub use publisher::WanPublisher;
pub use queue::WanQueue;
pub use record::{decode, encode, Decoded, WanOp, WanRecord};
pub use wire::{decode_msg, encode_msg, WanMsg};
