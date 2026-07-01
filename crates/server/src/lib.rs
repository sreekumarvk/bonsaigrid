//! BonsaiGrid server: io_uring reactor, handshake wiring, and dispatch.

pub mod catalog;
pub mod cluster_coordinator;
pub mod connection;
pub mod entry_processor;
pub mod events;
pub mod executor;
pub mod handlers;
pub mod jobs;
pub mod kafka;
pub mod member_thread;
pub mod membership;
pub mod metrics;
pub mod migration;
pub mod reactor;
#[cfg(test)]
mod sim;
pub mod txn;
