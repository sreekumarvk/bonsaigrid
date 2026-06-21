//! BonsaiGrid server: io_uring reactor, handshake wiring, and dispatch.

pub mod connection;
pub mod events;
pub mod handlers;
pub mod membership;
pub mod metrics;
pub mod reactor;
