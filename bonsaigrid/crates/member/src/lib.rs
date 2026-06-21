//! BonsaiGrid member-to-member plane: a custom (non-Hazelcast) framed protocol,
//! an io_uring full-mesh transport, and the synchronous-backup replication state
//! machine. Only the *client* protocol is Hazelcast-compatible; everything here
//! is BonsaiGrid-internal and may change freely.

pub mod wire;
