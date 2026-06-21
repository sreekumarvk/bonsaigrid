//! Lightweight operational metrics registry, exposed over HTTP.
//!
//! Hazelcast's full operator surface is JMX + the Management Center protocol
//! (a large, separately-verified surface). This provides a concrete, verifiable
//! starting point: live counters scraped via a Prometheus-style `/metrics`
//! endpoint on the main port (protocol-detected alongside the binary client).

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Metrics {
    requests: AtomicU64,
}

impl Metrics {
    pub fn new() -> Metrics {
        Metrics::default()
    }
    pub fn inc_request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }
    pub fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    /// Prometheus text exposition for `GET /metrics`.
    pub fn prometheus(&self, cluster_size: usize) -> String {
        format!(
            "# HELP bonsaigrid_requests_total Total client requests dispatched.\n\
             # TYPE bonsaigrid_requests_total counter\n\
             bonsaigrid_requests_total {}\n\
             # HELP bonsaigrid_cluster_size Number of cluster members.\n\
             # TYPE bonsaigrid_cluster_size gauge\n\
             bonsaigrid_cluster_size {}\n",
            self.requests(),
            cluster_size,
        )
    }
}
