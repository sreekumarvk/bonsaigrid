"""Near-cache invalidation: a near-cache client caches a value, an external
client updates it, and the near-cache client must see the fresh value (the
server delivered an invalidation event)."""
import sys
import time

import hazelcast


def main() -> int:
    cfg = {"near_caches": {"nc": {"invalidate_on_change": True}}}
    a = hazelcast.HazelcastClient(cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=10.0, **cfg)
    b = hazelcast.HazelcastClient(cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=10.0)
    ma = a.get_map("nc").blocking()
    mb = b.get_map("nc").blocking()

    ma.put("k", "v1")
    assert ma.get("k") == "v1"  # populates the near-cache
    mb.put("k", "v2")           # external update -> server invalidation
    time.sleep(0.6)
    assert ma.get("k") == "v2", "near-cache served stale data (invalidation missed)"

    print("NEARCACHE SMOKE OK")
    a.shutdown()
    b.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
