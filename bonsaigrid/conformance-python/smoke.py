"""Stock Hazelcast Python client smoke test against BonsaiGrid.

Connects an unmodified hazelcast-python-client to the running server, then
exercises IMap.put / IMap.get. Proves wire compatibility end-to-end.
"""
import logging
import sys

import hazelcast

logging.basicConfig(level=logging.INFO)


def main() -> int:
    client = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5701"],
        cluster_connect_timeout=10.0,
        # smart_routing defaults to True: the client fetches the partition table
        # and routes per-partition, exercising our partition-table encoding.
    )
    m = client.get_map("m").blocking()
    assert m.put("k", "v") is None, "first put should return no prior value"
    got = m.get("k")
    assert got == "v", f"expected 'v', got {got!r}"
    print("PYTHON SMOKE OK")
    client.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
