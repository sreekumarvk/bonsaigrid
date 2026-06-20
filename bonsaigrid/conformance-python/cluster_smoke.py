"""Multi-node conformance: a stock smart client against a 3-member BonsaiGrid
cluster. The client routes each key to its partition's owner; correct round-trip
of many keys proves cross-node routing + per-member storage.
"""
import sys

import hazelcast


def main() -> int:
    client = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"],
        cluster_connect_timeout=15.0,
        # smart routing (default): connect to all members, route per partition.
    )
    m = client.get_map("clustermap").blocking()
    n = 1000
    for i in range(n):
        m.put(f"key{i}", f"val{i}")
    for i in range(n):
        got = m.get(f"key{i}")
        assert got == f"val{i}", f"mismatch at key{i}: {got!r}"

    print(f"CLUSTER SMOKE OK — {n} keys round-tripped across the cluster")
    client.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
