"""Distributed Set + MultiMap via a stock client."""
import sys

import hazelcast


def main() -> int:
    client = hazelcast.HazelcastClient(
        cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=10.0
    )

    # ---- Set ----
    s = client.get_set("s").blocking()
    assert s.add("a") is True
    assert s.add("a") is False  # duplicate
    assert s.add("b") is True
    assert s.size() == 2
    assert s.contains("a") and not s.contains("z")
    assert set(s.get_all()) == {"a", "b"}
    assert s.remove("a") is True
    assert s.size() == 1

    # ---- MultiMap (Set semantics) ----
    mm = client.get_multi_map("mm").blocking()
    assert mm.put("k", "1") is True
    assert mm.put("k", "2") is True
    assert mm.put("k", "1") is False  # duplicate value
    assert sorted(mm.get("k")) == ["1", "2"]
    assert mm.value_count("k") == 2
    assert mm.size() == 2

    print("STRUCTURES SMOKE OK")
    client.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
