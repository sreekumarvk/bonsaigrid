"""Exercises the broadened IMap API via a stock Hazelcast Python client:
remove, delete, contains_key/value, size, is_empty, put_if_absent, replace,
clear, and TTL expiry.
"""
import sys
import time

import hazelcast


def main() -> int:
    client = hazelcast.HazelcastClient(
        cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=10.0
    )
    m = client.get_map("api").blocking()
    m.clear()
    assert m.size() == 0
    assert m.is_empty()

    assert m.put("k", "v") is None
    assert m.get("k") == "v"
    assert m.contains_key("k")
    assert not m.contains_key("nope")
    assert m.contains_value("v")
    assert not m.contains_value("zzz")
    assert m.size() == 1
    assert not m.is_empty()

    assert m.put_if_absent("k", "v2") == "v"   # present -> returns existing, no change
    assert m.get("k") == "v"
    assert m.put_if_absent("k2", "x") is None  # absent -> inserts
    assert m.get("k2") == "x"

    assert m.replace("k", "v3") == "v"          # present -> old value
    assert m.get("k") == "v3"
    assert m.replace("absent", "y") is None     # absent -> no insert
    assert not m.contains_key("absent")

    assert m.remove("k") == "v3"
    assert not m.contains_key("k")
    m.delete("k2")
    assert not m.contains_key("k2")

    # TTL (python client ttl is in seconds; wire is ms)
    m.put("t", "temp", ttl=0.05)
    time.sleep(0.25)
    assert m.get("t") is None, "TTL entry should have expired"
    assert not m.contains_key("t")

    m.clear()
    assert m.size() == 0 and m.is_empty()

    print("API SMOKE OK")
    client.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
