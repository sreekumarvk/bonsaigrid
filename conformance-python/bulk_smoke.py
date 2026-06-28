"""Bulk IMap ops via a stock client: put_all, get_all, key_set, values, entry_set."""
import sys

import hazelcast


def main() -> int:
    client = hazelcast.HazelcastClient(
        cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=10.0
    )
    m = client.get_map("bulk").blocking()
    m.clear()

    m.put_all({"a": "1", "b": "2", "c": "3"})
    assert m.size() == 3, m.size()

    got = m.get_all(["a", "c", "x"])  # x is absent
    assert got == {"a": "1", "c": "3"}, got

    assert set(m.key_set()) == {"a", "b", "c"}, m.key_set()
    assert sorted(m.values()) == ["1", "2", "3"], m.values()
    assert dict(m.entry_set()) == {"a": "1", "b": "2", "c": "3"}, m.entry_set()

    m.clear()
    assert m.key_set() == [] and m.values() == []

    print("BULK SMOKE OK")
    client.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
