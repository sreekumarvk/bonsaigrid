"""Cross-connection entry listeners: client A registers a listener; client B
(a different connection, possibly a different core) mutates the map; A must
receive the events via the broker + reactor timer.
"""
import sys
import time

import hazelcast


def main() -> int:
    a = hazelcast.HazelcastClient(cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=10.0)
    b = hazelcast.HazelcastClient(cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=10.0)
    events = []

    ma = a.get_map("xc").blocking()
    ma.add_entry_listener(
        include_value=True,
        added_func=lambda e: events.append(("added", e.key, e.value)),
        removed_func=lambda e: events.append(("removed", e.key)),
    )

    mb = b.get_map("xc").blocking()
    mb.put("k", "v")   # mutation on a DIFFERENT connection
    mb.remove("k")

    for _ in range(100):
        if len(events) >= 2:
            break
        time.sleep(0.05)

    print("events:", events)
    assert ("added", "k", "v") in events, "missing cross-connection ADDED"
    assert ("removed", "k") in events, "missing cross-connection REMOVED"
    print("XCONN LISTENER SMOKE OK")
    a.shutdown()
    b.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
