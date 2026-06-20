"""Entry-listener conformance: a stock client registers a map entry listener and
must receive added/updated/removed events for mutations on the same connection.
"""
import sys
import time

import hazelcast


def main() -> int:
    client = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5701"],
        cluster_connect_timeout=10.0,
        smart_routing=False,  # one connection: register + mutate on the same core
    )
    m = client.get_map("listen").blocking()
    events = []
    m.add_entry_listener(
        include_value=True,
        added_func=lambda e: events.append(("added", e.key, e.value)),
        removed_func=lambda e: events.append(("removed", e.key)),
        updated_func=lambda e: events.append(("updated", e.key, e.value)),
    )

    m.put("k", "v1")  # ADDED
    m.put("k", "v2")  # UPDATED
    m.remove("k")     # REMOVED

    # Give the async events time to arrive.
    for _ in range(50):
        if len(events) >= 3:
            break
        time.sleep(0.05)

    print("events:", events)
    assert any(e == ("added", "k", "v1") for e in events), "missing ADDED"
    assert any(e == ("updated", "k", "v2") for e in events), "missing UPDATED"
    assert any(e == ("removed", "k") for e in events), "missing REMOVED"
    print("LISTENER SMOKE OK")
    client.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
