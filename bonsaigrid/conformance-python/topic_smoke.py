"""Distributed Topic pub/sub via a stock client (uses the event path)."""
import sys
import time

import hazelcast


def main() -> int:
    client = hazelcast.HazelcastClient(
        cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=10.0
    )
    t = client.get_topic("t").blocking()
    msgs = []
    t.add_listener(lambda m: msgs.append(m.message))

    t.publish("hello")
    t.publish("world")

    for _ in range(60):
        if len(msgs) >= 2:
            break
        time.sleep(0.05)

    print("messages:", msgs)
    assert "hello" in msgs and "world" in msgs, "topic messages not received"
    print("TOPIC SMOKE OK")
    client.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
