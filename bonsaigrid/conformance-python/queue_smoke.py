"""Distributed Queue via a stock client: offer/poll/peek/size/contains/remove."""
import sys

import hazelcast


def main() -> int:
    client = hazelcast.HazelcastClient(
        cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=10.0
    )
    q = client.get_queue("q").blocking()
    q.clear()
    assert q.is_empty()

    assert q.offer("a") is True
    assert q.offer("b") is True
    assert q.offer("c") is True
    assert q.size() == 3
    assert q.peek() == "a"          # head, not removed
    assert q.contains("b")
    assert not q.contains("z")

    assert q.poll() == "a"          # FIFO
    assert q.poll() == "b"
    assert q.size() == 1

    assert q.remove("c") is True    # remove the last remaining
    assert q.is_empty()
    assert q.poll() is None         # empty -> null

    print("QUEUE SMOKE OK")
    client.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
