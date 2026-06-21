"""Blocking lock under contention: client A holds the lock; client B's lock()
blocks until A releases, then is granted (deferred response delivered via the
reactor)."""
import sys
import threading
import time

import hazelcast


def main() -> int:
    a = hazelcast.HazelcastClient(cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=10.0)
    b = hazelcast.HazelcastClient(cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=10.0)
    ma = a.get_map("blk").blocking()
    mb = b.get_map("blk").blocking()

    ma.lock("k")  # A holds the lock
    acquired = threading.Event()

    def worker():
        mb.lock("k")  # blocks until A releases
        acquired.set()
        mb.unlock("k")

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.6)
    blocked = not acquired.is_set()
    ma.unlock("k")  # release -> B is granted
    t.join(timeout=5)

    assert blocked, "B should have been blocked while A held the lock"
    assert acquired.is_set(), "B should have acquired the lock after A released"
    print("BLOCKING LOCK SMOKE OK")
    a.shutdown()
    b.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
