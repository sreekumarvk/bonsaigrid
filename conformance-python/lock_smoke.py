"""IMap per-key locking via a stock client: tryLock/isLocked/unlock/lock/forceUnlock."""
import sys

import hazelcast


def main() -> int:
    client = hazelcast.HazelcastClient(
        cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=10.0
    )
    m = client.get_map("lk").blocking()

    assert m.try_lock("k") is True      # acquire
    assert m.is_locked("k") is True
    assert m.try_lock("k") is True      # reentrant (same thread)
    m.unlock("k")
    m.unlock("k")                       # release both holds
    assert m.is_locked("k") is False

    m.lock("k")                         # blocking lock (uncontended)
    assert m.is_locked("k") is True
    m.force_unlock("k")
    assert m.is_locked("k") is False

    print("LOCK SMOKE OK")
    client.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
