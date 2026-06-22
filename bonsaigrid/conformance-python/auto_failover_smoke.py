"""Phase D1: automatic failover via heartbeat detection (NO manual promote).

1. A smart client puts N keys across a 3-member cluster (K=1 sync backups).
2. We SIGKILL member 0 and do NOTHING else — no /cluster/promote.
3. After the heartbeat timeout, the survivors detect the death, the next-oldest
   becomes master, removes member 0, and republishes the member list. A fresh
   client (survivors only) then reads every key — member 0's partitions are now
   served by the backup that held them.
"""
import os
import signal
import sys
import time

import hazelcast

N = 300


def kill_member0() -> None:
    with open("/tmp/bonsai_m0.pid") as f:
        os.kill(int(f.read().strip()), signal.SIGKILL)


def main() -> int:
    a = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"],
        cluster_connect_timeout=15.0,
    )
    m = a.get_map("af").blocking()
    for i in range(N):
        m.put(f"key{i}", f"val{i}")
    a.shutdown()

    # Kill member 0 with NO promote call; wait past the heartbeat timeout (3s).
    kill_member0()
    time.sleep(6.0)

    b = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5702", "127.0.0.1:5703"],
        cluster_connect_timeout=15.0,
    )
    mb = b.get_map("af").blocking()
    missing = [i for i in range(N) if mb.get(f"key{i}") != f"val{i}"]
    b.shutdown()

    if missing:
        print(f"AUTO FAILOVER SMOKE FAILED — {len(missing)} keys lost, e.g. {missing[:10]}")
        return 1
    print(f"AUTO FAILOVER SMOKE OK — all {N} keys survived member-0 loss with no manual promote")
    return 0


if __name__ == "__main__":
    sys.exit(main())
