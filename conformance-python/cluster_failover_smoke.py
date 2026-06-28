"""Phase C failover: a promoted backup serves a dead primary's keys.

1. A smart client puts N keys across a 3-member cluster (K=1 sync backups), so
   every key is replicated to its backup before put() returns.
2. We promote member 0 (mark it dead) on both survivors, then kill member 0's
   process.
3. A fresh smart client (survivors only) reads every key back — the partitions
   member 0 owned are now served by member 1, which held the backups.
"""
import os
import signal
import sys
import time
import urllib.request

import hazelcast

N = 300


def promote(port: int, dead: int) -> str:
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/cluster/promote?dead={dead}", method="POST", data=b""
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        return r.read().decode()


def kill_member0() -> None:
    """Kill member 0 by the PID the harness recorded (env-var prefixes are not in
    argv, so pkill -f cannot find it)."""
    with open("/tmp/bonsai_m0.pid") as f:
        pid = int(f.read().strip())
    os.kill(pid, signal.SIGKILL)


def main() -> int:
    # 1) Put N keys with a smart client; synchronous backups make each durable.
    a = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"],
        cluster_connect_timeout=15.0,
    )
    m = a.get_map("fo").blocking()
    for i in range(N):
        m.put(f"key{i}", f"val{i}")
    a.shutdown()

    # 2) Promote member 0 (dead) on both survivors, then kill its process.
    print("promote 5702:", promote(5702, 0))
    print("promote 5703:", promote(5703, 0))
    kill_member0()
    time.sleep(1.0)

    # 3) Fresh client to the survivors only reads everything back.
    b = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5702", "127.0.0.1:5703"],
        cluster_connect_timeout=15.0,
    )
    mb = b.get_map("fo").blocking()
    missing = [i for i in range(N) if mb.get(f"key{i}") != f"val{i}"]
    b.shutdown()

    if missing:
        print(f"CLUSTER FAILOVER SMOKE FAILED — {len(missing)} keys lost, e.g. {missing[:10]}")
        return 1
    print(f"CLUSTER FAILOVER SMOKE OK — all {N} keys survived member-0 loss")
    return 0


if __name__ == "__main__":
    sys.exit(main())
