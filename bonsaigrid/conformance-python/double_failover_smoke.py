"""Restore-K: data survives TWO sequential node losses.

3 members, K=1. Put IMap keys + an IQueue. Kill member 0; after auto-failover the
survivors must re-replicate (restore K) so every partition again has a backup.
Then kill member 1; the lone survivor (member 2) must still have everything —
which is only possible if K was actually restored after the first death.
"""
import os
import signal
import sys
import time

import hazelcast

N = 200


def kill(pidfile: str) -> None:
    with open(pidfile) as f:
        os.kill(int(f.read().strip()), signal.SIGKILL)


def main() -> int:
    a = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"],
        cluster_connect_timeout=15.0,
    )
    m = a.get_map("df").blocking()
    for i in range(N):
        m.put(f"key{i}", f"val{i}")
    q = a.get_queue("dfq").blocking()
    for i in range(N):
        q.offer(f"item{i}")
    a.shutdown()

    kill("/tmp/bonsai_df0.pid")
    time.sleep(7.0)  # auto-failover + restore-K re-replication
    kill("/tmp/bonsai_df1.pid")
    time.sleep(7.0)  # second failover

    b = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5703"],
        cluster_connect_timeout=15.0,
    )
    mb = b.get_map("df").blocking()
    missing = [i for i in range(N) if mb.get(f"key{i}") != f"val{i}"]
    qsize = b.get_queue("dfq").blocking().size()
    b.shutdown()

    if missing:
        print(f"DOUBLE FAILOVER SMOKE FAILED — {len(missing)} map keys lost, e.g. {missing[:10]}")
        return 1
    if qsize != N:
        print(f"DOUBLE FAILOVER SMOKE FAILED — queue has {qsize}/{N} after two deaths")
        return 1
    print(f"DOUBLE FAILOVER SMOKE OK — all {N} map keys + {N} queue items survived TWO node losses (K restored)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
