"""HA for the auxiliary structures: queues/lists/sets (name-partitioned) and
MultiMap (key-partitioned) survive node loss.

Fill many named IQueue/IList/ISet/MultiMap (spread across partitions, so several
land on member 0), SIGKILL member 0, and after auto-failover a fresh client reads
identical contents — the structures member 0 owned are served by their backups.
"""
import os
import signal
import sys
import time

import hazelcast

M = 18  # structures per type -> several land on each member


def kill(pidfile: str) -> None:
    with open(pidfile) as f:
        os.kill(int(f.read().strip()), signal.SIGKILL)


def main() -> int:
    a = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"],
        cluster_connect_timeout=15.0,
    )
    for i in range(M):
        lst = a.get_list(f"L{i}").blocking()
        lst.add(f"l{i}_0")
        lst.add(f"l{i}_1")
        st = a.get_set(f"S{i}").blocking()
        st.add(f"s{i}_0")
        st.add(f"s{i}_1")
        q = a.get_queue(f"Q{i}").blocking()
        q.offer(f"q{i}_0")
        q.offer(f"q{i}_1")
        mm = a.get_multi_map(f"MM{i}").blocking()
        mm.put(f"k{i}", f"v{i}_0")
        mm.put(f"k{i}", f"v{i}_1")
    a.shutdown()

    kill("/tmp/bonsai_ha0.pid")
    time.sleep(6.0)  # > heartbeat timeout: auto-failover

    b = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5702", "127.0.0.1:5703"],
        cluster_connect_timeout=15.0,
    )
    bad = []
    for i in range(M):
        if sorted(b.get_list(f"L{i}").blocking().get_all()) != [f"l{i}_0", f"l{i}_1"]:
            bad.append(f"L{i}")
        if sorted(b.get_set(f"S{i}").blocking().get_all()) != [f"s{i}_0", f"s{i}_1"]:
            bad.append(f"S{i}")
        if b.get_queue(f"Q{i}").blocking().size() != 2:
            bad.append(f"Q{i}")
        if sorted(b.get_multi_map(f"MM{i}").blocking().get(f"k{i}")) != [f"v{i}_0", f"v{i}_1"]:
            bad.append(f"MM{i}")
    b.shutdown()

    if bad:
        print(f"STRUCTURE HA SMOKE FAILED — {len(bad)} structures lost data, e.g. {bad[:10]}")
        return 1
    print(f"STRUCTURE HA SMOKE OK — all {M} of each (list/set/queue/multimap) survived member-0 loss")
    return 0


if __name__ == "__main__":
    sys.exit(main())
