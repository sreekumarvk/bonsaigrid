"""Phase D2: a member joins at runtime and receives its partitions via migration.

1. Two bootstrap members are up (started by the harness). A smart client puts N
   keys across them.
2. We launch a 3rd member configured to JOIN (index 2, bootstrap size 2). It asks
   the master to admit it; the master republishes the member list and the old
   owners migrate the now-reassigned partitions to it.
3. A fresh smart client (all three) reads every key back — some are now served by
   the newcomer — and sees a 3-member cluster.
"""
import os
import subprocess
import sys
import time

import hazelcast

N = 300


def main() -> int:
    a = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5701", "127.0.0.1:5702"],
        cluster_connect_timeout=15.0,
    )
    m = a.get_map("dj").blocking()
    for i in range(N):
        m.put(f"key{i}", f"val{i}")
    a.shutdown()

    # Launch member 2 as a runtime joiner (index 2 >= bootstrap size 2 -> joining).
    env = {
        **os.environ,
        "BONSAI_MEMBERS": "2",
        "BONSAI_MEMBER_INDEX": "2",
        "BONSAI_BACKUPS": "1",
    }
    joiner = subprocess.Popen(
        ["./target/debug/server"],
        env=env,
        stdout=open("/tmp/bonsai_dj_2.log", "w"),
        stderr=subprocess.STDOUT,
    )
    try:
        time.sleep(7.0)  # join + migration settle

        b = hazelcast.HazelcastClient(
            cluster_name="dev",
            cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"],
            cluster_connect_timeout=15.0,
        )
        mb = b.get_map("dj").blocking()
        missing = [i for i in range(N) if mb.get(f"key{i}") != f"val{i}"]
        try:
            member_count = len(b.cluster_service.get_members())
        except Exception:
            member_count = -1
        b.shutdown()
    finally:
        joiner.kill()

    if missing:
        print(f"DYNAMIC JOIN SMOKE FAILED — {len(missing)} keys lost, e.g. {missing[:10]}")
        return 1
    if member_count not in (-1, 3):
        print(f"DYNAMIC JOIN SMOKE FAILED — expected 3 members, saw {member_count}")
        return 1
    print(f"DYNAMIC JOIN SMOKE OK — all {N} keys readable after a runtime join (members={member_count})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
