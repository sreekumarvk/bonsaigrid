"""Phase D3: a quorum write-gate stops a minority from accepting writes.

1. Three members run with BONSAI_QUORUM=2. A client puts keys (3 live -> writes
   allowed).
2. We kill two members. After the heartbeat timeout the survivor's cluster shrinks
   to one (< quorum 2).
3. A client against the survivor finds writes rejected (SplitBrainProtectionError)
   while reads still succeed.
"""
import os
import signal
import sys
import time

import hazelcast

N = 50


def kill(pidfile: str) -> None:
    with open(pidfile) as f:
        os.kill(int(f.read().strip()), signal.SIGKILL)


def main() -> int:
    a = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"],
        cluster_connect_timeout=15.0,
    )
    m = a.get_map("q").blocking()
    for i in range(N):
        m.put(f"key{i}", f"val{i}")
    a.shutdown()

    # Kill members 0 and 1 -> only member 2 survives (1 < quorum 2).
    kill("/tmp/bonsai_q0.pid")
    kill("/tmp/bonsai_q1.pid")
    time.sleep(6.0)  # > heartbeat timeout so the survivor drops the dead members

    b = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5703"],
        cluster_connect_timeout=15.0,
    )
    mb = b.get_map("q").blocking()

    # A read must still succeed (not raise).
    try:
        mb.get("key0")
    except Exception as e:  # noqa: BLE001
        print(f"QUORUM SMOKE FAILED — read raised below quorum: {e!r}")
        b.shutdown()
        return 1

    # A write must be rejected.
    write_rejected = False
    try:
        mb.put("newkey", "newval")
    except Exception:  # noqa: BLE001
        write_rejected = True
    b.shutdown()

    if not write_rejected:
        print("QUORUM SMOKE FAILED — write accepted below quorum")
        return 1
    print("QUORUM SMOKE OK — writes rejected below quorum, reads still served")
    return 0


if __name__ == "__main__":
    sys.exit(main())
