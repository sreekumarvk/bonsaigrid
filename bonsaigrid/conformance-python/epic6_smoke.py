"""ReplicatedMap, Ringbuffer, PNCounter, FlakeIdGenerator via a stock client."""
import sys

import hazelcast


def main() -> int:
    c = hazelcast.HazelcastClient(
        cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=10.0
    )

    rm = c.get_replicated_map("rm").blocking()
    assert rm.put("a", "1") is None
    assert rm.get("a") == "1"
    assert rm.size() == 1 and rm.contains_key("a")
    assert set(rm.key_set()) == {"a"}

    rb = c.get_ringbuffer("rb").blocking()
    s0 = rb.add("x")
    s1 = rb.add("y")
    assert s1 == s0 + 1
    assert rb.read_one(s0) == "x" and rb.read_one(s1) == "y"
    assert rb.size() == 2 and rb.tail_sequence() == s1

    pn = c.get_pn_counter("pn").blocking()
    assert pn.get() == 0
    assert pn.add_and_get(5) == 5
    assert pn.get_and_add(3) == 5
    assert pn.get() == 8

    fg = c.get_flake_id_generator("fg").blocking()
    ids = {fg.new_id() for _ in range(10)}
    assert len(ids) == 10  # all unique

    print("EPIC6 SMOKE OK")
    c.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
