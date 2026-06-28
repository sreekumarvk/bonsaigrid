"""Authentication: the correct cluster name connects; a wrong one is rejected.
(Run the server with the default cluster name 'dev'.)"""
import sys

import hazelcast


def main() -> int:
    c = hazelcast.HazelcastClient(cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=5.0)
    c.shutdown()

    rejected = False
    try:
        c2 = hazelcast.HazelcastClient(
            cluster_name="wrong", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=5.0, retry_count=0
        )
        c2.shutdown()
    except Exception:
        rejected = True

    assert rejected, "client with wrong cluster name should be rejected"
    print("AUTH SMOKE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
