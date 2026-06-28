"""Redpanda + BonsaiGrid streaming demo (the Hazelcast pizza-recommender demo).

A streaming SQL job consumes pizza orders from a Redpanda topic, enriches each
with the user's recommendation from an IMap (stream-to-table JOIN), filters to
'Soup' starters, and produces the enriched order to another Redpanda topic.

Flow: Python produces orders -> Redpanda(pizzastream) -> BonsaiGrid SQL job
      (JOIN recommender + WHERE) -> Redpanda(recommender_pizzastream) -> verified.
"""
import json
import subprocess
import sys
import time

import hazelcast

# user -> (starter, side). Only 'Soup' starters pass the job's WHERE filter.
RECS = {
    "user_1": ("Soup", "Onion_rings"),
    "user_2": ("Salad", "Fries"),
    "user_3": ("Soup", "Coleslaw"),
}
# (order_id, user_id, pizza)
ORDERS = [
    ("o1", "user_1", "Margherita"),
    ("o2", "user_2", "Pepperoni"),
    ("o3", "user_3", "Hawaiian"),
    ("o4", "user_1", "Veggie"),
    ("o5", "user_2", "Meat Feast"),
    ("o6", "user_3", "Four Cheese"),
]
EXPECTED = {oid for (oid, uid, _) in ORDERS if RECS[uid][0] == "Soup"}  # o1,o3,o4,o6


def rpk(*args, **kw):
    return subprocess.run(["docker", "exec", "redpanda", "rpk", *args], capture_output=True, text=True, **kw)


def produce(topic, lines):
    subprocess.run(
        ["docker", "exec", "-i", "redpanda", "rpk", "topic", "produce", topic],
        input="".join(l + "\n" for l in lines),
        text=True,
        capture_output=True,
    )


def consume(topic, num, timeout=20):
    try:
        r = subprocess.run(
            ["docker", "exec", "redpanda", "rpk", "topic", "consume", topic, "--num", str(num), "--offset", "start", "-f", "%v\n"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        r = e
    out = (r.stdout or "") if hasattr(r, "stdout") and r.stdout else ""
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def main() -> int:
    print("=" * 70)
    print("BonsaiGrid + Redpanda streaming demo: real-time pizza enrichment")
    print("=" * 70)

    c = hazelcast.HazelcastClient(cluster_name="dev", cluster_members=["127.0.0.1:5701"], cluster_connect_timeout=10.0)
    sql = c.sql

    print("\n[1] SQL: define the recommendation IMap + seed it")
    sql.execute(
        "CREATE MAPPING recommender (user_id VARCHAR, starter VARCHAR, side VARCHAR) "
        "TYPE IMap OPTIONS ('keyFormat'='varchar','valueFormat'='json-flat')"
    ).result()
    vals = ", ".join(f"('{u}','{s}','{sd}')" for u, (s, sd) in RECS.items())
    sql.execute(f"INSERT INTO recommender VALUES {vals}").result()
    for u, (s, sd) in RECS.items():
        print(f"      recommender[{u}] = starter={s}, side={sd}")

    print("\n[2] SQL: map the Redpanda source + sink topics")
    sql.execute(
        "CREATE MAPPING pizzastream (order_id VARCHAR, user_id VARCHAR, pizza VARCHAR) "
        "TYPE Kafka OPTIONS ('bootstrap.servers'='127.0.0.1:9092','valueFormat'='json-flat')"
    ).result()
    sql.execute(
        "CREATE MAPPING recommender_pizzastream (order_id VARCHAR, user_id VARCHAR, pizza VARCHAR, starter VARCHAR, side VARCHAR) "
        "TYPE Kafka OPTIONS ('bootstrap.servers'='127.0.0.1:9092','valueFormat'='json-flat')"
    ).result()

    print("\n[3] SQL: start the streaming JOB (enrich orders, keep only 'Soup')")
    sql.execute(
        "CREATE JOB enrich AS SINK INTO recommender_pizzastream "
        "SELECT order_id, user_id, pizza, starter, side FROM pizzastream "
        "JOIN recommender ON pizzastream.user_id = recommender.user_id "
        "WHERE starter = 'Soup'"
    ).result()
    c.shutdown()
    time.sleep(1.0)

    print("\n[4] Produce pizza orders to Redpanda(pizzastream):")
    lines = [json.dumps({"order_id": o, "user_id": u, "pizza": p}) for (o, u, p) in ORDERS]
    for ln in lines:
        print("      ->", ln)
    produce("pizzastream", lines)

    print(f"\n[5] Read enriched orders from Redpanda(recommender_pizzastream) (expecting {len(EXPECTED)}):")
    got = consume("recommender_pizzastream", len(EXPECTED), timeout=25)
    for row in sorted(got, key=lambda r: r.get("order_id", "")):
        print("      <-", json.dumps(row, sort_keys=True))

    # Verify.
    by_id = {r["order_id"]: r for r in got}
    if set(by_id) != EXPECTED:
        print(f"\nFAIL: enriched order ids {set(by_id)} != expected {EXPECTED}")
        return 1
    for oid, uid, pizza in ORDERS:
        if oid not in EXPECTED:
            continue
        r = by_id[oid]
        exp_starter, exp_side = RECS[uid]
        if (r["user_id"], r["pizza"], r["starter"], r["side"]) != (uid, pizza, exp_starter, exp_side):
            print(f"\nFAIL: {oid} enriched wrong: {r}")
            return 1

    print("\n" + "=" * 70)
    print(f"DEMO OK — {len(EXPECTED)} 'Soup' orders enriched via the streaming JOIN;")
    print("         Salad orders (user_2) correctly filtered out by the job.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
