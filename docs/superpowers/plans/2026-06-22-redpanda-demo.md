# Redpanda + BonsaiGrid Streaming Demo — Implementation Plan

> Goal: run the Redpanda↔Hazelcast "pizza recommender" demo on BonsaiGrid. A
> streaming SQL job consumes orders from a Redpanda topic, enriches each with a
> recommendation from an IMap (stream⋈table JOIN), and produces the result to
> another Redpanda topic. End state: show enriched messages flowing.

**Reference:** Hazelcast Java (SQL mappings, Jet pipeline, Kafka connector) for
design; implemented minimally in Rust for exactly what the demo needs.

**Test target:** Redpanda in Docker on `127.0.0.1:9092` (verified working).

## Architecture

The BonsaiGrid server gains a **SQL control plane** (catalog of mappings + jobs)
and a **streaming connector** (a dedicated thread per job that bridges Redpanda
and the in-memory IMap):

```
 SQL client ──SqlExecute──▶ server: CREATE MAPPING / INSERT / CREATE JOB
                                  │  catalog (mappings) + jobs
   Redpanda topic pizzastream ──▶ │ job thread: poll source → JOIN imap → filter
                                  │            → JSON row → produce
                              ◀── │ Redpanda topic recommender_pizzastream
```

## Chunks (each: implement → test → commit)

### Chunk A — SQL catalog + `CREATE MAPPING` + DDL response
- `server::catalog`: `Mapping { name, kind: Imap|Kafka, columns: Vec<(String,ColType)>, options: HashMap<String,String> }`; a `Catalog` (Arc<Mutex<HashMap<name,Mapping>>>) shared like `schemas`.
- Extend `query::sql`: parse `CREATE MAPPING <name> (<cols>) TYPE <Imap|Kafka> [OPTIONS(...)]` → a `Statement::CreateMapping`.
- SqlExecute handler: a `CreateMapping` returns a void/update response (`update_count=0`, null metadata/page/error).
- **Test:** Rust unit (parse CREATE MAPPING); e2e — client sends CREATE MAPPING, no error.

### Chunk B — `json-flat` values + `INSERT INTO` + SELECT over JSON
- Add `serde_json` to `query` (+ a `JsonExtractor: FieldExtractor` over a JSON-object value blob: the IMap value is a `HazelcastJsonValue` Data — `[hdr][type=HZ_JSON][utf8 json]`).
- Parse `INSERT INTO <mapping> VALUES (..),(..)`. Map row → key (first column / keyFormat) + json-flat value (remaining columns). `store.put` the entry as a JSON-value Data blob.
- `SELECT` chooses the extractor by the mapping's `valueFormat` (json-flat → JsonExtractor, else Compact).
- **Test:** unit (json extract); e2e — CREATE MAPPING(json-flat) + INSERT + SELECT returns rows.

### Chunk C — stream⋈table JOIN (and table⋈table)
- Parse `... FROM <left> JOIN <right> ON <left.col> = <right.col> [WHERE ...]`.
- Executor: for table⋈table, hash-join (build map from right keyed by join col, probe with left). For the streaming case the "left row" is a single Kafka record and the right is the IMap (direct `get` when the join col is the IMap key).
- **Test:** unit + e2e — two json-flat IMaps joined.

### Chunk D — Kafka client + `TYPE Kafka` mapping
- New `crates/kafka` (or `server::kafka`) wrapping the `kafka` crate: `Consumer` (poll a topic from offset) + `Producer` (send to a topic). `bootstrap.servers` from mapping OPTIONS.
- **Test:** Rust integration against the running Redpanda — produce then consume a record.

### Chunk E — `CREATE JOB` streaming pipeline
- Parse `CREATE JOB <name> AS SINK INTO <kafka sink> <select-with-join>`.
- Spawn a job thread: poll the source Kafka topic; per record → parse JSON → JOIN with the IMap (lookup by key) → apply WHERE → project columns → JSON-encode → produce to the sink topic. Generation/stop handling so the job runs until shutdown.
- **Test:** unit (one record through the transform); then the demo.

### Chunk F — the demo, end to end
- `conformance-python/redpanda_demo.py` + `run_redpanda_demo.sh`: start Redpanda + BonsaiGrid; create topics; client issues CREATE MAPPING(recommender, IMap json-flat) + INSERT recommendations + CREATE MAPPING(pizzastream, Kafka) + CREATE JOB; a producer writes pizza orders to `pizzastream`; consume `recommender_pizzastream` and assert each order is enriched with its user's recommendation.
- **Show it working.**

## Demo shape (self-defined, matching the blog)
- `recommender` IMap: key = `user_id` (VARCHAR), value json-flat `{starter, side, dessert}`.
- `pizzastream` Kafka: json `{order_id, user_id, pizza}`.
- Job: enrich each order with `recommender[user_id]` where `starter='Soup'`, sink the
  merged JSON to `recommender_pizzastream`.

## Notes / pragmatic scope
- SQL stays single-statement, single-node execution (one member runs the job).
- Only the column types the demo needs (VARCHAR/INT) and json-flat/Compact value formats.
- Keep the IMap/member HA paths untouched; the streaming connector is a separate thread.
