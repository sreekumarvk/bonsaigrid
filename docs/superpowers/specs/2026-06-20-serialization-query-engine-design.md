# Serialization + Query Engine Design

**Date:** 2026-06-20
**Status:** Approved (design); implementing
**Scope:** Decode Hazelcast Compact-serialized values server-side and evaluate
predicate queries (`MapValuesWithPredicate` / `MapKeySetWithPredicate` /
`MapEntriesWithPredicate`) over an IMap — the gated tail of Epic 1 + Epic 5.

## Goal & guiding constraint

Make stock-client object queries work end-to-end: a client stores
Compact-serialized objects and queries by field predicate; the server decodes
the values, evaluates the predicate, and returns the matching keys/values/
entries — byte-compatible with the Hazelcast client.

**No-rewrite mandate.** The MVP must be a strict *subset* of the broad target
(Compact+Portable, full predicate set, indexes). Reaching the broad target must
be additive: new enum variants, new trait impls, an index acceleration layer —
never a rework of the core. The interfaces below are the seams that guarantee
this.

## MVP scope (each axis expands later, additively)

- **Format:** Compact only. (Portable = a second `FieldExtractor` impl later.)
- **Field kinds:** scalars `BOOLEAN`, `INT32`, `INT64`, `FLOAT64`, `STRING`.
  (Arrays/temporal/decimal/nested = more reader match arms + `FieldValue`
  variants later.)
- **Predicates:** `Compare` (`=`, `<`, `<=`, `>`, `>=`) and logical `And`/`Or`.
  (Between/In/Like/Not + `SqlPredicate` parser later — all emit the same AST.)
- **Execution:** full scan. (Indexes later = an alternative candidate-key source;
  scan stays the fallback.)

## Component architecture & data flow

```
MapValuesWithPredicate request
  → decode (map name + predicate Data)
  → PredicateDecoder: predicate Data → Predicate AST
  → QueryExecutor.query(map, pred, want):
        candidates = store.entries(map)         // index seam: an index supplies a subset here
        for (key, valueData) in candidates:
          eval(pred, valueData, schemas, &CompactExtractor) → bool
        collect matches per `want` (Keys|Values|Entries)
  → List<Data> / EntryList<Data,Data> response
```

Schemas arrive out-of-band (before puts) via `ClientSendSchema` and are kept in
a `SchemaService`. The store is unchanged — keys/values remain opaque blobs; the
engine only *reads* value bytes on demand during a query.

### Crate boundaries (testable in isolation)

- **`serialization`** owns: `Schema`, `SchemaService`, the Compact reader,
  `FieldValue`, the `FieldExtractor` trait, and the schema-id fingerprint.
- **`query`** (new crate) owns: the `Predicate` AST, `PredicateDecoder`, and
  `eval` — pure, store-agnostic, fully unit-testable.
- **`codecs`** owns: `SchemaCodec`/`FieldDescriptorCodec` decode, and the
  query/schema request decoders + response encoders.
- **`server`** owns: the `QueryExecutor` (it has the store) and the new op
  handlers.

## Section 1 — Schema service (gating piece)

`ClientSendSchema (4864/4865)` carries a Compact `Schema` = `typeName: String` +
`fields: [FieldDescriptor]` where `FieldDescriptor = (fieldName: String, kind:
FieldKind, ...)`. Handling:
- `SchemaCodec`/`FieldDescriptorCodec` decode the schema into
  `Schema { type_name, fields: HashMap<String, FieldDescriptor>, schema_id }`.
- `schema_id` is computed with the same fingerprint Hazelcast uses (an RABIN
  fingerprint over the canonical schema bytes) so server-computed ids match the
  ids embedded in Compact records.
- `SchemaService` stores `Mutex<HashMap<i64, Schema>>`.
- **Respond** to `ClientSendSchema` with the set of replicating members
  (`ListUUID`) — single-node: `[self.member_uuid]`. (My prior empty ack broke
  exactly this.)
- `ClientFetchSchema (5120/5121)` → return the stored schema (nullable).
- `ClientSendAllSchemas (5376/5377)` → store the batch, empty response.

A Compact `Data` payload begins with its `schemaId`; the reader looks up the
`Schema` to interpret the record. **Unknown schema at query time → that entry is
non-matching** (safe), never an error.

## Section 2 — Compact reader + `FieldExtractor` / `FieldValue`

```rust
pub enum FieldValue { Null, Bool(bool), I32(i32), I64(i64), F64(f64), Str(String) }
pub trait FieldExtractor {
    fn extract(&self, value: &[u8], schemas: &SchemaService, field: &str) -> FieldValue;
}
```

The Compact record (after the `schemaId`) is **fixed-size fields packed at known
offsets**, then **variable-size fields located via an offset table at the tail**.
The schema gives each field its `FieldKind` and position. `CompactExtractor`:
- reads `BOOLEAN/INT32/INT64/FLOAT64` from fixed offsets;
- reads `STRING` from the variable section via the offset table;
- any other kind → `FieldValue::Null` (non-matching) until implemented.

`FieldValue` defines comparison (`partial_cmp` for ordering; `Eq`). This trait +
enum are the **no-rewrite seam**: Portable = another `FieldExtractor` impl; new
kinds = more match arms + `FieldValue` variants; nothing above changes.

## Section 3 — Predicate AST + decoder

```rust
pub enum Op { Eq, Lt, Le, Gt, Ge }
pub enum Predicate {
    Compare { field: String, op: Op, value: FieldValue },
    And(Vec<Predicate>), Or(Vec<Predicate>),
    MatchNone,  // unsupported predicate id → matches nothing (loud + safe)
}
```

The predicate arrives as a serialized `Data` (IdentifiedDataSerializable:
`factoryId` + `classId` + fields). `PredicateDecoder` maps the predicate factory
+ class ids to AST nodes:
- `EqualPredicate` → `Compare{Eq}`; `NotEqualPredicate` → handled as `Eq`
  negation later (MVP: `MatchNone` if not directly supported);
- `GreaterLessPredicate` (carries `equal` + `less` boolean flags) →
  `Compare{Lt|Le|Gt|Ge}`;
- `AndPredicate`/`OrPredicate` (carry a list of nested predicate `Data`) →
  `And`/`Or(decode each child)`.

The compared `value` is a serialized scalar decoded into a `FieldValue`.
**Unsupported class id → `MatchNone`** (empty result, not wrong-and-populated,
not an error). When `SqlPredicate` lands later, its parser emits this same AST.

## Section 4 — Evaluator + QueryExecutor

```rust
// query crate — pure:
pub fn eval(p: &Predicate, value: &[u8], schemas: &SchemaService, ex: &dyn FieldExtractor) -> bool
//   Compare → ex.extract(field) compared to value by Op (type-aware)
//   And/Or  → short-circuit; MatchNone → false
```

```rust
// server — owns the store:
pub enum Want { Keys, Values, Entries }
pub fn query(store: &Store, map: &str, pred: &Predicate, schemas: &SchemaService, want: Want) -> QueryResult
//   candidates = store.entries(map)    // index seam
//   for (k, v): if eval(...) collect per want
```

One executor serves all three query ops. The only thing an index changes later
is the `candidates` source; scan remains the fallback.

## Section 5 — Query op handlers

New dispatch arms (decode `name = frames[1]`, `predicate = frames[2].content`):
- `MapKeySetWithPredicate (75264/75265)` → `query(Keys)` → `List<Data>`.
- `MapValuesWithPredicate (75520/75521)` → `query(Values)` → `List<Data>`.
- `MapEntriesWithPredicate (75776/75777)` → `query(Entries)` →
  `EntryList<Data,Data>`.
Plus the three schema arms (Section 1). Reuses existing list/entry-list response
encoders. The hot path (plain get/put) is untouched.

## Testing

- **Unit (engine, no server):** Compact reader (hand-built record+schema →
  extract each scalar); predicate decoder (hand-built Equal/GreaterLess/And
  `Data` → AST); evaluator (AST × values → bool); unsupported predicate → empty.
- **Golden:** decode a real `Schema` and a real predicate from captured client
  bytes; assert fields / AST.
- **End-to-end (the proof):** a stock Python client with a `CompactSerializer`
  stores `Person{name, age}`; then `values(greater_or_equal("age", 30))`,
  `entry_set(equal("name","alice"))`, and an `and_(...)` — assert the matching
  set is exactly correct. Exercises schema-send → Compact decode → predicate
  decode → eval end-to-end.

## Out of scope (additive later, no rewrite)

Portable/IdentifiedDataSerializable field decode; array/temporal/decimal/nested
field kinds; `SqlPredicate` parser; `Between`/`In`/`Like`/`Not`/`Regex`
predicates; paging predicates; aggregations/projections; indexes (hash/sorted)
and the query optimizer; entry processors. Each plugs into the seams above.
