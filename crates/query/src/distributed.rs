//! Two-phase distributed SQL aggregation. Each member computes a *partial*
//! aggregate over its local partitions (mergeable per-group state: sum, count,
//! min, max); the coordinator merges partials across members and formats the
//! final rows. This is the correctness core of distributed SQL — the member
//! transport that scatters the query and gathers partials wires on top.
//!
//! Splitting the input across members and merging the partials MUST match a
//! single-node run over the whole input (verified by the tests).

use crate::sql::{bare_col, ColExpr, Select};
use crate::{eval, Predicate};
use serialization::compact::{FieldExtractor, FieldValue};
use serialization::schema::SchemaService;
use std::collections::HashMap;

/// Which aggregate a SELECT column computes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// A mergeable per-group, per-aggregate-column accumulator.
#[derive(Clone, Default, Debug, PartialEq)]
pub struct Acc {
    rows: i64, // COUNT(*) — rows folded into this group
    sum: f64,
    nvals: i64, // non-null numeric count (for AVG)
    min: f64,
    max: f64,
    seen: bool,
}

impl Acc {
    fn add_value(&mut self, v: f64) {
        if self.seen {
            self.min = self.min.min(v);
            self.max = self.max.max(v);
        } else {
            self.min = v;
            self.max = v;
            self.seen = true;
        }
        self.sum += v;
        self.nvals += 1;
    }
    fn merge(&mut self, o: &Acc) {
        self.rows += o.rows;
        self.sum += o.sum;
        self.nvals += o.nvals;
        if o.seen {
            if self.seen {
                self.min = self.min.min(o.min);
                self.max = self.max.max(o.max);
            } else {
                self.min = o.min;
                self.max = o.max;
                self.seen = true;
            }
        }
    }
    fn finalize(&self, kind: AggKind) -> Option<String> {
        match kind {
            AggKind::Count => Some(self.rows.to_string()),
            AggKind::Sum => self.seen.then(|| trim(self.sum)),
            AggKind::Avg => (self.nvals > 0).then(|| trim(self.sum / self.nvals as f64)),
            AggKind::Min => self.seen.then(|| trim(self.min)),
            AggKind::Max => self.seen.then(|| trim(self.max)),
        }
    }
}

/// Render an f64 result without a trailing `.0` for whole numbers.
fn trim(v: f64) -> String {
    if v.fract() == 0.0 {
        (v as i64).to_string()
    } else {
        v.to_string()
    }
}

/// One member's partial result: per group key, one [`Acc`] per aggregate column.
#[derive(Clone, Default, Debug, PartialEq)]
pub struct Partial {
    pub groups: HashMap<Vec<Option<String>>, Vec<Acc>>,
}

// ---- Partial wire serialization (for the scatter/gather transport) ----

fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_le_bytes());
}
fn put_i64(b: &mut Vec<u8>, v: i64) {
    b.extend_from_slice(&v.to_le_bytes());
}
fn put_str(b: &mut Vec<u8>, s: &Option<String>) {
    match s {
        Some(s) => {
            b.push(1);
            put_u32(b, s.len() as u32);
            b.extend_from_slice(s.as_bytes());
        }
        None => b.push(0),
    }
}

/// Serialize a partial for transport between members.
pub fn encode_partial(p: &Partial) -> Vec<u8> {
    let mut b = Vec::new();
    put_u32(&mut b, p.groups.len() as u32);
    for (key, accs) in &p.groups {
        put_u32(&mut b, key.len() as u32);
        for k in key {
            put_str(&mut b, k);
        }
        put_u32(&mut b, accs.len() as u32);
        for a in accs {
            put_i64(&mut b, a.rows);
            b.extend_from_slice(&a.sum.to_le_bytes());
            put_i64(&mut b, a.nvals);
            b.extend_from_slice(&a.min.to_le_bytes());
            b.extend_from_slice(&a.max.to_le_bytes());
            b.push(a.seen as u8);
        }
    }
    b
}

struct Rd<'a> {
    b: &'a [u8],
    p: usize,
}
impl Rd<'_> {
    fn u32(&mut self) -> Option<u32> {
        let v = u32::from_le_bytes(self.b.get(self.p..self.p + 4)?.try_into().ok()?);
        self.p += 4;
        Some(v)
    }
    fn i64(&mut self) -> Option<i64> {
        let v = i64::from_le_bytes(self.b.get(self.p..self.p + 8)?.try_into().ok()?);
        self.p += 8;
        Some(v)
    }
    fn f64(&mut self) -> Option<f64> {
        let v = f64::from_le_bytes(self.b.get(self.p..self.p + 8)?.try_into().ok()?);
        self.p += 8;
        Some(v)
    }
    fn u8(&mut self) -> Option<u8> {
        let v = *self.b.get(self.p)?;
        self.p += 1;
        Some(v)
    }
    fn opt_str(&mut self) -> Option<Option<String>> {
        if self.u8()? == 0 {
            return Some(None);
        }
        let n = self.u32()? as usize;
        let s = self.b.get(self.p..self.p + n)?;
        self.p += n;
        Some(Some(String::from_utf8_lossy(s).into_owned()))
    }
}

/// Deserialize a partial produced by [`encode_partial`].
pub fn decode_partial(bytes: &[u8]) -> Option<Partial> {
    let mut r = Rd { b: bytes, p: 0 };
    let ng = r.u32()? as usize;
    let mut groups = HashMap::new();
    for _ in 0..ng {
        let kl = r.u32()? as usize;
        let mut key = Vec::with_capacity(kl);
        for _ in 0..kl {
            key.push(r.opt_str()?);
        }
        let na = r.u32()? as usize;
        let mut accs = Vec::with_capacity(na);
        for _ in 0..na {
            accs.push(Acc {
                rows: r.i64()?,
                sum: r.f64()?,
                nvals: r.i64()?,
                min: r.f64()?,
                max: r.f64()?,
                seen: r.u8()? != 0,
            });
        }
        groups.insert(key, accs);
    }
    Some(Partial { groups })
}

/// True if the query aggregates (so members return partials, not rows).
pub fn is_aggregate(select: &Select) -> bool {
    select.window.is_some()
        || select.group_by.is_some()
        || select.cols.iter().any(|c| !matches!(c, ColExpr::Col(_)))
}

/// Serialize a batch of result rows for the row-gather path.
pub fn encode_rows(rows: &[Vec<Option<String>>]) -> Vec<u8> {
    let mut b = Vec::new();
    put_u32(&mut b, rows.len() as u32);
    for row in rows {
        put_u32(&mut b, row.len() as u32);
        for cell in row {
            put_str(&mut b, cell);
        }
    }
    b
}

/// Deserialize rows produced by [`encode_rows`].
pub fn decode_rows(bytes: &[u8]) -> Option<Vec<Vec<Option<String>>>> {
    let mut r = Rd { b: bytes, p: 0 };
    let nrows = r.u32()? as usize;
    let mut rows = Vec::with_capacity(nrows);
    for _ in 0..nrows {
        let nc = r.u32()? as usize;
        let mut row = Vec::with_capacity(nc);
        for _ in 0..nc {
            row.push(r.opt_str()?);
        }
        rows.push(row);
    }
    Some(rows)
}

/// Column names for a plain (non-aggregate) projected SELECT.
pub fn plain_columns(select: &Select) -> Vec<String> {
    select
        .cols
        .iter()
        .map(|c| match c {
            ColExpr::Col(n) => bare_col(n),
            _ => String::new(),
        })
        .collect()
}

/// Merge gathered plain rows at the coordinator: concat, then DISTINCT / ORDER BY
/// / LIMIT (applied once over the whole cluster, not per member).
pub fn finalize_rows(
    select: &Select,
    mut rows: Vec<Vec<Option<String>>>,
) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    if select.is_distinct {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|r| seen.insert(r.clone()));
    }
    if let Some((col, desc)) = &select.order_by {
        let cols = plain_columns(select);
        if let Some(idx) = cols.iter().position(|c| c == &bare_col(col)) {
            rows.sort_by(|a, b| {
                let ord = num_or_str(&a.get(idx).cloned().flatten())
                    .partial_cmp(&num_or_str(&b.get(idx).cloned().flatten()))
                    .unwrap_or(std::cmp::Ordering::Equal);
                if *desc {
                    ord.reverse()
                } else {
                    ord
                }
            });
        }
    }
    if let Some(n) = select.limit {
        rows.truncate(n);
    }
    (plain_columns(select), rows)
}

/// The aggregate columns of a SELECT, in order (used to align `Acc`s).
fn agg_cols(select: &Select) -> Vec<(AggKind, String)> {
    select
        .cols
        .iter()
        .filter_map(|c| match c {
            ColExpr::Count(n) => Some((AggKind::Count, bare_col(n))),
            ColExpr::Sum(n) => Some((AggKind::Sum, bare_col(n))),
            ColExpr::Avg(n) => Some((AggKind::Avg, bare_col(n))),
            ColExpr::Min(n) => Some((AggKind::Min, bare_col(n))),
            ColExpr::Max(n) => Some((AggKind::Max, bare_col(n))),
            ColExpr::Col(_) => None,
        })
        .collect()
}

/// The effective grouping columns (window_start/window_end prepended when the
/// query windows). Mirrors the executor.
fn group_cols(select: &Select) -> Vec<String> {
    let mut g = select.group_by.clone().unwrap_or_default();
    if select.window.is_some() {
        g.retain(|c| c != "window_start" && c != "window_end");
        g.insert(0, "window_end".to_string());
        g.insert(0, "window_start".to_string());
    }
    g
}

fn extract_num(
    ex: &dyn FieldExtractor,
    v: &[u8],
    schemas: &SchemaService,
    col: &str,
) -> Option<f64> {
    match ex.extract(v, schemas, col) {
        FieldValue::I32(x) => Some(x as f64),
        FieldValue::I64(x) => Some(x as f64),
        FieldValue::F64(x) => Some(x),
        _ => None,
    }
}

/// Compute this member's partial aggregate over its local `entries`.
pub fn local_partial(
    select: &Select,
    entries: &[(Vec<u8>, Vec<u8>)],
    schemas: &SchemaService,
    ex: &dyn FieldExtractor,
    key_col: Option<&str>,
) -> Partial {
    let matchall = Predicate::And(vec![]);
    let filter = select.filter.as_ref().unwrap_or(&matchall);
    let gcols = group_cols(select);
    let aggs = agg_cols(select);
    let mut groups: HashMap<Vec<Option<String>>, Vec<Acc>> = HashMap::new();

    for (k, v) in entries.iter().filter(|(_, v)| eval(filter, v, schemas, ex)) {
        // Window buckets (one sentinel when not windowed).
        let buckets: Vec<(Option<String>, Option<String>)> = match &select.window {
            Some(w) => {
                let ts = match ex.extract(v, schemas, &w.ts_col) {
                    FieldValue::I32(x) => x as i64,
                    FieldValue::I64(x) => x,
                    FieldValue::F64(x) => x as i64,
                    _ => 0,
                };
                w.windows(ts)
                    .into_iter()
                    .map(|(s, e)| (Some(s.to_string()), Some(e.to_string())))
                    .collect()
            }
            None => vec![(None, None)],
        };
        for (ws, we) in buckets {
            let key: Vec<Option<String>> = gcols
                .iter()
                .map(|col| match col.as_str() {
                    "window_start" if select.window.is_some() => ws.clone(),
                    "window_end" if select.window.is_some() => we.clone(),
                    _ if Some(col.as_str()) == key_col => fmt(&crate::json::decode_key_data(k)),
                    _ => fmt(&ex.extract(v, schemas, col)),
                })
                .collect();
            let accs = groups
                .entry(key)
                .or_insert_with(|| vec![Acc::default(); aggs.len()]);
            for (i, (kind, src)) in aggs.iter().enumerate() {
                accs[i].rows += 1;
                if *kind != AggKind::Count {
                    if let Some(x) = extract_num(ex, v, schemas, src) {
                        accs[i].add_value(x);
                    }
                }
            }
        }
    }
    Partial { groups }
}

/// Merge partial results from several members into one.
pub fn merge(partials: Vec<Partial>) -> Partial {
    let mut out: HashMap<Vec<Option<String>>, Vec<Acc>> = HashMap::new();
    for p in partials {
        for (key, accs) in p.groups {
            let e = out
                .entry(key)
                .or_insert_with(|| vec![Acc::default(); accs.len()]);
            for (i, a) in accs.iter().enumerate() {
                if i < e.len() {
                    e[i].merge(a);
                }
            }
        }
    }
    Partial { groups: out }
}

/// Format the merged partial into result columns + rows, honoring ORDER BY/LIMIT.
pub fn finalize(select: &Select, merged: &Partial) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    let gcols = group_cols(select);
    let columns: Vec<String> = select
        .cols
        .iter()
        .map(|c| match c {
            ColExpr::Col(n) => bare_col(n),
            ColExpr::Count(n) => format!("count({})", bare_col(n)),
            ColExpr::Sum(n) => format!("sum({})", bare_col(n)),
            ColExpr::Avg(n) => format!("avg({})", bare_col(n)),
            ColExpr::Min(n) => format!("min({})", bare_col(n)),
            ColExpr::Max(n) => format!("max({})", bare_col(n)),
        })
        .collect();

    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    for (key, accs) in &merged.groups {
        let mut row = Vec::new();
        let mut ai = 0;
        for c in &select.cols {
            match c {
                ColExpr::Col(n) => {
                    let b = bare_col(n);
                    let v = gcols
                        .iter()
                        .position(|g| g == &b)
                        .and_then(|idx| key.get(idx).cloned().flatten());
                    row.push(v);
                }
                ColExpr::Count(_) => {
                    row.push(accs[ai].finalize(AggKind::Count));
                    ai += 1;
                }
                ColExpr::Sum(_) => {
                    row.push(accs[ai].finalize(AggKind::Sum));
                    ai += 1;
                }
                ColExpr::Avg(_) => {
                    row.push(accs[ai].finalize(AggKind::Avg));
                    ai += 1;
                }
                ColExpr::Min(_) => {
                    row.push(accs[ai].finalize(AggKind::Min));
                    ai += 1;
                }
                ColExpr::Max(_) => {
                    row.push(accs[ai].finalize(AggKind::Max));
                    ai += 1;
                }
            }
        }
        rows.push(row);
    }

    if let Some((col, desc)) = &select.order_by {
        if let Some(idx) = columns.iter().position(|c| c == &bare_col(col)) {
            rows.sort_by(|a, b| {
                let (x, y) = (a.get(idx).cloned().flatten(), b.get(idx).cloned().flatten());
                let ord = num_or_str(&x)
                    .partial_cmp(&num_or_str(&y))
                    .unwrap_or(std::cmp::Ordering::Equal);
                if *desc {
                    ord.reverse()
                } else {
                    ord
                }
            });
        }
    } else {
        rows.sort();
    }
    if let Some(n) = select.limit {
        rows.truncate(n);
    }
    (columns, rows)
}

/// Comparison key: numeric if parseable, else the string (None sorts first).
fn num_or_str(v: &Option<String>) -> (f64, String) {
    match v {
        Some(s) => (s.parse().unwrap_or(f64::INFINITY), s.clone()),
        None => (f64::NEG_INFINITY, String::new()),
    }
}

fn fmt(v: &FieldValue) -> Option<String> {
    crate::sql::fmt_value(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parse_select;

    struct Kv;
    impl FieldExtractor for Kv {
        fn extract(&self, value: &[u8], _s: &SchemaService, field: &str) -> FieldValue {
            let s = std::str::from_utf8(value).unwrap_or("");
            for pair in s.split(',') {
                if let Some((k, v)) = pair.split_once('=') {
                    if k == field {
                        return v
                            .parse::<i64>()
                            .map(FieldValue::I64)
                            .unwrap_or_else(|_| FieldValue::Str(v.into()));
                    }
                }
            }
            FieldValue::Null
        }
    }

    fn e(s: &str) -> (Vec<u8>, Vec<u8>) {
        (b"k".to_vec(), s.as_bytes().to_vec())
    }

    #[test]
    fn split_then_merge_equals_single_node() {
        let schemas = SchemaService::new();
        let sel = parse_select(
            "SELECT region, SUM(amount), COUNT(amount), MIN(amount), MAX(amount), AVG(amount) \
             FROM sales GROUP BY region ORDER BY region",
        )
        .unwrap();

        // Data split across two members.
        let m1 = vec![
            e("region=us,amount=10"),
            e("region=us,amount=30"),
            e("region=eu,amount=5"),
        ];
        let m2 = vec![e("region=us,amount=20"), e("region=eu,amount=15")];

        let p1 = local_partial(&sel, &m1, &schemas, &Kv, None);
        let p2 = local_partial(&sel, &m2, &schemas, &Kv, None);
        let (cols, rows) = finalize(&sel, &merge(vec![p1, p2]));

        assert_eq!(cols[0], "region");
        // eu: {5,15} sum20 cnt2 min5 max15 avg10 ; us: {10,30,20} sum60 cnt3 min10 max30 avg20
        assert_eq!(
            rows,
            vec![
                vec![
                    Some("eu".into()),
                    Some("20".into()),
                    Some("2".into()),
                    Some("5".into()),
                    Some("15".into()),
                    Some("10".into())
                ],
                vec![
                    Some("us".into()),
                    Some("60".into()),
                    Some("3".into()),
                    Some("10".into()),
                    Some("30".into()),
                    Some("20".into())
                ],
            ]
        );
    }

    #[test]
    fn matches_single_node_run() {
        // Merging two partials must equal running over the concatenated input.
        let schemas = SchemaService::new();
        let sel = parse_select("SELECT dept, SUM(sal) FROM emp GROUP BY dept").unwrap();
        let a = vec![e("dept=x,sal=100"), e("dept=y,sal=50")];
        let b = vec![e("dept=x,sal=200")];
        let dist = finalize(
            &sel,
            &merge(vec![
                local_partial(&sel, &a, &schemas, &Kv, None),
                local_partial(&sel, &b, &schemas, &Kv, None),
            ]),
        );
        let mut all = a.clone();
        all.extend(b);
        let single = finalize(&sel, &local_partial(&sel, &all, &schemas, &Kv, None));
        assert_eq!(dist, single);
    }

    #[test]
    fn partial_wire_roundtrip() {
        let schemas = SchemaService::new();
        let sel =
            parse_select("SELECT region, SUM(amount), MIN(amount) FROM s GROUP BY region").unwrap();
        let m = vec![e("region=us,amount=10"), e("region=eu,amount=5")];
        let p = local_partial(&sel, &m, &schemas, &Kv, None);
        let back = decode_partial(&encode_partial(&p)).expect("decodes");
        assert_eq!(back, p);
        // A partial sent over the wire merges identically to the in-memory one.
        assert_eq!(finalize(&sel, &merge(vec![back])), finalize(&sel, &p));
    }

    #[test]
    fn distributed_windowed_aggregation() {
        let schemas = SchemaService::new();
        let sel = parse_select(
            "SELECT window_start, SUM(amount) FROM TUMBLE(s, ts, 10) GROUP BY window_start ORDER BY window_start",
        )
        .unwrap();
        let m1 = vec![e("ts=1,amount=5")]; // window [0,10)
        let m2 = vec![e("ts=3,amount=7"), e("ts=11,amount=4")]; // [0,10) and [10,20)
        let (_, rows) = finalize(
            &sel,
            &merge(vec![
                local_partial(&sel, &m1, &schemas, &Kv, None),
                local_partial(&sel, &m2, &schemas, &Kv, None),
            ]),
        );
        assert_eq!(
            rows,
            vec![
                vec![Some("0".into()), Some("12".into())],
                vec![Some("10".into()), Some("4".into())],
            ]
        );
    }
}

#[cfg(test)]
mod row_tests {
    use super::*;
    use crate::sql::parse_select;

    #[test]
    fn rows_wire_roundtrip_and_finalize() {
        let rows = vec![
            vec![Some("us".into()), Some("10".into())],
            vec![Some("eu".into()), None],
        ];
        assert_eq!(decode_rows(&encode_rows(&rows)).unwrap(), rows);

        // Concat two members' rows, DISTINCT + ORDER BY + LIMIT at the coordinator.
        let sel = parse_select("SELECT DISTINCT region FROM m ORDER BY region LIMIT 2").unwrap();
        let a = vec![vec![Some("us".into())], vec![Some("eu".into())]];
        let b = vec![vec![Some("us".into())], vec![Some("ap".into())]]; // us duplicate
        let mut all = a;
        all.extend(b);
        let (cols, out) = finalize_rows(&sel, all);
        assert_eq!(cols, vec!["region"]);
        assert_eq!(
            out,
            vec![vec![Some("ap".into())], vec![Some("eu".into())]] // distinct+sorted, limited to 2
        );
    }
}
