//! A minimal SQL surface: `SELECT <cols|*> FROM <map> [WHERE <cond> [AND ...]]`
//! over Compact IMap values. Reuses the predicate evaluator and Compact extractor.
//! Columns are returned as text (VARCHAR) — enough to make the SQL API work; full
//! typing/optimization/joins are out of scope.

use crate::{eval, Op, Predicate};
use serialization::compact::{AutoExtractor, FieldExtractor, FieldValue};
use serialization::schema::SchemaService;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColType {
    Varchar,
    Int,
    Bigint,
    Double,
    Boolean,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MappingKind {
    Imap,
    Kafka,
}

/// A `CREATE MAPPING` definition (catalog entry).
#[derive(Clone, Debug, PartialEq)]
pub struct Mapping {
    pub name: String,
    pub kind: MappingKind,
    pub columns: Vec<(String, ColType)>,
    pub options: HashMap<String, String>,
}

impl Mapping {
    pub fn option(&self, k: &str) -> Option<&str> {
        self.options.get(k).map(|s| s.as_str())
    }
    pub fn value_format(&self) -> &str {
        self.option("valueFormat").unwrap_or("compact")
    }
}

/// An `INSERT INTO <mapping> VALUES (..),(..)`.
#[derive(Clone, Debug, PartialEq)]
pub struct Insert {
    pub mapping: String,
    pub rows: Vec<Vec<FieldValue>>,
}

/// A `CREATE JOB <name> AS SINK INTO <sink> <select>`.
#[derive(Clone, Debug, PartialEq)]
pub struct Job {
    pub name: String,
    pub sink: String,
    pub select: Select,
}

#[derive(Debug, PartialEq)]
pub enum Statement {
    Select(Select),
    CreateMapping(Mapping),
    Insert(Insert),
    CreateJob(Job),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ColExpr {
    Col(String),
    Count(String),
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

/// A join clause: `JOIN <right> ON <left_col> = <right_col>`.
#[derive(Clone, Debug, PartialEq)]
pub struct Join {
    pub right: String,
    pub left_col: String,
    pub right_col: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Select {
    pub is_distinct: bool,
    pub cols: Vec<ColExpr>,
    pub star: bool,
    pub map: String,
    pub join: Option<Join>,
    pub filter: Option<Predicate>,
    pub group_by: Option<Vec<String>>,
    pub order_by: Option<(String, bool)>, // (col, is_desc)
    pub limit: Option<usize>,
}

/// Parse any supported statement.
pub fn parse(sql: &str) -> Option<Statement> {
    let mut t = Tokenizer::new(sql);
    if t.keyword("select").is_some() {
        return Some(Statement::Select(parse_select_body(&mut t)?));
    }
    if t.keyword("insert").is_some() {
        return Some(Statement::Insert(parse_insert(&mut t)?));
    }
    if t.keyword("create").is_some() {
        if t.keyword("mapping").is_some() {
            return Some(Statement::CreateMapping(parse_create_mapping(&mut t)?));
        }
        if t.keyword("job").is_some() {
            return Some(Statement::CreateJob(parse_create_job(&mut t)?));
        }
    }
    None
}

/// Convenience for callers/tests that only want a SELECT.
pub fn parse_select(sql: &str) -> Option<Select> {
    match parse(sql)? {
        Statement::Select(s) => Some(s),
        _ => None,
    }
}

fn parse_col_expr(t: &mut Tokenizer) -> Option<ColExpr> {
    t.skip_ws();
    if t.keyword("count").is_some() {
        t.symbol("(");
        let arg = if t.symbol("*") { "*".to_string() } else { t.col_ref()? };
        t.symbol(")");
        Some(ColExpr::Count(arg))
    } else if t.keyword("sum").is_some() {
        t.symbol("(");
        let arg = t.col_ref()?;
        t.symbol(")");
        Some(ColExpr::Sum(arg))
    } else if t.keyword("avg").is_some() {
        t.symbol("(");
        let arg = t.col_ref()?;
        t.symbol(")");
        Some(ColExpr::Avg(arg))
    } else if t.keyword("min").is_some() {
        t.symbol("(");
        let arg = t.col_ref()?;
        t.symbol(")");
        Some(ColExpr::Min(arg))
    } else if t.keyword("max").is_some() {
        t.symbol("(");
        let arg = t.col_ref()?;
        t.symbol(")");
        Some(ColExpr::Max(arg))
    } else {
        let name = t.col_ref()?;
        Some(ColExpr::Col(name))
    }
}

fn parse_select_body(t: &mut Tokenizer) -> Option<Select> {
    let is_distinct = t.keyword("distinct").is_some();
    let mut cols = Vec::new();
    let mut star = false;
    if t.symbol("*") {
        star = true;
    } else {
        cols.push(parse_col_expr(t)?);
        while t.symbol(",") {
            cols.push(parse_col_expr(t)?);
        }
    }
    t.keyword("from")?;
    let map = t.ident()?;
    let join = if t.keyword("join").is_some() {
        let right = t.ident()?;
        t.keyword("on")?;
        let a = t.col_ref()?;
        t.op()?; // '=' (only equi-join supported)
        let b = t.col_ref()?;
        // Normalize: left col belongs to `map`, right col to `right`.
        let (left_col, right_col) = split_join_cols(&map, &right, &a, &b);
        Some(Join { right, left_col, right_col })
    } else {
        None
    };
    let filter = if t.keyword("where").is_some() { Some(parse_conds(t)?) } else { None };
    
    let mut group_by = None;
    if t.keyword("group").is_some() {
        t.keyword("by")?;
        let mut gcols = vec![t.ident()?];
        while t.symbol(",") {
            gcols.push(t.ident()?);
        }
        group_by = Some(gcols);
    }
    
    let mut order_by = None;
    if t.keyword("order").is_some() {
        t.keyword("by")?;
        let col = t.ident()?;
        let is_desc = if t.keyword("desc").is_some() {
            true
        } else {
            let _ = t.keyword("asc");
            false
        };
        order_by = Some((col, is_desc));
    }
    
    let mut limit = None;
    if t.keyword("limit").is_some() {
        t.skip_ws();
        let start = t.i;
        while t.i < t.s.len() && t.s[t.i].is_ascii_digit() {
            t.i += 1;
        }
        let num_str = std::str::from_utf8(&t.s[start..t.i]).ok()?;
        limit = Some(num_str.parse::<usize>().ok()?);
    }
    
    Some(Select { is_distinct, cols, star, map, join, filter, group_by, order_by, limit })
}

/// Given `a` and `b` from `ON a = b`, return (col-on-left, col-on-right) using the
/// `table.` qualifier when present.
fn split_join_cols(left: &str, right: &str, a: &str, b: &str) -> (String, String) {
    let bare = |c: &str| c.rsplit('.').next().unwrap_or(c).to_string();
    let on = |c: &str, tbl: &str| c.starts_with(&format!("{tbl}."));
    if on(a, right) || on(b, left) {
        (bare(b), bare(a))
    } else {
        (bare(a), bare(b))
    }
}

fn parse_create_mapping(t: &mut Tokenizer) -> Option<Mapping> {
    let name = t.ident()?;
    let mut columns = Vec::new();
    if t.symbol("(") {
        loop {
            let col = t.ident()?;
            let ty = parse_coltype(t)?;
            columns.push((col, ty));
            if t.symbol(")") {
                break;
            }
            t.symbol(",");
        }
    }
    t.keyword("type")?;
    let kind = match t.ident()?.to_ascii_lowercase().as_str() {
        "kafka" => MappingKind::Kafka,
        _ => MappingKind::Imap,
    };
    let mut options = HashMap::new();
    if t.keyword("options").is_some() && t.symbol("(") {
        loop {
            let k = t.string_lit()?;
            t.op(); // '='
            let v = t.string_lit()?;
            options.insert(k, v);
            if t.symbol(")") {
                break;
            }
            t.symbol(",");
        }
    }
    Some(Mapping { name, kind, columns, options })
}

fn parse_coltype(t: &mut Tokenizer) -> Option<ColType> {
    let id = t.ident()?.to_ascii_lowercase();
    Some(match id.as_str() {
        "int" | "integer" => ColType::Int,
        "bigint" => ColType::Bigint,
        "double" => ColType::Double,
        "boolean" => ColType::Boolean,
        _ => ColType::Varchar,
    })
}

fn parse_insert(t: &mut Tokenizer) -> Option<Insert> {
    t.keyword("into")?;
    let mapping = t.ident()?;
    // optional column list — ignored (positional)
    if t.symbol("(") {
        loop {
            t.ident()?;
            if t.symbol(")") {
                break;
            }
            t.symbol(",");
        }
    }
    t.keyword("values")?;
    let mut rows = Vec::new();
    loop {
        if !t.symbol("(") {
            break;
        }
        let mut row = vec![t.literal()?];
        while t.symbol(",") {
            row.push(t.literal()?);
        }
        t.symbol(")");
        rows.push(row);
        if !t.symbol(",") {
            break;
        }
    }
    Some(Insert { mapping, rows })
}

fn parse_create_job(t: &mut Tokenizer) -> Option<Job> {
    let name = t.ident()?;
    t.keyword("as")?;
    t.keyword("sink")?;
    t.keyword("into")?;
    let sink = t.ident()?;
    t.keyword("select")?;
    let select = parse_select_body(t)?;
    Some(Job { name, sink, select })
}

fn parse_conds(t: &mut Tokenizer) -> Option<Predicate> {
    let mut conds = vec![parse_cond(t)?];
    while t.keyword("and").is_some() {
        conds.push(parse_cond(t)?);
    }
    if conds.len() == 1 {
        conds.pop()
    } else {
        Some(Predicate::And(conds))
    }
}

fn parse_cond(t: &mut Tokenizer) -> Option<Predicate> {
    let field = t.ident()?;
    let op = t.op()?;
    let value = t.literal()?;
    Some(Predicate::Compare { field, op, value })
}

/// Execute `select` over `(key, value)` IMap entries with the given field
/// extractor (Compact or JSON). `star_cols` supplies column names for `SELECT *`
/// (e.g. a mapping's declared columns); empty falls back to the Compact schema.
/// Returns the column names and the rows (each cell is text or NULL).
pub fn execute_with(
    select: &Select,
    entries: &[(Vec<u8>, Vec<u8>)],
    schemas: &SchemaService,
    ex: &dyn FieldExtractor,
    star_cols: &[String],
    key_col: Option<&str>,
) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    let matchall = Predicate::And(vec![]); // empty AND matches everything
    let filter = select.filter.as_ref().unwrap_or(&matchall);

    let matched: Vec<&(Vec<u8>, Vec<u8>)> =
        entries.iter().filter(|(_, v)| eval(filter, v, schemas, ex)).collect();

    let mut columns = Vec::new();
    if select.star {
        if !star_cols.is_empty() {
            columns = star_cols.to_vec();
        } else {
            columns = matched.first().and_then(|(_, v)| schema_fields(v, schemas)).unwrap_or_default();
        }
    } else {
        for c in &select.cols {
            match c {
                ColExpr::Col(n) => columns.push(bare_col(n)),
                ColExpr::Count(n) => columns.push(format!("count({})", bare_col(n))),
                ColExpr::Sum(n) => columns.push(format!("sum({})", bare_col(n))),
                ColExpr::Avg(n) => columns.push(format!("avg({})", bare_col(n))),
                ColExpr::Min(n) => columns.push(format!("min({})", bare_col(n))),
                ColExpr::Max(n) => columns.push(format!("max({})", bare_col(n))),
            }
        }
    }

    let has_aggregates = !select.star && select.cols.iter().any(|c| !matches!(c, ColExpr::Col(_)));
    let mut rows = Vec::new();

    if has_aggregates || select.group_by.is_some() {
        let group_cols = select.group_by.clone().unwrap_or_default();
        let mut groups: HashMap<Vec<Option<String>>, Vec<&(Vec<u8>, Vec<u8>)>> = HashMap::new();
        
        for entry in &matched {
            let mut group_key = Vec::new();
            for col in &group_cols {
                let val = if Some(col.as_str()) == key_col {
                    fmt(crate::json::decode_key_data(&entry.0))
                } else {
                    fmt(ex.extract(&entry.1, schemas, col))
                };
                group_key.push(val);
            }
            groups.entry(group_key).or_default().push(entry);
        }
        
        for (group_key, group_entries) in groups {
            let mut row = Vec::new();
            if select.star {
                continue;
            }
            for col_expr in &select.cols {
                match col_expr {
                    ColExpr::Col(n) => {
                        let b = bare_col(n);
                        let opt_idx = group_cols.iter().position(|gc| gc == &b);
                        if let Some(idx) = opt_idx {
                            row.push(group_key[idx].clone());
                        } else if let Some(entry) = group_entries.first() {
                            let val = if Some(b.as_str()) == key_col {
                                fmt(crate::json::decode_key_data(&entry.0))
                            } else {
                                fmt(ex.extract(&entry.1, schemas, &b))
                            };
                            row.push(val);
                        } else {
                            row.push(None);
                        }
                    }
                    ColExpr::Count(_) => {
                        row.push(Some(group_entries.len().to_string()));
                    }
                    ColExpr::Sum(n) => {
                        let b = bare_col(n);
                        let mut sum = 0.0;
                        let mut has_val = false;
                        for entry in &group_entries {
                            let val = if Some(b.as_str()) == key_col {
                                crate::json::decode_key_data(&entry.0)
                            } else {
                                ex.extract(&entry.1, schemas, &b)
                            };
                            match val {
                                FieldValue::I32(v) => { sum += v as f64; has_val = true; }
                                FieldValue::I64(v) => { sum += v as f64; has_val = true; }
                                FieldValue::F64(v) => { sum += v; has_val = true; }
                                _ => {}
                            }
                        }
                        row.push(if has_val { Some(sum.to_string()) } else { None });
                    }
                    ColExpr::Avg(n) => {
                        let b = bare_col(n);
                        let mut sum = 0.0;
                        let mut count = 0;
                        for entry in &group_entries {
                            let val = if Some(b.as_str()) == key_col {
                                crate::json::decode_key_data(&entry.0)
                            } else {
                                ex.extract(&entry.1, schemas, &b)
                            };
                            match val {
                                FieldValue::I32(v) => { sum += v as f64; count += 1; }
                                FieldValue::I64(v) => { sum += v as f64; count += 1; }
                                FieldValue::F64(v) => { sum += v; count += 1; }
                                _ => {}
                            }
                        }
                        row.push(if count > 0 { Some((sum / count as f64).to_string()) } else { None });
                    }
                    ColExpr::Min(n) => {
                        let b = bare_col(n);
                        let mut min_fv: Option<FieldValue> = None;
                        for entry in &group_entries {
                            let val = if Some(b.as_str()) == key_col {
                                crate::json::decode_key_data(&entry.0)
                            } else {
                                ex.extract(&entry.1, schemas, &b)
                            };
                            if val != FieldValue::Null {
                                if let Some(ref m) = min_fv {
                                    if val.compare(m) == Some(std::cmp::Ordering::Less) {
                                        min_fv = Some(val);
                                    }
                                } else {
                                    min_fv = Some(val);
                                }
                            }
                        }
                        row.push(min_fv.and_then(|v| fmt_value(&v)));
                    }
                    ColExpr::Max(n) => {
                        let b = bare_col(n);
                        let mut max_fv: Option<FieldValue> = None;
                        for entry in &group_entries {
                            let val = if Some(b.as_str()) == key_col {
                                crate::json::decode_key_data(&entry.0)
                            } else {
                                ex.extract(&entry.1, schemas, &b)
                            };
                            if val != FieldValue::Null {
                                if let Some(ref m) = max_fv {
                                    if val.compare(m) == Some(std::cmp::Ordering::Greater) {
                                        max_fv = Some(val);
                                    }
                                } else {
                                    max_fv = Some(val);
                                }
                            }
                        }
                        row.push(max_fv.and_then(|v| fmt_value(&v)));
                    }
                }
            }
            rows.push(row);
        }
    } else {
        for entry in &matched {
            let mut row = Vec::new();
            if select.star {
                for c in &columns {
                    let val = if Some(c.as_str()) == key_col {
                        fmt(crate::json::decode_key_data(&entry.0))
                    } else {
                        fmt(ex.extract(&entry.1, schemas, c))
                    };
                    row.push(val);
                }
            } else {
                for col_expr in &select.cols {
                    match col_expr {
                        ColExpr::Col(n) => {
                            let b = bare_col(n);
                            let val = if Some(b.as_str()) == key_col {
                                fmt(crate::json::decode_key_data(&entry.0))
                            } else {
                                fmt(ex.extract(&entry.1, schemas, &b))
                            };
                            row.push(val);
                        }
                        _ => {}
                    }
                }
            }
            rows.push(row);
        }
    }

    if select.is_distinct {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|r| seen.insert(r.clone()));
    }

    if let Some((order_col, is_desc)) = &select.order_by {
        if let Some(col_idx) = columns.iter().position(|c| c == order_col) {
            rows.sort_by(|a, b| {
                let val_a = &a[col_idx];
                let val_b = &b[col_idx];
                let ord = match (val_a.as_ref().and_then(|s| s.parse::<f64>().ok()), 
                                 val_b.as_ref().and_then(|s| s.parse::<f64>().ok())) {
                    (Some(na), Some(nb)) => na.total_cmp(&nb),
                    _ => val_a.cmp(val_b),
                };
                if *is_desc {
                    ord.reverse()
                } else {
                    ord
                }
            });
        }
    }

    if let Some(limit) = select.limit {
        rows.truncate(limit);
    }

    (columns, rows)
}

/// Convenience: execute a Compact SELECT (no mapping).
pub fn execute(
    select: &Select,
    entries: &[(Vec<u8>, Vec<u8>)],
    schemas: &SchemaService,
) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    execute_with(select, entries, schemas, &AutoExtractor, &[], None)
}

/// Strip a `table.` qualifier from a column reference.
pub fn bare_col(c: &str) -> String {
    c.rsplit('.').next().unwrap_or(c).to_string()
}

/// Evaluate a predicate against a flat `(name, value)` field list (used by the
/// JOIN / streaming path, where rows are field maps rather than Compact blobs).
pub fn eval_fields(pred: &Predicate, fields: &[(String, FieldValue)]) -> bool {
    use std::cmp::Ordering;
    let get = |name: &str| {
        let b = bare_col(name);
        fields.iter().find(|(k, _)| *k == b).map(|(_, v)| v)
    };
    match pred {
        Predicate::Compare { field, op, value } => match get(field) {
            Some(fv) => match fv.compare(value) {
                Some(o) => match op {
                    Op::Eq => o == Ordering::Equal,
                    Op::NotEq => o != Ordering::Equal,
                    Op::Lt => o == Ordering::Less,
                    Op::Le => o != Ordering::Greater,
                    Op::Gt => o == Ordering::Greater,
                    Op::Ge => o != Ordering::Less,
                },
                None => false,
            },
            None => false,
        },
        Predicate::And(c) => c.iter().all(|p| eval_fields(p, fields)),
        Predicate::Or(c) => c.iter().any(|p| eval_fields(p, fields)),
        Predicate::Not(inner) => !eval_fields(inner, fields),
        Predicate::Between { field, from, to } => match get(field) {
            Some(fv) => match (fv.compare(from), fv.compare(to)) {
                (Some(o_from), Some(o_to)) => {
                    o_from != Ordering::Less && o_to != Ordering::Greater
                }
                _ => false,
            },
            None => false,
        },
        Predicate::In { field, values } => match get(field) {
            Some(fv) => values.iter().any(|v| fv.equals(v)),
            None => false,
        },
        Predicate::Like { field, expr } => match get(field) {
            Some(FieldValue::Str(s)) => {
                if let Ok(re) = crate::eval::like_to_regex(expr, false) {
                    re.is_match(s)
                } else {
                    false
                }
            }
            _ => false,
        },
        Predicate::ILike { field, expr } => match get(field) {
            Some(FieldValue::Str(s)) => {
                if let Ok(re) = crate::eval::like_to_regex(expr, true) {
                    re.is_match(s)
                } else {
                    false
                }
            }
            _ => false,
        },
        Predicate::Regex { field, regex } => match get(field) {
            Some(FieldValue::Str(s)) => {
                if let Ok(re) = crate::eval::compile_regex(regex) {
                    re.is_match(s)
                } else {
                    false
                }
            }
            _ => false,
        },
        Predicate::True => true,
        Predicate::False => false,
        Predicate::Sql(_) => false,
        Predicate::Paging { inner, .. } => {
            if let Some(inner_pred) = inner {
                eval_fields(inner_pred, fields)
            } else {
                true
            }
        }
        Predicate::Partition { target, .. } => eval_fields(target, fields),
        Predicate::MatchNone => false,
    }
}

/// Apply `select`'s WHERE to a combined field row and project its output columns.
/// Returns the output `(column, value)` row, or None if filtered out. `SELECT *`
/// emits all fields in order.
pub fn project_row(select: &Select, fields: &[(String, FieldValue)]) -> Option<Vec<(String, FieldValue)>> {
    let matchall = Predicate::And(vec![]);
    let filter = select.filter.as_ref().unwrap_or(&matchall);
    if !eval_fields(filter, fields) {
        return None;
    }
    let mut cols = Vec::new();
    if select.star {
        for (k, _) in fields {
            cols.push(k.clone());
        }
    } else {
        for expr in &select.cols {
            if let ColExpr::Col(n) = expr {
                cols.push(bare_col(n));
            }
        }
    }
    Some(
        cols.iter()
            .map(|c| {
                let v = fields.iter().find(|(k, _)| k == c).map(|(_, v)| v.clone()).unwrap_or(FieldValue::Null);
                (c.clone(), v)
            })
            .collect(),
    )
}

fn schema_fields(value: &[u8], schemas: &SchemaService) -> Option<Vec<String>> {
    if value.len() < serialization::DATA_OFFSET + 8 {
        return None;
    }
    let payload = &value[serialization::DATA_OFFSET..];
    let id = i64::from_be_bytes(payload[0..8].try_into().ok()?);
    schemas.get(id).map(|s| s.fields.iter().map(|f| f.name.clone()).collect())
}

fn fmt(v: FieldValue) -> Option<String> {
    fmt_value(&v)
}

/// Render a field value as result text (NULL → None).
pub fn fmt_value(v: &FieldValue) -> Option<String> {
    match v {
        FieldValue::Null => None,
        FieldValue::Bool(b) => Some(b.to_string()),
        FieldValue::I32(i) => Some(i.to_string()),
        FieldValue::I64(i) => Some(i.to_string()),
        FieldValue::F64(f) => Some(f.to_string()),
        FieldValue::Str(s) => Some(s.clone()),
    }
}

// ---- tiny tokenizer ----
struct Tokenizer<'a> {
    s: &'a [u8],
    i: usize,
}
impl<'a> Tokenizer<'a> {
    fn new(s: &'a str) -> Tokenizer<'a> {
        Tokenizer { s: s.as_bytes(), i: 0 }
    }
    fn skip_ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }
    /// Consume `kw` (case-insensitive) if present; returns Some(()) if consumed.
    fn keyword(&mut self, kw: &str) -> Option<()> {
        self.skip_ws();
        let end = self.i + kw.len();
        if end <= self.s.len()
            && self.s[self.i..end].eq_ignore_ascii_case(kw.as_bytes())
            && (end == self.s.len() || !is_ident(self.s[end]))
        {
            self.i = end;
            Some(())
        } else {
            None
        }
    }
    fn symbol(&mut self, sym: &str) -> bool {
        self.skip_ws();
        let end = self.i + sym.len();
        if end <= self.s.len() && &self.s[self.i..end] == sym.as_bytes() {
            self.i = end;
            true
        } else {
            false
        }
    }
    fn ident(&mut self) -> Option<String> {
        self.skip_ws();
        let start = self.i;
        while self.i < self.s.len() && is_ident(self.s[self.i]) {
            self.i += 1;
        }
        if self.i > start {
            Some(String::from_utf8_lossy(&self.s[start..self.i]).into_owned())
        } else {
            None
        }
    }
    /// An identifier possibly qualified with `table.col`.
    fn col_ref(&mut self) -> Option<String> {
        self.skip_ws();
        let start = self.i;
        while self.i < self.s.len() && (is_ident(self.s[self.i]) || self.s[self.i] == b'.') {
            self.i += 1;
        }
        if self.i > start {
            Some(String::from_utf8_lossy(&self.s[start..self.i]).into_owned())
        } else {
            None
        }
    }
    fn string_lit(&mut self) -> Option<String> {
        self.skip_ws();
        if self.i < self.s.len() && self.s[self.i] == b'\'' {
            self.i += 1;
            let start = self.i;
            while self.i < self.s.len() && self.s[self.i] != b'\'' {
                self.i += 1;
            }
            let s = String::from_utf8_lossy(&self.s[start..self.i]).into_owned();
            self.i += 1; // closing quote
            Some(s)
        } else {
            None
        }
    }
    fn op(&mut self) -> Option<Op> {
        self.skip_ws();
        for (sym, op) in [(">=", Op::Ge), ("<=", Op::Le), ("=", Op::Eq), (">", Op::Gt), ("<", Op::Lt)] {
            if self.symbol(sym) {
                return Some(op);
            }
        }
        None
    }
    fn literal(&mut self) -> Option<FieldValue> {
        self.skip_ws();
        if self.i < self.s.len() && self.s[self.i] == b'\'' {
            self.i += 1;
            let start = self.i;
            while self.i < self.s.len() && self.s[self.i] != b'\'' {
                self.i += 1;
            }
            let s = String::from_utf8_lossy(&self.s[start..self.i]).into_owned();
            self.i += 1; // closing quote
            return Some(FieldValue::Str(s));
        }
        // number
        let start = self.i;
        while self.i < self.s.len() && (self.s[self.i].is_ascii_digit() || self.s[self.i] == b'-') {
            self.i += 1;
        }
        let tok = std::str::from_utf8(&self.s[start..self.i]).ok()?;
        tok.parse::<i64>().ok().map(FieldValue::I64)
    }
}

fn is_ident(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_select_with_where() {
        let s = parse_select("SELECT name, age FROM people WHERE age > 30 AND name = 'alice'").unwrap();
        assert_eq!(s.map, "people");
        assert_eq!(s.cols, vec![ColExpr::Col("name".into()), ColExpr::Col("age".into())]);
        assert_eq!(s.star, false);
        match s.filter.unwrap() {
            Predicate::And(v) => assert_eq!(v.len(), 2),
            _ => panic!("expected AND"),
        }
    }

    #[test]
    fn parse_star_no_where() {
        let s = parse_select("select * from m").unwrap();
        assert_eq!(s.star, true);
        assert!(s.cols.is_empty());
        assert!(s.filter.is_none());
    }

    #[test]
    fn parse_aggregations_and_modifiers() {
        let s = parse_select("SELECT DISTINCT count(*), sum(age), avg(salary) FROM people GROUP BY dept ORDER BY age DESC LIMIT 10").unwrap();
        assert_eq!(s.is_distinct, true);
        assert_eq!(s.cols, vec![
            ColExpr::Count("*".into()),
            ColExpr::Sum("age".into()),
            ColExpr::Avg("salary".into()),
        ]);
        assert_eq!(s.group_by, Some(vec!["dept".into()]));
        assert_eq!(s.order_by, Some(("age".into(), true)));
        assert_eq!(s.limit, Some(10));
    }

    #[test]
    fn parse_create_mapping_stmt() {
        let st = parse(
            "CREATE MAPPING recommender (user_id VARCHAR, starter VARCHAR) TYPE IMap \
             OPTIONS ('keyFormat'='varchar', 'valueFormat'='json-flat')",
        )
        .unwrap();
        match st {
            Statement::CreateMapping(m) => {
                assert_eq!(m.name, "recommender");
                assert_eq!(m.kind, MappingKind::Imap);
                assert_eq!(m.columns, vec![("user_id".into(), ColType::Varchar), ("starter".into(), ColType::Varchar)]);
                assert_eq!(m.value_format(), "json-flat");
            }
            _ => panic!("expected CreateMapping"),
        }
    }

    #[test]
    fn parse_insert_and_job() {
        match parse("INSERT INTO recommender VALUES ('user_1','Soup'), ('user_2','Salad')").unwrap() {
            Statement::Insert(i) => {
                assert_eq!(i.mapping, "recommender");
                assert_eq!(i.rows.len(), 2);
                assert_eq!(i.rows[0][0], FieldValue::Str("user_1".into()));
            }
            _ => panic!("expected Insert"),
        }
        match parse(
            "CREATE JOB enrich AS SINK INTO out SELECT * FROM pizzastream JOIN recommender \
             ON pizzastream.user_id = recommender.user_id WHERE starter = 'Soup'",
        )
        .unwrap()
        {
            Statement::CreateJob(j) => {
                assert_eq!(j.name, "enrich");
                assert_eq!(j.sink, "out");
                assert_eq!(j.select.map, "pizzastream");
                let join = j.select.join.unwrap();
                assert_eq!(join.right, "recommender");
                assert_eq!(join.left_col, "user_id");
                assert_eq!(join.right_col, "user_id");
            }
            _ => panic!("expected CreateJob"),
        }
    }

    #[test]
    fn join_core_filters_and_projects() {
        let sel = match parse(
            "CREATE JOB j AS SINK INTO out SELECT order_id, user_id, starter FROM orders \
             JOIN recommender ON orders.user_id = recommender.user_id WHERE starter = 'Soup'",
        )
        .unwrap()
        {
            Statement::CreateJob(j) => j.select,
            _ => panic!(),
        };
        let combined = vec![
            ("order_id".to_string(), FieldValue::I64(7)),
            ("user_id".to_string(), FieldValue::Str("user_1".into())),
            ("starter".to_string(), FieldValue::Str("Soup".into())),
        ];
        let row = project_row(&sel, &combined).unwrap();
        assert_eq!(
            row,
            vec![
                ("order_id".into(), FieldValue::I64(7)),
                ("user_id".into(), FieldValue::Str("user_1".into())),
                ("starter".into(), FieldValue::Str("Soup".into())),
            ]
        );
        let salad = vec![("starter".to_string(), FieldValue::Str("Salad".into()))];
        assert!(project_row(&sel, &salad).is_none());
    }

    #[test]
    fn execute_projects_and_filters() {
        use serialization::schema::{FieldDescriptor, Schema, INT32, STRING};
        let schemas = SchemaService::new();
        schemas.put(Schema::new(
            "person".into(),
            vec![FieldDescriptor::new("name".into(), STRING), FieldDescriptor::new("age".into(), INT32)],
        ));
        let payload: Vec<u8> = (0.."eac7fcf34f8f1c720000000d0000002300000005616c69636504".len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&"eac7fcf34f8f1c720000000d0000002300000005616c69636504"[i..i + 2], 16).unwrap())
            .collect();
        let mut v = vec![0u8; serialization::DATA_OFFSET];
        v.extend_from_slice(&payload);
        let entries = vec![(b"a".to_vec(), v)];

        let sel = parse_select("SELECT name, age FROM people WHERE age > 30").unwrap();
        let (cols, rows) = execute(&sel, &entries, &schemas);
        assert_eq!(cols, vec!["name", "age"]);
        assert_eq!(rows, vec![vec![Some("alice".into()), Some("35".into())]]);

        let sel2 = parse_select("SELECT name FROM people WHERE age > 40").unwrap();
        assert!(execute(&sel2, &entries, &schemas).1.is_empty());
    }

    #[test]
    fn execute_aggregations_group_order_limit() {
        use serialization::schema::{FieldDescriptor, Schema, INT32, STRING};
        let schemas = SchemaService::new();
        let schema = Schema::new(
            "employee".into(),
            vec![
                FieldDescriptor::new("dept".into(), STRING),
                FieldDescriptor::new("salary".into(), INT32),
            ],
        );
        schemas.put(schema);
        
        let helper = |dept: &str, salary: i32| {
            let mut payload = Vec::new();
            let schema = Schema::new("employee".into(), vec![
                FieldDescriptor::new("dept".into(), STRING),
                FieldDescriptor::new("salary".into(), INT32),
            ]);
            payload.extend_from_slice(&schema.id.to_be_bytes());
            payload.extend_from_slice(&4u32.to_be_bytes());
            payload.extend_from_slice(&salary.to_be_bytes());
            payload.push(5);
            payload.extend_from_slice(&(dept.len() as u32).to_be_bytes());
            payload.extend_from_slice(dept.as_bytes());
            
            let mut v = vec![0u8; serialization::DATA_OFFSET];
            v.extend_from_slice(&payload);
            (vec![0], v)
        };

        let entries = vec![
            helper("sales", 100),
            helper("sales", 200),
            helper("eng", 300),
            helper("eng", 400),
        ];

        // 1. Group by dept, avg salary
        let sel = parse_select("SELECT dept, avg(salary), count(*) FROM employees GROUP BY dept ORDER BY dept").unwrap();
        let (cols, mut rows) = execute(&sel, &entries, &schemas);
        assert_eq!(cols, vec!["dept", "avg(salary)", "count(*)"]);
        rows.sort();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![Some("eng".into()), Some("350".into()), Some("2".into())]);
        assert_eq!(rows[1], vec![Some("sales".into()), Some("150".into()), Some("2".into())]);
    }
}
