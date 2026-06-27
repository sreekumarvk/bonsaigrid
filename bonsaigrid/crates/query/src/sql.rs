//! A minimal SQL surface: `SELECT <cols|*> FROM <map> [WHERE <cond> [AND ...]]`
//! over Compact IMap values. Reuses the predicate evaluator and Compact extractor.
//! Columns are returned as text (VARCHAR) — enough to make the SQL API work; full
//! typing/optimization/joins are out of scope.

use crate::{eval, Op, Predicate};
use serialization::compact::{AutoExtractor, CompactExtractor, FieldExtractor, FieldValue};
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
pub enum Cols {
    Star,
    Named(Vec<String>),
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
    pub cols: Cols,
    pub map: String,
    pub join: Option<Join>,
    pub filter: Option<Predicate>,
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

fn parse_select_body(t: &mut Tokenizer) -> Option<Select> {
    let cols = if t.symbol("*") {
        Cols::Star
    } else {
        let mut names = vec![t.col_ref()?];
        while t.symbol(",") {
            names.push(t.col_ref()?);
        }
        Cols::Named(names)
    };
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
    Some(Select { cols, map, join, filter })
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

    let columns: Vec<String> = match &select.cols {
        Cols::Named(names) => names.iter().map(|n| bare_col(n)).collect(),
        Cols::Star if !star_cols.is_empty() => star_cols.to_vec(),
        Cols::Star => matched.first().and_then(|(_, v)| schema_fields(v, schemas)).unwrap_or_default(),
    };

    let rows = matched
        .iter()
        .map(|(k, v)| {
            columns
                .iter()
                .map(|c| {
                    if Some(c.as_str()) == key_col {
                        fmt(crate::json::decode_key_data(k)) // key column projected from the key
                    } else {
                        fmt(ex.extract(v, schemas, c))
                    }
                })
                .collect()
        })
        .collect();
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
    let cols: Vec<String> = match &select.cols {
        Cols::Named(n) => n.iter().map(|c| bare_col(c)).collect(),
        Cols::Star => fields.iter().map(|(k, _)| k.clone()).collect(),
    };
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
        assert_eq!(s.cols, Cols::Named(vec!["name".into(), "age".into()]));
        match s.filter.unwrap() {
            Predicate::And(v) => assert_eq!(v.len(), 2),
            _ => panic!("expected AND"),
        }
    }

    #[test]
    fn parse_star_no_where() {
        let s = parse_select("select * from m").unwrap();
        assert_eq!(s.cols, Cols::Star);
        assert!(s.filter.is_none());
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
        // Simulate a streamed order joined with a recommendation row.
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
        // WHERE filters non-Soup out.
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
        // Real Person{name:"alice", age:35} Compact value.
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

        // filtered out
        let sel2 = parse_select("SELECT name FROM people WHERE age > 40").unwrap();
        assert!(execute(&sel2, &entries, &schemas).1.is_empty());
    }
}
