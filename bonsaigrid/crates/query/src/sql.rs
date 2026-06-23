//! A minimal SQL surface: `SELECT <cols|*> FROM <map> [WHERE <cond> [AND ...]]`
//! over Compact IMap values. Reuses the predicate evaluator and Compact extractor.
//! Columns are returned as text (VARCHAR) — enough to make the SQL API work; full
//! typing/optimization/joins are out of scope.

use crate::{eval, Op, Predicate};
use serialization::compact::{CompactExtractor, FieldExtractor, FieldValue};
use serialization::schema::SchemaService;

#[derive(Debug, PartialEq)]
pub enum Cols {
    Star,
    Named(Vec<String>),
}

#[derive(Debug, PartialEq)]
pub struct Select {
    pub cols: Cols,
    pub map: String,
    pub filter: Option<Predicate>,
}

/// Parse a `SELECT` statement. Returns None if it isn't a supported SELECT.
pub fn parse(sql: &str) -> Option<Select> {
    let mut t = Tokenizer::new(sql);
    t.keyword("select")?;
    let cols = if t.symbol("*") {
        Cols::Star
    } else {
        let mut names = vec![t.ident()?];
        while t.symbol(",") {
            names.push(t.ident()?);
        }
        Cols::Named(names)
    };
    t.keyword("from")?;
    let map = t.ident()?;
    let filter = if t.keyword("where").is_some() {
        Some(parse_conds(&mut t)?)
    } else {
        None
    };
    Some(Select { cols, map, filter })
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

/// Execute `select` over `(key, value)` IMap entries. Returns the column names and
/// the rows (each cell is text or NULL).
pub fn execute(
    select: &Select,
    entries: &[(Vec<u8>, Vec<u8>)],
    schemas: &SchemaService,
) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    let ex = CompactExtractor;
    let matchall = Predicate::And(vec![]); // empty AND matches everything
    let filter = select.filter.as_ref().unwrap_or(&matchall);

    let matched: Vec<&Vec<u8>> =
        entries.iter().filter(|(_, v)| eval(filter, v, schemas, &ex)).map(|(_, v)| v).collect();

    // Resolve column names: explicit, or the schema fields of the first match.
    let columns: Vec<String> = match &select.cols {
        Cols::Named(names) => names.clone(),
        Cols::Star => matched
            .first()
            .and_then(|v| schema_fields(v, schemas))
            .unwrap_or_default(),
    };

    let rows = matched
        .iter()
        .map(|v| columns.iter().map(|c| fmt(ex.extract(v, schemas, c))).collect())
        .collect();
    (columns, rows)
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
    match v {
        FieldValue::Null => None,
        FieldValue::Bool(b) => Some(b.to_string()),
        FieldValue::I32(i) => Some(i.to_string()),
        FieldValue::I64(i) => Some(i.to_string()),
        FieldValue::F64(f) => Some(f.to_string()),
        FieldValue::Str(s) => Some(s),
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
        let s = parse("SELECT name, age FROM people WHERE age > 30 AND name = 'alice'").unwrap();
        assert_eq!(s.map, "people");
        assert_eq!(s.cols, Cols::Named(vec!["name".into(), "age".into()]));
        match s.filter.unwrap() {
            Predicate::And(v) => assert_eq!(v.len(), 2),
            _ => panic!("expected AND"),
        }
    }

    #[test]
    fn parse_star_no_where() {
        let s = parse("select * from m").unwrap();
        assert_eq!(s.cols, Cols::Star);
        assert!(s.filter.is_none());
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

        let sel = parse("SELECT name, age FROM people WHERE age > 30").unwrap();
        let (cols, rows) = execute(&sel, &entries, &schemas);
        assert_eq!(cols, vec!["name", "age"]);
        assert_eq!(rows, vec![vec![Some("alice".into()), Some("35".into())]]);

        // filtered out
        let sel2 = parse("SELECT name FROM people WHERE age > 40").unwrap();
        assert!(execute(&sel2, &entries, &schemas).1.is_empty());
    }
}
