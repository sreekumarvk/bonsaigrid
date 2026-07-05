//! JDBC-style database connector (PostgreSQL): load a SQL query result into an IMap
//! — the Rust analog of Hazelcast's JDBC MapLoader / Jet JDBC source. Blocking, run
//! on its own connector thread (off the reactor hot path), like the Kafka connector.
//!
//! Convention (matches the SQL / json-flat surface): the **first** selected column is
//! the IMap key; the remaining columns form a flat JSON object value, so the loaded
//! map is immediately queryable via the SQL engine and joinable in streaming jobs.

use postgres::types::Type;
use postgres::{Client, NoTls, Row};
use store::Store;

pub struct JdbcSource {
    client: Client,
}

impl JdbcSource {
    /// Connect to a PostgreSQL server, e.g.
    /// `"host=127.0.0.1 port=5432 user=postgres password=pw dbname=postgres"`.
    pub fn connect(conn_str: &str) -> Result<JdbcSource, String> {
        let client = Client::connect(conn_str, NoTls).map_err(|e| e.to_string())?;
        Ok(JdbcSource { client })
    }

    /// Execute a statement (DDL/DML: CREATE/INSERT/UPDATE/…); returns rows affected.
    pub fn execute(&mut self, sql: &str) -> Result<u64, String> {
        self.client.execute(sql, &[]).map_err(|e| e.to_string())
    }

    /// Run a SELECT and return `(key, json-value)` per row (first column = key).
    pub fn query_rows(&mut self, sql: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
        let rows = self.client.query(sql, &[]).map_err(|e| e.to_string())?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            if row.columns().is_empty() {
                continue;
            }
            let key = scalar_string(row, 0);
            let mut json = String::from("{");
            for i in 1..row.columns().len() {
                if i > 1 {
                    json.push(',');
                }
                json.push('"');
                json.push_str(row.columns()[i].name());
                json.push_str("\":");
                json.push_str(&scalar_json(row, i));
            }
            json.push('}');
            out.push((key, json.into_bytes()));
        }
        Ok(out)
    }

    /// Load the query result into `store` under IMap `map`. Returns the row count.
    pub fn load_into_map(&mut self, store: &Store, map: &str, sql: &str) -> Result<usize, String> {
        let rows = self.query_rows(sql)?;
        let n = rows.len();
        for (k, v) in rows {
            store.put(map, k.into_bytes(), v);
        }
        Ok(n)
    }
}

/// A column value as a plain string (for the key): numbers as digits, text as-is.
fn scalar_string(row: &Row, i: usize) -> String {
    match *row.columns()[i].type_() {
        Type::INT2 => opt(row.try_get::<_, Option<i16>>(i)),
        Type::INT4 => opt(row.try_get::<_, Option<i32>>(i)),
        Type::INT8 => opt(row.try_get::<_, Option<i64>>(i)),
        Type::FLOAT4 => opt(row.try_get::<_, Option<f32>>(i)),
        Type::FLOAT8 => opt(row.try_get::<_, Option<f64>>(i)),
        Type::BOOL => opt(row.try_get::<_, Option<bool>>(i)),
        _ => row
            .try_get::<_, Option<String>>(i)
            .ok()
            .flatten()
            .unwrap_or_default(),
    }
}

/// A column value as a JSON scalar (numbers/bools bare, everything else quoted).
fn scalar_json(row: &Row, i: usize) -> String {
    match *row.columns()[i].type_() {
        Type::INT2 => opt_or_null(row.try_get::<_, Option<i16>>(i)),
        Type::INT4 => opt_or_null(row.try_get::<_, Option<i32>>(i)),
        Type::INT8 => opt_or_null(row.try_get::<_, Option<i64>>(i)),
        Type::FLOAT4 => opt_or_null(row.try_get::<_, Option<f32>>(i)),
        Type::FLOAT8 => opt_or_null(row.try_get::<_, Option<f64>>(i)),
        Type::BOOL => opt_or_null(row.try_get::<_, Option<bool>>(i)),
        _ => match row.try_get::<_, Option<String>>(i).ok().flatten() {
            Some(s) => json_quote(&s),
            None => "null".into(),
        },
    }
}

fn opt<T: ToString, E>(r: Result<Option<T>, E>) -> String {
    r.ok().flatten().map(|v| v.to_string()).unwrap_or_default()
}
fn opt_or_null<T: ToString, E>(r: Result<Option<T>, E>) -> String {
    r.ok()
        .flatten()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "null".into())
}

fn json_quote(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('"');
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\t' => o.push_str("\\t"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

#[cfg(test)]
mod tests {
    use super::json_quote;

    #[test]
    fn json_quote_escapes() {
        assert_eq!(json_quote("hi"), "\"hi\"");
        assert_eq!(json_quote("a\"b\\c"), "\"a\\\"b\\\\c\"");
        assert_eq!(json_quote("line\nbreak"), "\"line\\nbreak\"");
    }
}
