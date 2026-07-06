//! Change Data Capture (CDC) connector for PostgreSQL — the log-based analog of the
//! JDBC batch loader. It reads committed INSERT/UPDATE/DELETE from the write-ahead
//! log via a logical-replication slot (the built-in `test_decoding` output plugin),
//! polled through `pg_logical_slot_get_changes` — so no reads hit the source tables
//! and every mutation is captured in commit order. Blocking, on its own connector
//! thread (off the reactor hot path). Requires the server to run with
//! `wal_level = logical`.

use postgres::{Client, NoTls};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    pub op: ChangeOp,
    pub table: String,                  // "schema.table"
    pub columns: Vec<(String, String)>, // (column, value-as-text)
}

impl Change {
    /// The value of a captured column, if present.
    pub fn get(&self, col: &str) -> Option<&str> {
        self.columns
            .iter()
            .find(|(c, _)| c == col)
            .map(|(_, v)| v.as_str())
    }
}

pub struct CdcSource {
    client: Client,
    slot: String,
}

impl CdcSource {
    /// Connect and (re)create the logical slot `slot` with the `test_decoding` plugin.
    /// Dropping first makes the start clean and idempotent.
    pub fn connect(conn_str: &str, slot: &str) -> Result<CdcSource, String> {
        let mut client = Client::connect(conn_str, NoTls).map_err(|e| e.to_string())?;
        let _ = client.execute("SELECT pg_drop_replication_slot($1)", &[&slot]); // ignore if absent
        client
            .query(
                "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
                &[&slot],
            )
            .map_err(|e| e.to_string())?;
        Ok(CdcSource {
            client,
            slot: slot.to_string(),
        })
    }

    /// Consume all changes committed since the last poll (advances the slot).
    pub fn poll(&mut self) -> Result<Vec<Change>, String> {
        let rows = self
            .client
            .query(
                "SELECT data FROM pg_logical_slot_get_changes($1, NULL, NULL)",
                &[&self.slot],
            )
            .map_err(|e| e.to_string())?;
        let mut changes = Vec::new();
        for row in &rows {
            let line: String = row.get(0);
            if let Some(c) = parse_change(&line) {
                changes.push(c);
            }
        }
        Ok(changes)
    }

    /// Release the slot (so the WAL it pins can be reclaimed).
    pub fn drop_slot(&mut self) -> Result<(), String> {
        self.client
            .execute("SELECT pg_drop_replication_slot($1)", &[&self.slot])
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

/// Parse one `test_decoding` line, e.g.
/// `table public.users: INSERT: id[integer]:1 name[text]:'alice'`.
/// `BEGIN`/`COMMIT` lines (and anything unrecognized) yield `None`.
fn parse_change(line: &str) -> Option<Change> {
    let rest = line.strip_prefix("table ")?;
    let (table, rest) = rest.split_once(": ")?; // "public.users", "INSERT: id[..]:1 ..."
    let (op_str, cols) = rest.split_once(": ").unwrap_or((rest, ""));
    let op = match op_str {
        "INSERT" => ChangeOp::Insert,
        "UPDATE" => ChangeOp::Update,
        "DELETE" => ChangeOp::Delete,
        _ => return None,
    };
    Some(Change {
        op,
        table: table.to_string(),
        columns: parse_columns(cols),
    })
}

/// Parse `name[type]:value name[type]:'quoted value' …` into (name, value) pairs.
/// Quoted (text) values may contain spaces; a doubled `''` is a literal quote.
fn parse_columns(s: &str) -> Vec<(String, String)> {
    let b = s.as_bytes();
    let mut cols = Vec::new();
    let mut i = 0;
    while i < b.len() {
        while i < b.len() && b[i] == b' ' {
            i += 1;
        }
        if i >= b.len() {
            break;
        }
        let name_start = i;
        while i < b.len() && b[i] != b'[' {
            i += 1;
        }
        let name = s[name_start..i].to_string();
        while i < b.len() && b[i] != b']' {
            i += 1; // skip the [type]
        }
        if i < b.len() {
            i += 1; // ']'
        }
        if i < b.len() && b[i] == b':' {
            i += 1;
        }
        let value = if i < b.len() && b[i] == b'\'' {
            i += 1;
            let mut v = Vec::new();
            while i < b.len() {
                if b[i] == b'\'' {
                    if i + 1 < b.len() && b[i + 1] == b'\'' {
                        v.push(b'\'');
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    v.push(b[i]);
                    i += 1;
                }
            }
            String::from_utf8_lossy(&v).into_owned()
        } else {
            let vs = i;
            while i < b.len() && b[i] != b' ' {
                i += 1;
            }
            s[vs..i].to_string()
        };
        cols.push((name, value));
    }
    cols
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_test_decoding_lines() {
        assert_eq!(parse_change("BEGIN 745"), None);
        assert_eq!(parse_change("COMMIT 745"), None);

        let ins = parse_change(
            "table public.users: INSERT: id[integer]:1 name[text]:'alice' active[boolean]:true",
        )
        .unwrap();
        assert_eq!(ins.op, ChangeOp::Insert);
        assert_eq!(ins.table, "public.users");
        assert_eq!(ins.get("id"), Some("1"));
        assert_eq!(ins.get("name"), Some("alice"));
        assert_eq!(ins.get("active"), Some("true"));

        // quoted value with spaces + a doubled quote
        let upd =
            parse_change("table public.users: UPDATE: id[integer]:1 name[text]:'a b''c'").unwrap();
        assert_eq!(upd.op, ChangeOp::Update);
        assert_eq!(upd.get("name"), Some("a b'c"));

        let del = parse_change("table public.users: DELETE: id[integer]:7").unwrap();
        assert_eq!(del.op, ChangeOp::Delete);
        assert_eq!(del.get("id"), Some("7"));
    }
}
