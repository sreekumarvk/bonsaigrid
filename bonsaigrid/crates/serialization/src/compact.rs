//! Compact record reader: extract a scalar field from a Compact `Data`.
//!
//! Layout (all big-endian), payload = `Data[DATA_OFFSET..]`:
//!   [schemaId i64][dataLength i32 (only if the schema has var-size fields)][data][var offset table]
//! - fixed field  → `data_start + fd.offset`
//! - boolean      → bit `fd.bit` of byte `data_start + fd.offset`
//! - var (string) → offset table entry `fd.index` gives `off`; data at
//!   `data_start + off` is `[i32 len][utf8 bytes]`.

use crate::schema::{SchemaService, BOOLEAN, FLOAT64, INT32, INT64, STRING};
use crate::DATA_OFFSET;
use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq)]
pub enum FieldValue {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    Str(String),
}

impl FieldValue {
    /// Type-aware ordering (numeric kinds compare across I32/I64); None if the
    /// kinds aren't comparable.
    pub fn compare(&self, other: &FieldValue) -> Option<Ordering> {
        use FieldValue::*;
        match (self, other) {
            (I32(a), I32(b)) => a.partial_cmp(b),
            (I64(a), I64(b)) => a.partial_cmp(b),
            (I32(a), I64(b)) => (*a as i64).partial_cmp(b),
            (I64(a), I32(b)) => a.partial_cmp(&(*b as i64)),
            (F64(a), F64(b)) => a.partial_cmp(b),
            (Str(a), Str(b)) => a.partial_cmp(b),
            (Bool(a), Bool(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
    pub fn equals(&self, other: &FieldValue) -> bool {
        self.compare(other) == Some(Ordering::Equal)
    }
}

pub trait FieldExtractor {
    fn extract(&self, value: &[u8], schemas: &SchemaService, field: &str) -> FieldValue;
}

pub struct CompactExtractor;

fn be_i32(b: &[u8], p: usize) -> Option<i32> {
    b.get(p..p + 4).map(|s| i32::from_be_bytes(s.try_into().unwrap()))
}
fn be_i64(b: &[u8], p: usize) -> Option<i64> {
    b.get(p..p + 8).map(|s| i64::from_be_bytes(s.try_into().unwrap()))
}

/// Read the var-size offset for `index` from the offset table; -1 == null.
fn var_offset(payload: &[u8], table: usize, index: i32, data_len: usize) -> i32 {
    let i = index as usize;
    if data_len < 255 {
        match payload.get(table + i) {
            Some(&b) if b as i8 == -1 => -1,
            Some(&b) => b as i32,
            None => -1,
        }
    } else if data_len < 65535 {
        match payload.get(table + i * 2..table + i * 2 + 2) {
            Some(s) => {
                let v = i16::from_be_bytes(s.try_into().unwrap());
                if v == -1 { -1 } else { (v as u16) as i32 }
            }
            None => -1,
        }
    } else {
        be_i32(payload, table + i * 4).unwrap_or(-1)
    }
}

impl FieldExtractor for CompactExtractor {
    fn extract(&self, value: &[u8], schemas: &SchemaService, field: &str) -> FieldValue {
        if value.len() < DATA_OFFSET + 8 {
            return FieldValue::Null;
        }
        let payload = &value[DATA_OFFSET..];
        let schema_id = match be_i64(payload, 0) {
            Some(v) => v,
            None => return FieldValue::Null,
        };
        let Some(schema) = schemas.get(schema_id) else { return FieldValue::Null };
        let Some(fd) = schema.field(field) else { return FieldValue::Null };

        let (data_start, var_table, data_len): (usize, usize, usize) = if schema.var_field_count > 0 {
            let dl = match be_i32(payload, 8) {
                Some(v) => v as usize,
                None => return FieldValue::Null,
            };
            (12, 12 + dl, dl)
        } else {
            (8, 0, 0)
        };

        let off = data_start + fd.offset.max(0) as usize;
        match fd.kind {
            INT32 => be_i32(payload, off).map_or(FieldValue::Null, FieldValue::I32),
            INT64 => be_i64(payload, off).map_or(FieldValue::Null, FieldValue::I64),
            FLOAT64 => be_i64(payload, off).map_or(FieldValue::Null, |b| FieldValue::F64(f64::from_bits(b as u64))),
            BOOLEAN => payload
                .get(off)
                .map_or(FieldValue::Null, |&byte| FieldValue::Bool((byte >> fd.bit.max(0)) & 1 == 1)),
            STRING => {
                let o = var_offset(payload, var_table, fd.index, data_len);
                if o < 0 {
                    return FieldValue::Null;
                }
                let pos = data_start + o as usize;
                let len = match be_i32(payload, pos) {
                    Some(v) if v >= 0 => v as usize,
                    _ => return FieldValue::Null,
                };
                match payload.get(pos + 4..pos + 4 + len) {
                    Some(s) => FieldValue::Str(String::from_utf8_lossy(s).into_owned()),
                    None => FieldValue::Null,
                }
            }
            _ => FieldValue::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldDescriptor, Schema};

    /// The real Person{name:"alice", age:35} Compact payload captured from a
    /// stock client (after the 8-byte Data header).
    fn person_value() -> Vec<u8> {
        let payload =
            hex("eac7fcf34f8f1c720000000d0000002300000005616c69636504");
        let mut v = vec![0u8; DATA_OFFSET]; // partitionHash + type header (ignored)
        v.extend_from_slice(&payload);
        v
    }
    fn hex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    #[test]
    fn extracts_int_and_string_from_real_record() {
        let schemas = SchemaService::new();
        schemas.put(Schema::new(
            "person".into(),
            vec![
                FieldDescriptor::new("name".into(), STRING),
                FieldDescriptor::new("age".into(), INT32),
            ],
        ));
        let v = person_value();
        let ex = CompactExtractor;
        assert_eq!(ex.extract(&v, &schemas, "age"), FieldValue::I32(35));
        assert_eq!(ex.extract(&v, &schemas, "name"), FieldValue::Str("alice".into()));
        assert_eq!(ex.extract(&v, &schemas, "missing"), FieldValue::Null);
    }

    #[test]
    fn unknown_schema_is_null() {
        let schemas = SchemaService::new(); // empty
        assert_eq!(CompactExtractor.extract(&person_value(), &schemas, "age"), FieldValue::Null);
    }

    #[test]
    fn field_value_compare() {
        assert!(FieldValue::I32(35).compare(&FieldValue::I32(30)) == Some(Ordering::Greater));
        assert!(FieldValue::I32(35).compare(&FieldValue::I64(35)) == Some(Ordering::Equal));
        assert!(FieldValue::Str("a".into()).equals(&FieldValue::Str("a".into())));
        assert_eq!(FieldValue::I32(1).compare(&FieldValue::Str("x".into())), None);
    }
}
