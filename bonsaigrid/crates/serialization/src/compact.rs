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

pub struct PortableExtractor;

pub struct AutoExtractor;

impl FieldExtractor for AutoExtractor {
    fn extract(&self, value: &[u8], schemas: &SchemaService, field: &str) -> FieldValue {
        if value.len() < 8 {
            return FieldValue::Null;
        }
        let type_id = i32::from_be_bytes(value[4..8].try_into().unwrap());
        if type_id == -1 {
            PortableExtractor.extract(value, schemas, field)
        } else {
            CompactExtractor.extract(value, schemas, field)
        }
    }
}

impl FieldExtractor for PortableExtractor {
    fn extract(&self, value: &[u8], _schemas: &SchemaService, field: &str) -> FieldValue {
        if value.len() < DATA_OFFSET + 20 {
            return FieldValue::Null;
        }
        let type_id = i32::from_be_bytes(value[4..8].try_into().unwrap());
        if type_id != -1 {
            return FieldValue::Null;
        }
        
        let payload = &value[DATA_OFFSET..];
        let field_count = match be_i32(payload, 16) {
            Some(v) if v >= 0 => v as usize,
            _ => return FieldValue::Null,
        };
        
        for i in 0..field_count {
            let offset_pos = 20 + i * 4;
            let pos = match be_i32(payload, offset_pos) {
                Some(v) if v >= 0 => v as usize,
                _ => continue,
            };
            if pos + 2 > payload.len() {
                continue;
            }
            let len = u16::from_be_bytes(payload[pos..pos+2].try_into().unwrap()) as usize;
            if pos + 2 + len > payload.len() {
                continue;
            }
            let name_bytes = &payload[pos + 2 .. pos + 2 + len];
            let name = String::from_utf8_lossy(name_bytes);
            if name == field {
                let type_id_pos = pos + 2 + len;
                if type_id_pos >= payload.len() {
                    return FieldValue::Null;
                }
                let field_type_id = payload[type_id_pos];
                let val_start = type_id_pos + 1;
                
                return match field_type_id {
                    1 => { // BYTE
                        if val_start < payload.len() {
                            FieldValue::I32(payload[val_start] as i32)
                        } else {
                            FieldValue::Null
                        }
                    }
                    2 => { // BOOLEAN
                        if val_start < payload.len() {
                            FieldValue::Bool(payload[val_start] != 0)
                        } else {
                            FieldValue::Null
                        }
                    }
                    3 => { // CHAR
                        if val_start + 2 <= payload.len() {
                            let c = u16::from_be_bytes(payload[val_start..val_start+2].try_into().unwrap());
                            FieldValue::I32(c as i32)
                        } else {
                            FieldValue::Null
                        }
                    }
                    4 => { // SHORT
                        if val_start + 2 <= payload.len() {
                            let s = i16::from_be_bytes(payload[val_start..val_start+2].try_into().unwrap());
                            FieldValue::I32(s as i32)
                        } else {
                            FieldValue::Null
                        }
                    }
                    5 => { // INT
                        if val_start + 4 <= payload.len() {
                            FieldValue::I32(i32::from_be_bytes(payload[val_start..val_start+4].try_into().unwrap()))
                        } else {
                            FieldValue::Null
                        }
                    }
                    6 => { // LONG
                        if val_start + 8 <= payload.len() {
                            FieldValue::I64(i64::from_be_bytes(payload[val_start..val_start+8].try_into().unwrap()))
                        } else {
                            FieldValue::Null
                        }
                    }
                    7 => { // FLOAT
                        if val_start + 4 <= payload.len() {
                            let f = f32::from_be_bytes(payload[val_start..val_start+4].try_into().unwrap());
                            FieldValue::F64(f as f64)
                        } else {
                            FieldValue::Null
                        }
                    }
                    8 => { // DOUBLE
                        if val_start + 8 <= payload.len() {
                            FieldValue::F64(f64::from_be_bytes(payload[val_start..val_start+8].try_into().unwrap()))
                        } else {
                            FieldValue::Null
                        }
                    }
                    9 => { // UTF (String)
                        if val_start + 4 <= payload.len() {
                            let str_len = i32::from_be_bytes(payload[val_start..val_start+4].try_into().unwrap());
                            if str_len < 0 {
                                FieldValue::Null
                            } else {
                                let str_end = val_start + 4 + str_len as usize;
                                if str_end <= payload.len() {
                                    FieldValue::Str(String::from_utf8_lossy(&payload[val_start + 4 .. str_end]).into_owned())
                                } else {
                                    FieldValue::Null
                                }
                            }
                        } else {
                            FieldValue::Null
                        }
                    }
                    _ => FieldValue::Null,
                };
            }
        }
        FieldValue::Null
    }
}

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

    #[test]
    fn extracts_from_portable_record() {
        let v = hex("00000000ffffffff0000000100000002000000030000003a00000002000000200000002a0000003a0003616765050000002300046e616d650900000005616c696365");
        let schemas = SchemaService::new();
        let ex = PortableExtractor;
        assert_eq!(ex.extract(&v, &schemas, "age"), FieldValue::I32(35));
        assert_eq!(ex.extract(&v, &schemas, "name"), FieldValue::Str("alice".into()));
        assert_eq!(ex.extract(&v, &schemas, "missing"), FieldValue::Null);

        let auto = AutoExtractor;
        assert_eq!(auto.extract(&v, &schemas, "age"), FieldValue::I32(35));
        assert_eq!(auto.extract(&v, &schemas, "name"), FieldValue::Str("alice".into()));
    }
}
