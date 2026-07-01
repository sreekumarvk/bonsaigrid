//! Compact schema model (with field layout) + schema service.
//!
//! A `Schema` (type name + fields) is sent by the client via `ClientSendSchema`.
//! Its `schemaId` is the RABIN fingerprint of (typeName, fieldCount,
//! [(fieldName, kindId)...]) over fields **sorted by name** — computed
//! identically here so it matches the id embedded in Compact records.
//!
//! `Schema::new` also computes each field's record position (matching Hazelcast):
//! fields sorted by name; fixed-size fields then ordered by size descending and
//! packed; booleans bit-packed after them; variable-size fields numbered by
//! index in name order.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

// FieldKind ids (== ordinal). Named: the kinds we lay out / read.
pub const BOOLEAN: i32 = 1;
pub const INT8: i32 = 3;
pub const CHAR: i32 = 5;
pub const INT16: i32 = 7;
pub const INT32: i32 = 9;
pub const INT64: i32 = 11;
pub const FLOAT32: i32 = 13;
pub const FLOAT64: i32 = 15;
pub const STRING: i32 = 17;

/// Byte size of a fixed-size (non-boolean) kind, else None (variable/boolean).
pub fn fixed_byte_size(kind: i32) -> Option<usize> {
    match kind {
        INT8 => Some(1),
        INT16 | CHAR => Some(2),
        INT32 | FLOAT32 => Some(4),
        INT64 | FLOAT64 => Some(8),
        _ => None,
    }
}

#[derive(Clone, Debug)]
pub struct FieldDescriptor {
    pub name: String,
    pub kind: i32,
    pub offset: i32, // byte offset for fixed/boolean fields (-1 otherwise)
    pub index: i32,  // var-size field index (-1 otherwise)
    pub bit: i8,     // bit position for boolean fields (-1 otherwise)
}

impl FieldDescriptor {
    pub fn new(name: String, kind: i32) -> FieldDescriptor {
        FieldDescriptor {
            name,
            kind,
            offset: -1,
            index: -1,
            bit: -1,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub type_name: String,
    pub fields: Vec<FieldDescriptor>,
    pub id: i64,
    pub fixed_size_length: usize,
    pub var_field_count: usize,
}

impl Schema {
    pub fn new(type_name: String, mut fields: Vec<FieldDescriptor>) -> Schema {
        fields.sort_by(|a, b| a.name.cmp(&b.name));
        let id = fingerprint(&type_name, &fields);

        // Fixed (non-boolean) fields: descending by size (stable → name order on ties).
        let mut fixed: Vec<usize> = (0..fields.len())
            .filter(|&i| fixed_byte_size(fields[i].kind).is_some())
            .collect();
        fixed.sort_by(|&a, &b| {
            fixed_byte_size(fields[b].kind)
                .unwrap()
                .cmp(&fixed_byte_size(fields[a].kind).unwrap())
        });
        let mut offset: i32 = 0;
        for &i in &fixed {
            fields[i].offset = offset;
            offset += fixed_byte_size(fields[i].kind).unwrap() as i32;
        }

        // Booleans bit-packed after the fixed fields.
        let mut bit: i8 = 0;
        for f in fields.iter_mut().filter(|f| f.kind == BOOLEAN) {
            f.offset = offset;
            f.bit = bit;
            bit += 1;
            if bit == 8 {
                bit = 0;
                offset += 1;
            }
        }
        if bit != 0 {
            offset += 1;
        }
        let fixed_size_length = offset as usize;

        // Variable-size fields numbered by index in name order.
        let mut index: i32 = 0;
        for f in fields.iter_mut() {
            if f.kind != BOOLEAN && fixed_byte_size(f.kind).is_none() {
                f.index = index;
                index += 1;
            }
        }
        let var_field_count = index as usize;

        Schema {
            type_name,
            fields,
            id,
            fixed_size_length,
            var_field_count,
        }
    }

    pub fn field(&self, name: &str) -> Option<&FieldDescriptor> {
        self.fields.iter().find(|f| f.name == name)
    }
}

// ---- RABIN fingerprint (matches Hazelcast RabinFingerprint) ----
const INIT: u64 = 0xc15d213aa4d7a795;

fn fp_table() -> &'static [u64; 256] {
    static TABLE: OnceLock<[u64; 256]> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut t = [0u64; 256];
        for (i, slot) in t.iter_mut().enumerate() {
            let mut fp = i as u64;
            for _ in 0..8 {
                fp = (fp >> 1) ^ (INIT & (fp & 1).wrapping_neg());
            }
            *slot = fp;
        }
        t
    })
}

fn fp_byte(fp: u64, b: u8) -> u64 {
    (fp >> 8) ^ fp_table()[((fp ^ b as u64) & 0xff) as usize]
}
fn fp_int(mut fp: u64, v: i32) -> u64 {
    let u = v as u32;
    for shift in [0, 8, 16, 24] {
        fp = fp_byte(fp, ((u >> shift) & 0xff) as u8);
    }
    fp
}
fn fp_str(mut fp: u64, s: &str) -> u64 {
    let bytes = s.as_bytes();
    fp = fp_int(fp, bytes.len() as i32);
    for &b in bytes {
        fp = fp_byte(fp, b);
    }
    fp
}

/// schemaId = fingerprint(typeName, fieldCount, [(fieldName, kindId)...]),
/// fields in the given order (callers pass name-sorted fields).
pub fn fingerprint(type_name: &str, fields: &[FieldDescriptor]) -> i64 {
    let mut fp = fp_str(INIT, type_name);
    fp = fp_int(fp, fields.len() as i32);
    for f in fields {
        fp = fp_str(fp, &f.name);
        fp = fp_int(fp, f.kind);
    }
    fp as i64
}

/// Stores Compact schemas keyed by id (shared across reactor threads).
#[derive(Default, Clone)]
pub struct SchemaService {
    schemas: Arc<Mutex<HashMap<i64, Schema>>>,
}

impl SchemaService {
    pub fn new() -> SchemaService {
        SchemaService::default()
    }
    pub fn put(&self, schema: Schema) -> i64 {
        let id = schema.id;
        self.schemas.lock().unwrap().insert(id, schema);
        id
    }
    pub fn get(&self, id: i64) -> Option<Schema> {
        self.schemas.lock().unwrap().get(&id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fd(name: &str, kind: i32) -> FieldDescriptor {
        FieldDescriptor::new(name.into(), kind)
    }

    #[test]
    fn fingerprint_matches_real_client_schema_id() {
        // Captured from a stock Python client: Person{name:String, age:int32}.
        // The Compact record's embedded schemaId (big-endian) was eac7fcf34f8f1c72.
        let s = Schema::new("person".into(), vec![fd("name", STRING), fd("age", INT32)]);
        let expected = i64::from_be_bytes([0xea, 0xc7, 0xfc, 0xf3, 0x4f, 0x8f, 0x1c, 0x72]);
        assert_eq!(s.id, expected);
    }

    #[test]
    fn person_layout_matches_capture() {
        // age(INT32) fixed @0; name(STRING) var index 0; fixed length 4; 1 var field.
        let s = Schema::new("person".into(), vec![fd("name", STRING), fd("age", INT32)]);
        assert_eq!(s.field("age").unwrap().offset, 0);
        assert_eq!(s.field("name").unwrap().index, 0);
        assert_eq!(s.fixed_size_length, 4);
        assert_eq!(s.var_field_count, 1);
    }

    #[test]
    fn fixed_fields_packed_descending_by_size() {
        // a:INT32(4), b:INT64(8), c:INT8(1) -> by size desc: b@0, a@8, c@12.
        let s = Schema::new(
            "t".into(),
            vec![fd("a", INT32), fd("b", INT64), fd("c", INT8)],
        );
        assert_eq!(s.field("b").unwrap().offset, 0);
        assert_eq!(s.field("a").unwrap().offset, 8);
        assert_eq!(s.field("c").unwrap().offset, 12);
        assert_eq!(s.fixed_size_length, 13);
    }

    #[test]
    fn fingerprint_is_order_independent_of_input() {
        // Schema::new sorts by name, so input order doesn't change the id.
        let a = Schema::new("p".into(), vec![fd("name", STRING), fd("age", INT32)]);
        let b = Schema::new("p".into(), vec![fd("age", INT32), fd("name", STRING)]);
        assert_eq!(a.id, b.id);
    }

    #[test]
    fn schema_service_roundtrip() {
        let s = SchemaService::new();
        let schema = Schema::new("person".into(), vec![fd("age", INT32)]);
        let id = s.put(schema.clone());
        assert_eq!(id, schema.id);
        assert_eq!(s.get(id).unwrap().type_name, "person");
        assert!(s.get(12345).is_none());
    }
}
