//! Compact schema model + schema service.
//!
//! A Compact `Schema` (type name + ordered field descriptors) is sent by the
//! client via `ClientSendSchema`; its `schemaId` is the RABIN fingerprint of
//! (typeName, fieldCount, [(fieldName, kindId)...]) — computed identically here
//! so the id matches the one embedded in Compact records.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

// FieldKind ids (FieldKind.getId() == ordinal). Only the kinds the MVP reads
// are named; others pass through as raw ids.
pub const BOOLEAN: i32 = 1;
pub const INT32: i32 = 9;
pub const INT64: i32 = 11;
pub const FLOAT64: i32 = 15;
pub const STRING: i32 = 17;

#[derive(Clone, Debug)]
pub struct FieldDescriptor {
    pub name: String,
    pub kind: i32,
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub type_name: String,
    pub fields: Vec<FieldDescriptor>, // in wire order (== fingerprint order)
    pub id: i64,
}

impl Schema {
    pub fn new(type_name: String, fields: Vec<FieldDescriptor>) -> Schema {
        let id = fingerprint(&type_name, &fields);
        Schema { type_name, fields, id }
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

/// schemaId = fingerprint(typeName, fieldCount, [(fieldName, kindId)...]).
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
#[derive(Default)]
pub struct SchemaService {
    schemas: Mutex<HashMap<i64, Schema>>,
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

    #[test]
    fn fingerprint_is_deterministic_and_order_sensitive() {
        let f1 = vec![
            FieldDescriptor { name: "name".into(), kind: STRING },
            FieldDescriptor { name: "age".into(), kind: INT32 },
        ];
        let f2 = vec![
            FieldDescriptor { name: "age".into(), kind: INT32 },
            FieldDescriptor { name: "name".into(), kind: STRING },
        ];
        let a = fingerprint("person", &f1);
        assert_eq!(a, fingerprint("person", &f1), "deterministic");
        assert_ne!(a, fingerprint("person", &f2), "field order matters");
        assert_ne!(a, fingerprint("other", &f1), "type name matters");
    }

    #[test]
    fn schema_service_roundtrip() {
        let s = SchemaService::new();
        let schema = Schema::new(
            "person".into(),
            vec![FieldDescriptor { name: "age".into(), kind: INT32 }],
        );
        let id = s.put(schema.clone());
        assert_eq!(id, schema.id);
        assert_eq!(s.get(id).unwrap().type_name, "person");
        assert!(s.get(12345).is_none());
    }
}
