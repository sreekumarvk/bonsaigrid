//! `json-flat` value support: extract fields from a HazelcastJsonValue `Data`
//! blob, and build the key/value `Data` blobs an INSERT produces.
//!
//! HazelcastJsonValue Data: `[partitionHash=0 i32][type=-130 i32][len i32 BE][utf8 json]`.
//! String Data: type -11; Integer Data: type -7.

use serialization::compact::{FieldExtractor, FieldValue};
use serialization::schema::SchemaService;
use serialization::DATA_OFFSET;

const T_STRING: i32 = -11;
const T_INTEGER: i32 = -7;
const T_JSON: i32 = -130;

/// Extracts a flat field from a JSON-object IMap value.
pub struct JsonExtractor;

impl FieldExtractor for JsonExtractor {
    fn extract(&self, value: &[u8], _schemas: &SchemaService, field: &str) -> FieldValue {
        let Some(obj) = parse_json_object(value) else {
            return FieldValue::Null;
        };
        match obj.get(field) {
            None | Some(serde_json::Value::Null) => FieldValue::Null,
            Some(serde_json::Value::String(s)) => FieldValue::Str(s.clone()),
            Some(serde_json::Value::Bool(b)) => FieldValue::Bool(*b),
            Some(serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    FieldValue::I64(i)
                } else {
                    FieldValue::F64(n.as_f64().unwrap_or(0.0))
                }
            }
            Some(other) => FieldValue::Str(other.to_string()),
        }
    }
}

/// Parse the JSON object inside a HazelcastJsonValue `Data` blob.
pub fn parse_json_object(value: &[u8]) -> Option<serde_json::Map<String, serde_json::Value>> {
    if value.len() < DATA_OFFSET + 4 {
        return None;
    }
    let len = i32::from_be_bytes(value[DATA_OFFSET..DATA_OFFSET + 4].try_into().ok()?) as usize;
    let start = DATA_OFFSET + 4;
    let json = value.get(start..start + len)?;
    serde_json::from_slice::<serde_json::Value>(json)
        .ok()?
        .as_object()
        .cloned()
}

/// All flat field names present in a JSON value (for `SELECT *`).
pub fn json_field_names(value: &[u8]) -> Option<Vec<String>> {
    parse_json_object(value).map(|o| o.keys().cloned().collect())
}

/// A serde JSON value as a `FieldValue`.
pub fn json_to_fieldvalue(v: &serde_json::Value) -> FieldValue {
    match v {
        serde_json::Value::Null => FieldValue::Null,
        serde_json::Value::Bool(b) => FieldValue::Bool(*b),
        serde_json::Value::String(s) => FieldValue::Str(s.clone()),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(FieldValue::I64)
            .unwrap_or_else(|| FieldValue::F64(n.as_f64().unwrap_or(0.0))),
        other => FieldValue::Str(other.to_string()),
    }
}

/// All fields of a json-flat IMap entry as `(name, value)`: the key column (from
/// the key blob) plus every JSON value field.
pub fn jsonflat_fields(key: &[u8], value: &[u8], key_col: &str) -> Vec<(String, FieldValue)> {
    let mut out = vec![(key_col.to_string(), decode_key_data(key))];
    if let Some(obj) = parse_json_object(value) {
        for (k, v) in obj.iter() {
            out.push((k.clone(), json_to_fieldvalue(v)));
        }
    }
    out
}

/// A flat field map (e.g. a parsed Kafka record) as `(name, value)` pairs.
pub fn json_record_fields(json: &str) -> Vec<(String, FieldValue)> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .map(|o| {
            o.iter()
                .map(|(k, v)| (k.clone(), json_to_fieldvalue(v)))
                .collect()
        })
        .unwrap_or_default()
}

fn data_blob(type_id: i32, payload: &[u8]) -> Vec<u8> {
    let mut d = Vec::with_capacity(DATA_OFFSET + payload.len());
    d.extend_from_slice(&0i32.to_be_bytes()); // partitionHash
    d.extend_from_slice(&type_id.to_be_bytes());
    d.extend_from_slice(payload);
    d
}

fn len_prefixed(s: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(4 + s.len());
    p.extend_from_slice(&(s.len() as i32).to_be_bytes());
    p.extend_from_slice(s);
    p
}

/// A String `Data` blob (IMap key for a VARCHAR column).
pub fn string_data(s: &str) -> Vec<u8> {
    data_blob(T_STRING, &len_prefixed(s.as_bytes()))
}

/// An Integer `Data` blob (IMap key for an INT column).
pub fn int_data(i: i32) -> Vec<u8> {
    data_blob(T_INTEGER, &i.to_be_bytes())
}

/// A HazelcastJsonValue `Data` blob from a JSON object string.
pub fn json_value_data(json: &str) -> Vec<u8> {
    data_blob(T_JSON, &len_prefixed(json.as_bytes()))
}

/// Decode an IMap key `Data` blob to a field value (String or Integer keys).
pub fn decode_key_data(key: &[u8]) -> FieldValue {
    if key.len() < DATA_OFFSET {
        return FieldValue::Null;
    }
    let ty = i32::from_be_bytes(key[4..8].try_into().unwrap());
    match ty {
        T_STRING => {
            let p = &key[DATA_OFFSET..];
            if p.len() < 4 {
                return FieldValue::Null;
            }
            let len = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
            match p.get(4..4 + len) {
                Some(s) => FieldValue::Str(String::from_utf8_lossy(s).into_owned()),
                None => FieldValue::Null,
            }
        }
        T_INTEGER => match key.get(DATA_OFFSET..DATA_OFFSET + 4) {
            Some(s) => FieldValue::I32(i32::from_be_bytes(s.try_into().unwrap())),
            None => FieldValue::Null,
        },
        _ => FieldValue::Null,
    }
}

/// Build a flat JSON object string from `(field, value)` pairs.
pub fn json_object(fields: &[(String, FieldValue)]) -> String {
    let mut m = serde_json::Map::new();
    for (k, v) in fields {
        let jv = match v {
            FieldValue::Null => serde_json::Value::Null,
            FieldValue::Bool(b) => serde_json::Value::Bool(*b),
            FieldValue::I32(i) => serde_json::Value::from(*i),
            FieldValue::I64(i) => serde_json::Value::from(*i),
            FieldValue::F64(f) => serde_json::Value::from(*f),
            FieldValue::Str(s) => serde_json::Value::String(s.clone()),
        };
        m.insert(k.clone(), jv);
    }
    serde_json::Value::Object(m).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_value_roundtrip_and_extract() {
        let blob = json_value_data(r#"{"starter":"Soup","n":7}"#);
        let s = SchemaService::new();
        let ex = JsonExtractor;
        assert_eq!(
            ex.extract(&blob, &s, "starter"),
            FieldValue::Str("Soup".into())
        );
        assert_eq!(ex.extract(&blob, &s, "n"), FieldValue::I64(7));
        assert_eq!(ex.extract(&blob, &s, "missing"), FieldValue::Null);
        let mut names = json_field_names(&blob).unwrap();
        names.sort();
        assert_eq!(names, vec!["n", "starter"]);
    }

    #[test]
    fn build_object() {
        let j = json_object(&[
            ("a".into(), FieldValue::Str("x".into())),
            ("b".into(), FieldValue::I64(2)),
        ]);
        // serde preserves insertion order for Map by default feature? Use contains checks.
        assert!(j.contains("\"a\":\"x\""));
        assert!(j.contains("\"b\":2"));
    }
}
