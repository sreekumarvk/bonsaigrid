//! Predicate AST, wire decoder, and evaluator.
//!
//! Predicates arrive as a serialized `Data` whose serializer type is
//! IdentifiedDataSerializable (type id -2). The IDS body (all big-endian) is:
//!   [identified: u8 = 1][factoryId: i32 = -20][classId: i32][fields...]
//!
//! Predicate classes (PredicateDataSerializerHook, factory -20):
//!   1 = And        : [count: i32][child: object]*       (children AND-ed)
//!   2 = Or         : [count: i32][child: object]*       (children OR-ed)
//!   3 = Equal      : [attr: string][value: object]
//!   4 = GreaterLess: [attr: string][value: object][equal: u8][less: u8]
//!
//! A child `object` is `writeObject` form: `[type i32][body]`; for a nested
//! predicate the type is -2 (IDS) again. A predicate `value` object is a
//! constant scalar: `[type i32][data]` where type is INTEGER(-7)/LONG(-8)/
//! DOUBLE(-10)/BOOLEAN(-4)/STRING(-11).
//!
//! Grounded against real client captures (e.g. equal(age,30) ->
//! `01 ffffffec 00000003 00000003 616765 fffffff9 0000001e`).

mod eval;
pub mod json;
pub mod sql;
pub mod index;
pub use eval::eval;

use serialization::compact::{FieldExtractor, FieldValue};
use serialization::schema::SchemaService;

pub fn field_value_partition_id(val: &FieldValue, partition_count: i32) -> i32 {
    let mut payload = Vec::new();
    match val {
        FieldValue::Str(s) => {
            let bytes = s.as_bytes();
            payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            payload.extend_from_slice(bytes);
        }
        FieldValue::I32(v) => {
            payload.extend_from_slice(&v.to_be_bytes());
        }
        FieldValue::I64(v) => {
            payload.extend_from_slice(&v.to_be_bytes());
        }
        FieldValue::Bool(v) => {
            payload.push(if *v { 1 } else { 0 });
        }
        FieldValue::F64(v) => {
            payload.extend_from_slice(&v.to_bits().to_be_bytes());
        }
        FieldValue::Null => {}
    }
    let h = if payload.is_empty() {
        0
    } else {
        serialization::murmur3_x86_32(&payload, 0x01000193) // MURMUR_SEED = 0x01000193
    };
    if h == i32::MIN {
        0
    } else {
        h.wrapping_abs() % partition_count
    }
}

/// Run `predicate` over candidate entries and return the matching `(key, value)`
/// pairs. Today the candidate set is every entry (full scan); an index would
/// supply a narrower candidate iterator without changing this signature — the
/// seam for future indexed queries. The predicate is evaluated against the
/// **value** (Compact records live in the value).
pub fn scan<I>(
    predicate: &Predicate,
    entries: I,
    schemas: &SchemaService,
    ex: &dyn FieldExtractor,
) -> Vec<(Vec<u8>, Vec<u8>)>
where
    I: IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
{
    match predicate {
        Predicate::Paging { inner, page, page_size, iteration_type: _ } => {
            let mut matched: Vec<(Vec<u8>, Vec<u8>)> = if let Some(inner_pred) = inner {
                entries
                    .into_iter()
                    .filter(|(_, v)| eval(inner_pred, v, schemas, ex))
                    .collect()
            } else {
                entries.into_iter().collect()
            };

            // Sort matched entries lexicographically by key as a stable, zero-allocation sorted order.
            matched.sort_by(|a, b| a.0.cmp(&b.0));

            let start = (*page as usize * *page_size as usize).min(matched.len());
            let end = (start + *page_size as usize).min(matched.len());
            matched[start..end].to_vec()
        }
        Predicate::Partition { partition_id, target } => {
            entries
                .into_iter()
                .filter(|(k, _)| {
                    let p = serialization::partition_id(k, 271); // PARTITION_COUNT = 271
                    p == *partition_id
                })
                .filter(|(_, v)| eval(target, v, schemas, ex))
                .collect()
        }
        other => {
            entries
                .into_iter()
                .filter(|(_, v)| eval(other, v, schemas, ex))
                .collect()
        }
    }
}

const TYPE_IDS: i32 = -2;
const CLASS_SQL: i32 = 0;
const CLASS_AND: i32 = 1;
const CLASS_BETWEEN: i32 = 2;
const CLASS_EQUAL: i32 = 3;
const CLASS_GREATER_LESS: i32 = 4;
const CLASS_LIKE: i32 = 5;
const CLASS_ILIKE: i32 = 6;
const CLASS_IN: i32 = 7;
const CLASS_INSTANCEOF: i32 = 8;
const CLASS_NOTEQUAL: i32 = 9;
const CLASS_NOT: i32 = 10;
const CLASS_OR: i32 = 11;
const CLASS_REGEX: i32 = 12;
const CLASS_FALSE: i32 = 13;
const CLASS_TRUE: i32 = 14;
const CLASS_PAGING: i32 = 15;
const CLASS_PARTITION: i32 = 16;

// Constant serializer type ids for scalar predicate values.
const T_BOOLEAN: i32 = -4;
const T_INTEGER: i32 = -7;
const T_LONG: i32 = -8;
const T_DOUBLE: i32 = -10;
const T_STRING: i32 = -11;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Eq,
    NotEq,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    Compare { field: String, op: Op, value: FieldValue },
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
    Between { field: String, from: FieldValue, to: FieldValue },
    In { field: String, values: Vec<FieldValue> },
    Like { field: String, expr: String },
    ILike { field: String, expr: String },
    Regex { field: String, regex: String },
    True,
    False,
    Sql(String),
    Paging {
        inner: Option<Box<Predicate>>,
        page: i32,
        page_size: i32,
        iteration_type: String,
    },
    Partition {
        partition_id: i32,
        target: Box<Predicate>,
    },
    /// Unsupported / malformed predicate — matches nothing (safe default).
    MatchNone,
}


/// Decode a predicate from its full `Data` blob (8-byte header + IDS body).
pub fn decode(data: &[u8]) -> Predicate {
    let mut cur = Cur { b: data, pos: 8 };
    // The serializer type lives in the Data header at [4..8].
    if serialization::type_id(data) != TYPE_IDS {
        return Predicate::MatchNone;
    }
    decode_ids(&mut cur).unwrap_or(Predicate::MatchNone)
}

/// Decode a child written via `writeObject`: `[type i32][body]`.
fn decode_object(c: &mut Cur) -> Option<Predicate> {
    let ty = c.i32()?;
    if ty != TYPE_IDS {
        return Some(Predicate::MatchNone);
    }
    decode_ids(c)
}

/// Decode an IDS body positioned at the `identified` byte.
fn decode_ids(c: &mut Cur) -> Option<Predicate> {
    let _identified = c.u8()?;
    let _factory = c.i32()?; // -20 (PredicateDataSerializerHook) for the kinds we support
    let class_id = c.i32()?;
    match class_id {
        CLASS_SQL => {
            let sql = c.string()?;
            Some(Predicate::Sql(sql))
        }
        CLASS_EQUAL => {
            let field = c.string()?;
            let value = decode_value(c)?;
            Some(Predicate::Compare { field, op: Op::Eq, value })
        }
        CLASS_GREATER_LESS => {
            let field = c.string()?;
            let value = decode_value(c)?;
            let equal = c.u8()? != 0;
            let less = c.u8()? != 0;
            let op = match (equal, less) {
                (false, false) => Op::Gt,
                (true, false) => Op::Ge,
                (false, true) => Op::Lt,
                (true, true) => Op::Le,
            };
            Some(Predicate::Compare { field, op, value })
        }
        CLASS_BETWEEN => {
            let field = c.string()?;
            let to = decode_value(c)?;
            let from = decode_value(c)?;
            Some(Predicate::Between { field, from, to })
        }
        CLASS_LIKE => {
            let field = c.string()?;
            let expr = c.string()?;
            Some(Predicate::Like { field, expr })
        }
        CLASS_ILIKE => {
            let field = c.string()?;
            let expr = c.string()?;
            Some(Predicate::ILike { field, expr })
        }
        CLASS_IN => {
            let field = c.string()?;
            let len = c.i32()?;
            if !(0..=4096).contains(&len) {
                return Some(Predicate::MatchNone);
            }
            let mut values = Vec::with_capacity(len as usize);
            for _ in 0..len {
                values.push(decode_value(c)?);
            }
            Some(Predicate::In { field, values })
        }
        CLASS_NOTEQUAL => {
            let field = c.string()?;
            let value = decode_value(c)?;
            Some(Predicate::Compare { field, op: Op::NotEq, value })
        }
        CLASS_NOT => {
            let inner = decode_object(c)?;
            Some(Predicate::Not(Box::new(inner)))
        }
        CLASS_REGEX => {
            let field = c.string()?;
            let regex = c.string()?;
            Some(Predicate::Regex { field, regex })
        }
        CLASS_FALSE => {
            Some(Predicate::False)
        }
        CLASS_TRUE => {
            Some(Predicate::True)
        }
        CLASS_AND | CLASS_OR => {
            let count = c.i32()?;
            if !(0..=4096).contains(&count) {
                return Some(Predicate::MatchNone);
            }
            let mut children = Vec::with_capacity(count as usize);
            for _ in 0..count {
                children.push(decode_object(c)?);
            }
            Some(if class_id == CLASS_AND {
                Predicate::And(children)
            } else {
                Predicate::Or(children)
            })
        }
        CLASS_PAGING => {
            let first_val = c.i32()?;
            let inner = if first_val == -2 || first_val == 0 {
                if first_val == -2 {
                    Some(decode_ids(c)?)
                } else {
                    None
                }
            } else {
                if first_val > 0 {
                    c.pos += first_val as usize;
                }
                let pred_ty = c.i32()?;
                if pred_ty == -2 {
                    Some(decode_ids(c)?)
                } else {
                    None
                }
            };
            
            skip_object(c)?; // comparator
            let page = c.i32()?;
            let page_size = c.i32()?;
            let iteration_type = c.string()?;
            
            let anchor_size = c.i32()?;
            if !(0..=4096).contains(&anchor_size) {
                return Some(Predicate::MatchNone);
            }
            for _ in 0..anchor_size {
                let _anchor_page = c.i32()?;
                skip_object(c)?; // key
                skip_object(c)?; // val
            }
            
            Some(Predicate::Paging {
                inner: inner.map(Box::new),
                page,
                page_size,
                iteration_type,
            })
        }
        CLASS_PARTITION => {
            let partition_key = decode_value(c)?;
            let target = decode_object(c)?;
            let partition_id = field_value_partition_id(&partition_key, 271);
            Some(Predicate::Partition { partition_id, target: Box::new(target) })
        }
        _ => Some(Predicate::MatchNone),
    }
}

fn skip_object(c: &mut Cur) -> Option<()> {
    let ty = c.i32()?;
    match ty {
        0 => Some(()), // null
        T_INTEGER => { c.i32()?; Some(()) }
        T_LONG => { c.i64()?; Some(()) }
        T_DOUBLE => { c.i64()?; Some(()) }
        T_BOOLEAN => { c.u8()?; Some(()) }
        T_STRING => { c.string()?; Some(()) }
        TYPE_IDS => {
            let _pred = decode_ids(c)?;
            Some(())
        }
        _ => {
            Some(())
        }
    }
}

/// Decode a constant scalar value object: `[type i32][data]`.
fn decode_value(c: &mut Cur) -> Option<FieldValue> {
    let ty = c.i32()?;
    Some(match ty {
        T_INTEGER => FieldValue::I32(c.i32()?),
        T_LONG => FieldValue::I64(c.i64()?),
        T_DOUBLE => FieldValue::F64(f64::from_bits(c.i64()? as u64)),
        T_BOOLEAN => FieldValue::Bool(c.u8()? != 0),
        T_STRING => FieldValue::Str(c.string()?),
        _ => FieldValue::Null,
    })
}

/// Big-endian byte cursor; every read is bounds-checked and returns None past EOF.
struct Cur<'a> {
    b: &'a [u8],
    pos: usize,
}

impl Cur<'_> {
    fn u8(&mut self) -> Option<u8> {
        let v = *self.b.get(self.pos)?;
        self.pos += 1;
        Some(v)
    }
    fn i32(&mut self) -> Option<i32> {
        let s = self.b.get(self.pos..self.pos + 4)?;
        self.pos += 4;
        Some(i32::from_be_bytes(s.try_into().unwrap()))
    }
    fn i64(&mut self) -> Option<i64> {
        let s = self.b.get(self.pos..self.pos + 8)?;
        self.pos += 8;
        Some(i64::from_be_bytes(s.try_into().unwrap()))
    }
    fn string(&mut self) -> Option<String> {
        let len = self.i32()?;
        if len < 0 {
            return None;
        }
        let len = len as usize;
        let s = self.b.get(self.pos..self.pos + len)?;
        self.pos += len;
        Some(String::from_utf8_lossy(s).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }
    /// Wrap a captured IDS payload (the bytes after the Data header) in a Data
    /// blob with serializer type -2.
    fn data(payload_hex: &str) -> Vec<u8> {
        let mut d = vec![0u8; 4]; // partitionHash
        d.extend_from_slice(&TYPE_IDS.to_be_bytes());
        d.extend_from_slice(&hex(payload_hex));
        d
    }

    #[test]
    fn decodes_equal_int() {
        let p = decode(&data("01ffffffec0000000300000003616765fffffff90000001e"));
        assert_eq!(p, Predicate::Compare { field: "age".into(), op: Op::Eq, value: FieldValue::I32(30) });
    }

    #[test]
    fn decodes_equal_string() {
        let p = decode(&data("01ffffffec00000003000000046e616d65fffffff500000005616c696365"));
        assert_eq!(
            p,
            Predicate::Compare { field: "name".into(), op: Op::Eq, value: FieldValue::Str("alice".into()) }
        );
    }

    #[test]
    fn decodes_greater_and_greater_equal() {
        let gt = decode(&data("01ffffffec0000000400000003616765fffffff90000001e0000"));
        assert_eq!(gt, Predicate::Compare { field: "age".into(), op: Op::Gt, value: FieldValue::I32(30) });
        let ge = decode(&data("01ffffffec0000000400000003616765fffffff90000001e0100"));
        assert_eq!(ge, Predicate::Compare { field: "age".into(), op: Op::Ge, value: FieldValue::I32(30) });
    }

    #[test]
    fn decodes_and_of_two() {
        // and(name == "alice", age > 30)
        let p = decode(&data(
            "01ffffffec0000000100000002fffffffe01ffffffec00000003000000046e616d65fffffff500000005616c696365fffffffe01ffffffec0000000400000003616765fffffff90000001e0000",
        ));
        assert_eq!(
            p,
            Predicate::And(vec![
                Predicate::Compare { field: "name".into(), op: Op::Eq, value: FieldValue::Str("alice".into()) },
                Predicate::Compare { field: "age".into(), op: Op::Gt, value: FieldValue::I32(30) },
            ])
        );
    }

    #[test]
    fn non_ids_is_match_none() {
        let mut d = vec![0u8; 4];
        d.extend_from_slice(&(-11i32).to_be_bytes()); // a String, not a predicate
        d.extend_from_slice(b"oops");
        assert_eq!(decode(&d), Predicate::MatchNone);
    }

    #[test]
    fn truncated_is_match_none() {
        assert_eq!(decode(&data("01ffffffec000000")), Predicate::MatchNone);
    }

    #[test]
    fn decodes_between() {
        let p = decode(&data("01ffffffec0000000200000003616765fffffff900000014fffffff90000000a"));
        assert_eq!(p, Predicate::Between { field: "age".into(), from: FieldValue::I32(10), to: FieldValue::I32(20) });
    }

    #[test]
    fn decodes_like() {
        let p = decode(&data("01ffffffec00000005000000046e616d6500000003612562"));
        assert_eq!(p, Predicate::Like { field: "name".into(), expr: "a%b".into() });
    }

    #[test]
    fn decodes_ilike() {
        let p = decode(&data("01ffffffec00000006000000046e616d6500000003612562"));
        assert_eq!(p, Predicate::ILike { field: "name".into(), expr: "a%b".into() });
    }

    #[test]
    fn decodes_regex() {
        let p = decode(&data("01ffffffec0000000c000000046e616d6500000004612e2a62"));
        assert_eq!(p, Predicate::Regex { field: "name".into(), regex: "a.*b".into() });
    }

    #[test]
    fn decodes_in() {
        let p = decode(&data("01ffffffec000000070000000361676500000002fffffff90000000afffffff900000014"));
        assert_eq!(p, Predicate::In { field: "age".into(), values: vec![FieldValue::I32(10), FieldValue::I32(20)] });
    }

    #[test]
    fn decodes_notequal() {
        let p = decode(&data("01ffffffec0000000900000003616765fffffff90000001e"));
        assert_eq!(p, Predicate::Compare { field: "age".into(), op: Op::NotEq, value: FieldValue::I32(30) });
    }

    #[test]
    fn decodes_not() {
        let p = decode(&data("01ffffffec0000000afffffffe01ffffffec0000000300000003616765fffffff90000001e"));
        assert_eq!(p, Predicate::Not(Box::new(Predicate::Compare { field: "age".into(), op: Op::Eq, value: FieldValue::I32(30) })));
    }

    #[test]
    fn decodes_true_false() {
        let p_true = decode(&data("01ffffffec0000000e"));
        assert_eq!(p_true, Predicate::True);
        let p_false = decode(&data("01ffffffec0000000d"));
        assert_eq!(p_false, Predicate::False);
    }

    #[test]
    fn decodes_paging_pre_5_4() {
        let p = decode(&data("01ffffffec0000000ffffffffe01ffffffec0000000300000003616765fffffff90000001e00000000000000010000000a000000034b455900000000"));
        assert_eq!(p, Predicate::Paging {
            inner: Some(Box::new(Predicate::Compare { field: "age".into(), op: Op::Eq, value: FieldValue::I32(30) })),
            page: 1,
            page_size: 10,
            iteration_type: "KEY".into(),
        });
    }

    #[test]
    fn decodes_paging_5_4() {
        let p = decode(&data("01ffffffec0000000f000000026e73fffffffe01ffffffec0000000300000003616765fffffff90000001e00000000000000010000000a000000034b455900000000"));
        assert_eq!(p, Predicate::Paging {
            inner: Some(Box::new(Predicate::Compare { field: "age".into(), op: Op::Eq, value: FieldValue::I32(30) })),
            page: 1,
            page_size: 10,
            iteration_type: "KEY".into(),
        });
    }

    #[test]
    fn decodes_partition() {
        let p = decode(&data("01ffffffec00000010fffffff500000008757365722d313233fffffffe01ffffffec0000000300000003616765fffffff90000001e"));
        let expected_partition = field_value_partition_id(&FieldValue::Str("user-123".into()), 271);
        assert_eq!(p, Predicate::Partition {
            partition_id: expected_partition,
            target: Box::new(Predicate::Compare { field: "age".into(), op: Op::Eq, value: FieldValue::I32(30) }),
        });
    }

    #[test]
    fn evaluates_paging_scan() {
        let mut entries = Vec::new();
        // Create 20 mock entries where key is i32, value is dummy
        for i in 0..20 {
            entries.push((vec![i as u8], vec![]));
        }

        let schemas = SchemaService::new();
        let ex = serialization::compact::CompactExtractor;

        // Paging page 0 size 5 (first 5 elements)
        let p0 = Predicate::Paging {
            inner: None,
            page: 0,
            page_size: 5,
            iteration_type: "KEY".into(),
        };
        let res0 = scan(&p0, entries.clone(), &schemas, &ex);
        assert_eq!(res0.len(), 5);
        assert_eq!(res0[0].0, vec![0]);
        assert_eq!(res0[4].0, vec![4]);

        // Paging page 1 size 5 (elements 5 to 9)
        let p1 = Predicate::Paging {
            inner: None,
            page: 1,
            page_size: 5,
            iteration_type: "KEY".into(),
        };
        let res1 = scan(&p1, entries, &schemas, &ex);
        assert_eq!(res1.len(), 5);
        assert_eq!(res1[0].0, vec![5]);
        assert_eq!(res1[4].0, vec![9]);
    }
}
