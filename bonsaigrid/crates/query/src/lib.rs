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
pub use eval::eval;

use serialization::compact::FieldValue;

const TYPE_IDS: i32 = -2;
const FACTORY_PREDICATE: i32 = -20;
const CLASS_AND: i32 = 1;
const CLASS_OR: i32 = 2;
const CLASS_EQUAL: i32 = 3;
const CLASS_GREATER_LESS: i32 = 4;

// Constant serializer type ids for scalar predicate values.
const T_BOOLEAN: i32 = -4;
const T_INTEGER: i32 = -7;
const T_LONG: i32 = -8;
const T_DOUBLE: i32 = -10;
const T_STRING: i32 = -11;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Eq,
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
    let _factory = c.i32()?; // FACTORY_PREDICATE for the kinds we support
    let class_id = c.i32()?;
    match class_id {
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
        _ => Some(Predicate::MatchNone),
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
}
