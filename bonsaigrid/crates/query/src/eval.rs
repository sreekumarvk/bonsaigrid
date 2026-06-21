//! Evaluate a `Predicate` against a serialized value via a `FieldExtractor`.

use crate::{Op, Predicate};
use serialization::compact::FieldExtractor;
use serialization::schema::SchemaService;
use std::cmp::Ordering;

/// True if `value` (a serialized record) satisfies `predicate`. A field that is
/// absent, null, or not comparable to the predicate constant does not match.
pub fn eval(predicate: &Predicate, value: &[u8], schemas: &SchemaService, ex: &dyn FieldExtractor) -> bool {
    match predicate {
        Predicate::Compare { field, op, value: pv } => {
            let fv = ex.extract(value, schemas, field);
            match fv.compare(pv) {
                Some(ord) => match op {
                    Op::Eq => ord == Ordering::Equal,
                    Op::Lt => ord == Ordering::Less,
                    Op::Le => ord != Ordering::Greater,
                    Op::Gt => ord == Ordering::Greater,
                    Op::Ge => ord != Ordering::Less,
                },
                None => false,
            }
        }
        Predicate::And(children) => children.iter().all(|c| eval(c, value, schemas, ex)),
        Predicate::Or(children) => children.iter().any(|c| eval(c, value, schemas, ex)),
        Predicate::MatchNone => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serialization::compact::CompactExtractor;
    use serialization::schema::{FieldDescriptor, Schema, INT32, STRING};
    use serialization::DATA_OFFSET;

    fn person_value() -> Vec<u8> {
        // Real Person{name:"alice", age:35} Compact payload.
        let payload: Vec<u8> = (0.."eac7fcf34f8f1c720000000d0000002300000005616c69636504".len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(
                    &"eac7fcf34f8f1c720000000d0000002300000005616c69636504"[i..i + 2],
                    16,
                )
                .unwrap()
            })
            .collect();
        let mut v = vec![0u8; DATA_OFFSET];
        v.extend_from_slice(&payload);
        v
    }

    fn schemas() -> SchemaService {
        let s = SchemaService::new();
        s.put(Schema::new(
            "person".into(),
            vec![FieldDescriptor::new("name".into(), STRING), FieldDescriptor::new("age".into(), INT32)],
        ));
        s
    }

    #[test]
    fn evaluates_compare_and_and_or() {
        let sc = schemas();
        let v = person_value();
        let ex = CompactExtractor;
        // age = 35
        let gt30 = Predicate::Compare {
            field: "age".into(),
            op: Op::Gt,
            value: serialization::compact::FieldValue::I32(30),
        };
        assert!(eval(&gt30, &v, &sc, &ex));
        let gt40 = Predicate::Compare {
            field: "age".into(),
            op: Op::Gt,
            value: serialization::compact::FieldValue::I32(40),
        };
        assert!(!eval(&gt40, &v, &sc, &ex));

        let name_alice = Predicate::Compare {
            field: "name".into(),
            op: Op::Eq,
            value: serialization::compact::FieldValue::Str("alice".into()),
        };
        assert!(eval(&Predicate::And(vec![gt30.clone(), name_alice.clone()]), &v, &sc, &ex));
        assert!(!eval(&Predicate::And(vec![gt40.clone(), name_alice.clone()]), &v, &sc, &ex));
        assert!(eval(&Predicate::Or(vec![gt40, name_alice]), &v, &sc, &ex));

        assert!(!eval(&Predicate::MatchNone, &v, &sc, &ex));
    }
}
