//! Evaluate a `Predicate` against a serialized value via a `FieldExtractor`.

use crate::{Op, Predicate};
use serialization::compact::{FieldExtractor, FieldValue};
use serialization::schema::SchemaService;
use std::cmp::Ordering;

pub fn like_to_regex(expr: &str, case_insensitive: bool) -> Result<regex::Regex, regex::Error> {
    let mut re_str = String::new();
    re_str.push_str("^");
    let mut chars = expr.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                if let Some(&next) = chars.peek() {
                    if next == '%' || next == '_' || next == '\\' {
                        re_str.push_str(&regex::escape(&next.to_string()));
                        chars.next();
                    } else {
                        re_str.push_str(&regex::escape("\\"));
                    }
                } else {
                    re_str.push_str(&regex::escape("\\"));
                }
            }
            '%' => {
                re_str.push_str(".*");
            }
            '_' => {
                re_str.push_str(".");
            }
            other => {
                re_str.push_str(&regex::escape(&other.to_string()));
            }
        }
    }
    re_str.push_str("$");

    regex::RegexBuilder::new(&re_str)
        .dot_matches_new_line(true)
        .case_insensitive(case_insensitive)
        .build()
}

pub fn compile_regex(user_regex: &str) -> Result<regex::Regex, regex::Error> {
    let anchored = format!("^(?:{})$", user_regex);
    regex::RegexBuilder::new(&anchored)
        .dot_matches_new_line(true)
        .build()
}

/// True if `value` (a serialized record) satisfies `predicate`. A field that is
/// absent, null, or not comparable to the predicate constant does not match.
pub fn eval(predicate: &Predicate, value: &[u8], schemas: &SchemaService, ex: &dyn FieldExtractor) -> bool {
    match predicate {
        Predicate::Compare { field, op, value: pv } => {
            let fv = ex.extract(value, schemas, field);
            match fv.compare(pv) {
                Some(ord) => match op {
                    Op::Eq => ord == Ordering::Equal,
                    Op::NotEq => ord != Ordering::Equal,
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
        Predicate::Not(inner) => !eval(inner, value, schemas, ex),
        Predicate::Between { field, from, to } => {
            let fv = ex.extract(value, schemas, field);
            match (fv.compare(from), fv.compare(to)) {
                (Some(ord_from), Some(ord_to)) => {
                    ord_from != Ordering::Less && ord_to != Ordering::Greater
                }
                _ => false,
            }
        }
        Predicate::In { field, values } => {
            let fv = ex.extract(value, schemas, field);
            values.iter().any(|v| fv.equals(v))
        }
        Predicate::Like { field, expr } => {
            let fv = ex.extract(value, schemas, field);
            if let FieldValue::Str(s) = fv {
                if let Ok(re) = like_to_regex(expr, false) {
                    re.is_match(&s)
                } else {
                    false
                }
            } else {
                false
            }
        }
        Predicate::ILike { field, expr } => {
            let fv = ex.extract(value, schemas, field);
            if let FieldValue::Str(s) = fv {
                if let Ok(re) = like_to_regex(expr, true) {
                    re.is_match(&s)
                } else {
                    false
                }
            } else {
                false
            }
        }
        Predicate::Regex { field, regex } => {
            let fv = ex.extract(value, schemas, field);
            if let FieldValue::Str(s) = fv {
                if let Ok(re) = compile_regex(regex) {
                    re.is_match(&s)
                } else {
                    false
                }
            } else {
                false
            }
        }
        Predicate::True => true,
        Predicate::False => false,
        Predicate::Sql(_) => false,
        Predicate::Paging { inner, .. } => {
            if let Some(inner_pred) = inner {
                eval(inner_pred, value, schemas, ex)
            } else {
                true
            }
        }
        Predicate::Partition { target, .. } => eval(target, value, schemas, ex),
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

    #[test]
    fn evaluates_new_predicates() {
        let sc = schemas();
        let v = person_value();
        let ex = CompactExtractor;

        // name != "bob" (should be true since name is "alice")
        let ne_bob = Predicate::Compare {
            field: "name".into(),
            op: Op::NotEq,
            value: FieldValue::Str("bob".into()),
        };
        assert!(eval(&ne_bob, &v, &sc, &ex));

        // name != "alice" (should be false)
        let ne_alice = Predicate::Compare {
            field: "name".into(),
            op: Op::NotEq,
            value: FieldValue::Str("alice".into()),
        };
        assert!(!eval(&ne_alice, &v, &sc, &ex));

        // not(age > 40) (should be true since age is 35)
        let gt40 = Predicate::Compare {
            field: "age".into(),
            op: Op::Gt,
            value: FieldValue::I32(40),
        };
        let not_gt40 = Predicate::Not(Box::new(gt40));
        assert!(eval(&not_gt40, &v, &sc, &ex));

        // age between 30 and 40 (true)
        let bet_30_40 = Predicate::Between {
            field: "age".into(),
            from: FieldValue::I32(30),
            to: FieldValue::I32(40),
        };
        assert!(eval(&bet_30_40, &v, &sc, &ex));

        // age between 10 and 20 (false)
        let bet_10_20 = Predicate::Between {
            field: "age".into(),
            from: FieldValue::I32(10),
            to: FieldValue::I32(20),
        };
        assert!(!eval(&bet_10_20, &v, &sc, &ex));

        // age in [30, 35, 40] (true)
        let in_list = Predicate::In {
            field: "age".into(),
            values: vec![FieldValue::I32(30), FieldValue::I32(35), FieldValue::I32(40)],
        };
        assert!(eval(&in_list, &v, &sc, &ex));

        // age in [10, 20] (false)
        let not_in_list = Predicate::In {
            field: "age".into(),
            values: vec![FieldValue::I32(10), FieldValue::I32(20)],
        };
        assert!(!eval(&not_in_list, &v, &sc, &ex));

        // name like "al%ce" (true)
        let like_alice = Predicate::Like {
            field: "name".into(),
            expr: "al%ce".into(),
        };
        assert!(eval(&like_alice, &v, &sc, &ex));

        // name like "al_e" (false since "alice" has 5 characters)
        let like_alice_four_chars = Predicate::Like {
            field: "name".into(),
            expr: "al_e".into(),
        };
        assert!(!eval(&like_alice_four_chars, &v, &sc, &ex));



        // name like "AL%CE" (false due to case)
        let like_alice_case = Predicate::Like {
            field: "name".into(),
            expr: "AL%CE".into(),
        };
        assert!(!eval(&like_alice_case, &v, &sc, &ex));

        // name ilike "AL%CE" (true due to case-insensitivity)
        let ilike_alice = Predicate::ILike {
            field: "name".into(),
            expr: "AL%CE".into(),
        };
        assert!(eval(&ilike_alice, &v, &sc, &ex));

        // name regex "al.*ce" (true)
        let regex_alice = Predicate::Regex {
            field: "name".into(),
            regex: "al.*ce".into(),
        };
        assert!(eval(&regex_alice, &v, &sc, &ex));

        // name regex "bob" (false)
        let regex_bob = Predicate::Regex {
            field: "name".into(),
            regex: "bob".into(),
        };
        assert!(!eval(&regex_bob, &v, &sc, &ex));

        // True / False
        assert!(eval(&Predicate::True, &v, &sc, &ex));
        assert!(!eval(&Predicate::False, &v, &sc, &ex));
    }
}
