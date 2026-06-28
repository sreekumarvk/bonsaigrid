use std::collections::{BTreeMap, HashMap, HashSet};
use serialization::compact::FieldValue;

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum IndexType {
    Sorted = 0,
    Hash = 1,
    Bitmap = 2,
}

impl IndexType {
    pub fn from_i32(v: i32) -> Self {
        match v {
            0 => IndexType::Sorted,
            1 => IndexType::Hash,
            2 => IndexType::Bitmap,
            _ => IndexType::Sorted,
        }
    }
}

#[derive(Clone, Debug)]
pub struct IndexConfig {
    pub name: Option<String>,
    pub ty: IndexType,
    pub attributes: Vec<String>,
}

#[derive(Clone, Debug)]
pub enum Index {
    Sorted(BTreeMap<FieldValue, HashSet<Vec<u8>>>),
    Hash(HashMap<FieldValue, HashSet<Vec<u8>>>),
}

impl Index {
    pub fn new(ty: IndexType) -> Self {
        match ty {
            IndexType::Sorted => Index::Sorted(BTreeMap::new()),
            IndexType::Hash | IndexType::Bitmap => Index::Hash(HashMap::new()),
        }
    }

    pub fn insert(&mut self, val: FieldValue, key: Vec<u8>) {
        match self {
            Index::Sorted(map) => {
                map.entry(val).or_default().insert(key);
            }
            Index::Hash(map) => {
                map.entry(val).or_default().insert(key);
            }
        }
    }

    pub fn remove(&mut self, val: &FieldValue, key: &[u8]) {
        match self {
            Index::Sorted(map) => {
                if let Some(set) = map.get_mut(val) {
                    set.remove(key);
                    if set.is_empty() {
                        map.remove(val);
                    }
                }
            }
            Index::Hash(map) => {
                if let Some(set) = map.get_mut(val) {
                    set.remove(key);
                    if set.is_empty() {
                        map.remove(val);
                    }
                }
            }
        }
    }

    /// Return the matching keys for the given predicate comparison operation.
    pub fn query(&self, op: crate::Op, val: &FieldValue) -> HashSet<Vec<u8>> {
        let mut out = HashSet::new();
        match self {
            Index::Sorted(map) => {
                match op {
                    crate::Op::Eq => {
                        if let Some(set) = map.get(val) {
                            out.extend(set.iter().cloned());
                        }
                    }
                    crate::Op::NotEq => {
                        for (k, set) in map {
                            if k != val {
                                out.extend(set.iter().cloned());
                            }
                        }
                    }
                    crate::Op::Lt => {
                        for (_, set) in map.range(..val) {
                            out.extend(set.iter().cloned());
                        }
                    }
                    crate::Op::Le => {
                        for (_, set) in map.range(..=val) {
                            out.extend(set.iter().cloned());
                        }
                    }
                    crate::Op::Gt => {
                        use std::ops::Bound;
                        for (_, set) in map.range((Bound::Excluded(val), Bound::Unbounded)) {
                            out.extend(set.iter().cloned());
                        }
                    }
                    crate::Op::Ge => {
                        use std::ops::Bound;
                        for (_, set) in map.range((Bound::Included(val), Bound::Unbounded)) {
                            out.extend(set.iter().cloned());
                        }
                    }
                }
            }
            Index::Hash(map) => {
                match op {
                    crate::Op::Eq => {
                        if let Some(set) = map.get(val) {
                            out.extend(set.iter().cloned());
                        }
                    }
                    crate::Op::NotEq => {
                        for (k, set) in map {
                            if k != val {
                                out.extend(set.iter().cloned());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        out
    }

    /// Return the matching keys for a between range query.
    pub fn query_between(&self, from: &FieldValue, to: &FieldValue) -> HashSet<Vec<u8>> {
        let mut out = HashSet::new();
        match self {
            Index::Sorted(map) => {
                for (_, set) in map.range(from..=to) {
                    out.extend(set.iter().cloned());
                }
            }
            Index::Hash(_) => {}
        }
        out
    }
}

/// Query Planner: analyzes the predicate and returns the list of matching keys
/// using the available indexes, or returns None if no index is applicable.
pub fn plan_and_resolve(
    predicate: &crate::Predicate,
    indexes: &HashMap<String, Index>,
) -> Option<HashSet<Vec<u8>>> {
    match predicate {
        crate::Predicate::Compare { field, op, value } => {
            if let Some(index) = indexes.get(field) {
                if matches!(op, crate::Op::Eq | crate::Op::NotEq) || matches!(index, Index::Sorted(_)) {
                    return Some(index.query(*op, value));
                }
            }
            None
        }
        crate::Predicate::Between { field, from, to } => {
            if let Some(index) = indexes.get(field) {
                if let Index::Sorted(_) = index {
                    return Some(index.query_between(from, to));
                }
            }
            None
        }
        crate::Predicate::In { field, values } => {
            if let Some(index) = indexes.get(field) {
                let mut out = HashSet::new();
                for val in values {
                    out.extend(index.query(crate::Op::Eq, val));
                }
                return Some(out);
            }
            None
        }
        crate::Predicate::And(preds) => {
            for pred in preds {
                if let Some(keys) = plan_and_resolve(pred, indexes) {
                    return Some(keys);
                }
            }
            None
        }
        _ => None,
    }
}
