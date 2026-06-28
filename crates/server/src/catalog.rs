//! SQL control-plane catalog: `CREATE MAPPING` definitions, process-global (one
//! per server). Not on the hot path; a `Mutex` is fine and avoids threading a
//! handle through every dispatch signature.

use query::sql::Mapping;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

fn mappings() -> &'static Mutex<HashMap<String, Mapping>> {
    static C: OnceLock<Mutex<HashMap<String, Mapping>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn put_mapping(m: Mapping) {
    mappings().lock().unwrap().insert(m.name.clone(), m);
}

pub fn get_mapping(name: &str) -> Option<Mapping> {
    mappings().lock().unwrap().get(name).cloned()
}
