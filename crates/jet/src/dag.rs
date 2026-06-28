use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Edge {
    pub source_vertex: String,
    pub source_ordinal: i32,
    pub dest_vertex: String,
    pub dest_ordinal: i32,
    pub priority: i32,
    pub is_buffered: bool,
}

#[derive(Debug, Clone)]
pub struct Vertex {
    pub name: String,
    pub processor_meta: Vec<u8>,
    pub local_parallelism: i32,
}

#[derive(Debug, Clone)]
pub struct Dag {
    pub vertices: Vec<Vertex>,
    pub edges: Vec<Edge>,
}

impl Dag {
    pub fn new() -> Self {
        Self {
            vertices: Vec::new(),
            edges: Vec::new(),
        }
    }
}
