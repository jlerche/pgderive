//! Resolved relational nodes lowered into executable durable circuits.
use super::{ColumnRef, Projected};
use serde::Serialize;
#[derive(Clone, Serialize)]
pub struct Source {
    pub(crate) schema: String,
    pub(crate) table: String,
}
#[derive(Clone, Serialize)]
pub enum Node {
    Source { id: String, source: usize },
    Map { id: String, input: String, computed: Vec<super::scalar::Computed> },
    KeyBy { id: String, input: String, key: ColumnRef },
    Join { id: String, left: String, right: String },
    Project { id: String, input: String },
    Output { id: String, input: String },
    PartitionBy { id: String, input: String, keys: Vec<ColumnRef> },
    Partition { id: String, input: String, spec: super::partition::Partition },
    Statistics { id: String, input: String, spec: super::partition::Partition },
    Finalize { id: String, input: String, spec: super::partition::Partition },
}
#[derive(Clone, Serialize)]
pub struct Relational {
    pub(crate) sources: Vec<Source>,
    pub(crate) nodes: Vec<Node>,
    pub(crate) output: Projected,
}
pub fn field(source: usize, name: &str) -> String {
    format!("{source}:{name}")
}
