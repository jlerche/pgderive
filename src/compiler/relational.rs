//! Resolved relational nodes lowered into executable durable circuits.
use super::{ColumnRef, Projected};
use serde::Serialize;
#[derive(Clone, Serialize)]
pub struct Source {
    pub(crate) schema: String,
    pub(crate) table: String,
}
#[derive(Clone, Serialize)]
pub struct Binding {
    pub(crate) input: ColumnRef,
    pub(crate) output: ColumnRef,
}
#[derive(Clone, Serialize)]
pub enum Node {
    Source { id: String, source: usize },
    Filter { id: String, input: String, predicate: super::Predicate },
    Expand { id: String, input: String, series: super::expansion::Series },
    Map { id: String, input: String, computed: Vec<super::scalar::Computed> },
    KeyBy { id: String, input: String, key: ColumnRef },
    Rebind { id: String, input: String, fields: Vec<Binding> },
    CrossKey { id: String, input: String },
    EquijoinKey { id: String, input: String, keys: Vec<ColumnRef> },
    Join { id: String, left: String, right: String },
    Union { id: String, left: String, right: String },
    LookupInput { id: String, input: String, spec: super::lookup::Input },
    Lookup { id: String, input: String, spec: super::lookup::Lookup },
    Project { id: String, input: String },
    Output { id: String, input: String },
    PartitionBy { id: String, input: String, keys: Vec<ColumnRef> },
    Partition { id: String, input: String, spec: super::partition::Partition },
    Statistics { id: String, input: String, spec: super::partition::Partition },
    DistinctInput { id: String, input: String, spec: super::partition::Partition },
    DistinctCounts { id: String, input: String, spec: super::partition::Partition },
    DistinctMembership { id: String, input: String, spec: super::partition::Partition },
    OrderedInput { id: String, input: String, spec: super::partition::Partition },
    OrderedFirst { id: String, input: String, spec: super::partition::Partition },
    OrderedGroup { id: String, input: String },
    OrderedFinalize { id: String, input: String, spec: super::partition::Partition },
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
