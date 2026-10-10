//! Typed partition aggregates and row-position window semantics.
use super::{ColumnRef, Predicate};
use serde::Serialize;
#[derive(Clone, Serialize)]
pub enum Function {
    Count,
    Sum,
    Average,
    Min,
    Max,
}
#[derive(Clone, Serialize)]
pub struct Aggregate {
    pub(crate) function: Function,
    pub(crate) argument: Option<ColumnRef>,
    pub(crate) filter: Option<Predicate>,
    pub(crate) field: String,
}
#[derive(Clone, Serialize)]
pub struct Order {
    pub(crate) column: ColumnRef,
    pub(crate) descending: bool,
    pub(crate) nulls_first: bool,
}
#[derive(Clone, Serialize)]
pub struct Frame {
    // None means unbounded; signed offset relative to current row.
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
}
#[derive(Clone, Serialize)]
pub enum Mode {
    Grouped { keys: Vec<ColumnRef> },
    Rows { order: Vec<Order>, frame: Frame },
}
#[derive(Clone, Serialize)]
pub struct Partition {
    pub(crate) mode: Mode,
    pub(crate) aggregates: Vec<Aggregate>,
}
