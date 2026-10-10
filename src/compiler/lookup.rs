//! Typed `PostgreSQL` left predecessor lookup semantics.
use super::{ColumnRef, partition::Order};
use serde::Serialize;
pub const SIDE: &str = "@lookup_side";
#[derive(Clone, Serialize)]
pub struct Input {
    pub(crate) side: usize,
    pub(crate) keys: Vec<ColumnRef>,
}
#[derive(Clone, Serialize)]
pub struct Lookup {
    pub(crate) left_keys: Vec<ColumnRef>,
    pub(crate) left_bound: ColumnRef,
    pub(crate) right_bound: ColumnRef,
    pub(crate) inclusive: bool,
    pub(crate) order: Vec<Order>,
    pub(crate) right_fields: Vec<String>,
}
