//! A nullable SQL membership marker preserves the complete outer occurrence.
use super::{ColumnRef, Predicate};
use serde::Serialize;
pub const MULTIPLE: &str = "\0multiple";
pub const CHECK: &str = "\0scalar_check";
#[derive(Clone, Serialize)]
pub struct Mark {
    pub(crate) probe: ColumnRef,
    pub(crate) value: ColumnRef,
    pub(crate) output: ColumnRef,
    pub(crate) comparison: Predicate,
    pub(crate) correlation: Option<Predicate>,
    pub(crate) semijoin: bool,
}
