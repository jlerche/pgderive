//! Native join kinds and tagged outer-join nullable output contracts.
use super::ColumnRef;
use serde::Serialize;
#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Kind {
    Inner,
    Left,
    Right,
    Full,
}
#[derive(Clone, Serialize)]
pub struct Input {
    pub(crate) left: bool,
    pub(crate) keys: Vec<ColumnRef>,
}
#[derive(Clone, Serialize)]
pub struct Spec {
    pub(crate) left: Vec<ColumnRef>,
    pub(crate) full: bool,
    pub(crate) right: Vec<ColumnRef>,
    pub(crate) predicate: Option<super::Predicate>,
}
