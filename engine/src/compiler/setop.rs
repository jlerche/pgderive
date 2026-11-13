//! SQL set multiplicities after positional native type reconciliation.
use super::ColumnRef;
use serde::Serialize;
#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Kind {
    Union,
    Intersect,
    Except,
}
#[derive(Clone, Serialize)]
pub struct Spec {
    pub(crate) kind: Kind,
    pub(crate) all: bool,
    pub(crate) columns: Vec<ColumnRef>,
}
