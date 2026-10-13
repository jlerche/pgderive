//! Explicit left-join tagged input and nullable output contracts.
use super::ColumnRef;
use serde::Serialize;
#[derive(Clone, Serialize)]
pub struct Input {
    pub(crate) left: bool,
    pub(crate) keys: Vec<ColumnRef>,
}
#[derive(Clone, Serialize)]
pub struct Spec {
    pub(crate) right: Vec<ColumnRef>,
}
