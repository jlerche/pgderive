//! Typed occurrence selection preserving the explicit `PostgreSQL` sort contract.
use super::partition::Order;
use serde::Serialize;
#[derive(Clone, Serialize)]
pub struct Spec {
    pub(crate) order: Vec<Order>,
    pub(crate) field: Option<String>,
    pub(crate) limit: usize,
    pub(crate) offset: usize,
    pub(crate) identity: Vec<super::ColumnRef>,
}
