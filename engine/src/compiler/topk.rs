//! Typed occurrence selection preserving the explicit `PostgreSQL` sort contract.
use super::partition::Order;
use serde::Serialize;
#[derive(Clone, Serialize)]
pub struct Spec {
    pub(crate) order: Vec<Order>,
    pub(crate) field: Option<String>,
    pub(crate) limit: usize,
    pub(crate) offset: usize,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(crate) with_ties: bool,
    pub(crate) identity: Vec<super::ColumnRef>,
}
