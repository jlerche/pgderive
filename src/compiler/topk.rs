//! Typed occurrence selection preserving the explicit `PostgreSQL` sort contract.
use super::partition::Order;
use serde::Serialize;
#[derive(Clone, Serialize)]
pub struct Spec {
    pub(crate) order: Vec<Order>,
    pub(crate) field: String,
    pub(crate) limit: usize,
}
