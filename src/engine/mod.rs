//! Transaction-ordered weighted algebra, immutable storage and trace operators.
//!
//! Local visibility is not durable catalog publication; no recursive scheduling.
mod batch;
pub mod dataflow;
pub mod execution;
pub mod plan;
pub mod reader;
pub mod trace;
mod weights;
mod zset;

pub use batch::{Batch, BatchBuilder};
pub use zset::ZSet;

#[cfg(test)]
pub(crate) mod tests;
