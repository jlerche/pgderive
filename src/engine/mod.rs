//! Transaction-ordered weighted algebra, immutable storage and trace operators.
//!
//! Local visibility is not durable catalog publication; no recursive scheduling.
mod batch;
mod circuit;
mod count;
pub mod dataflow;
mod join;
mod linear;
pub mod reader;
pub mod trace;
mod weights;
mod zset;

pub use batch::{Batch, BatchBuilder, IndexedZSet};
pub use circuit::{Circuit, CircuitStep};
pub use count::GroupedCount;
pub use join::{IncrementalJoin, Step};
pub use zset::ZSet;
