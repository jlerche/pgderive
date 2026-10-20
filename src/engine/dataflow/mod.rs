//! Explicit acyclic, transaction-ordered edges and an object-backed join/count graph.
//!
//! Preparation uploads immutable objects; only local commit changes graph visibility.
//! This is not a `PostgreSQL` durable publication or a recursive DBSP scheduler.
mod arrangement;
mod compaction;
pub use compaction::{CompactionJob, CompactionResult};
mod expand;
mod group;
mod join;
pub use group::{Group, GroupCursor, GroupFuture, GroupInput};
mod partition;
mod project;
mod runtime;
mod statistics;
mod sum;
mod union;
pub use arrangement::Arrangement;
pub use expand::Expand;
pub use join::Join;
pub use partition::{Partition, PartitionWork};
pub use project::Project;
pub use runtime::{Graph, PreparedGraph, PreparedMaintenance, Unregistered};
pub use statistics::Statistics;
pub use sum::{GroupSum, SumDelta, SumState};
pub use union::Union;

/// One complete logical delta batch at a tick. Empty batches still carry time.
#[derive(Debug, Clone)]
pub struct TimedBatch<B> {
    /// Logical delta-tick index, distinct from source LSNs.
    pub time: u64,
    /// Finalized batch for this tick.
    pub batch: B,
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "tests/operators.rs"]
mod operator_tests;

#[cfg(test)]
#[path = "tests/sum.rs"]
mod sum_tests;

#[cfg(test)]
#[path = "tests/budgets.rs"]
mod budget_tests;

#[cfg(test)]
#[path = "tests/join_probes.rs"]
mod join_probe_tests;

#[cfg(test)]
#[path = "tests/union.rs"]
mod union_tests;

#[cfg(test)]
#[path = "tests/partition.rs"]
mod partition_tests;

mod ordered_selection;
pub use ordered_selection::OrderedSelection;
#[cfg(test)]
#[path = "tests/topk.rs"]
mod topk_tests;

#[cfg(test)]
#[path = "tests/ordered.rs"]
mod ordered_tests;

#[cfg(test)]
#[path = "tests/count.rs"]
mod count_tests;

#[cfg(test)]
#[path = "tests/join.rs"]
mod join_tests;

#[cfg(test)]
#[path = "tests/recorded.rs"]
mod recorded_tests;

#[cfg(test)]
#[path = "tests/boundary.rs"]
mod boundary_tests;
#[cfg(test)]
#[path = "tests/linear.rs"]
mod linear_tests;

#[cfg(test)]
#[path = "tests/locality.rs"]
mod locality_tests;

#[cfg(test)]
#[path = "tests/group.rs"]
mod group_tests;

#[cfg(test)]
#[path = "tests/run_locality.rs"]
mod run_locality_tests;
