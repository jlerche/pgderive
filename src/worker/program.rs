mod aggregate;
mod bindings;
mod distinct;
mod distinct_statistics;
mod group_cursor;
mod left_join;
mod lookup;
#[cfg(test)]
mod membership;
mod membership_counts;
mod native;
mod navigation;
mod order;
mod ordered;
mod partition;
mod prefix;
pub(super) mod relational;
mod statistics;
mod topk;
use crate::{compiler::Compiled, engine::plan::Plan, source::Contract};
use anyhow::Result;
pub(super) fn plan(contract: &Contract, compiled: &Compiled) -> Result<Plan> {
    compiled.validate_execution()?;
    relational::committed_plan(contract, compiled)
}

mod keys;
