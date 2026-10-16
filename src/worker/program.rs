mod bindings;
mod distinct;
mod distinct_statistics;
mod left_join;
mod lookup;
mod membership;
mod navigation;
mod ordered;
mod partition;
pub(super) mod relational;
mod statistics;
mod topk;
use crate::{compiler::Compiled, engine::plan::Plan, source::Contract};
use anyhow::Result;
pub(super) fn plan(contract: &Contract, compiled: &Compiled) -> Result<Plan> {
    compiled.validate_execution()?;
    relational::plan(contract, compiled, compiled.relational())
}
