//! Apply the `PostgreSQL` planner rewrites that decide which rows an expression sees.
mod dummy;
mod graph;
mod implied;
mod lift;
mod not_null;
mod prune;
mod push;
mod reduce;
use super::relational::Relational;
use crate::source::Contract;
use anyhow::Result;
use graph::Graph;

const PASSES: usize = 256;

/// Inlining nested computations can grow them exponentially; past this, they stay put.
fn bounded(value: &impl serde::Serialize) -> bool {
    serde_json::to_vec(value).is_ok_and(|bytes| bytes.len() <= EXPRESSION_BYTES)
}
const EXPRESSION_BYTES: usize = 16_384;

pub(super) fn rewrite(ir: &mut Relational, contract: &Contract) -> Result<()> {
    let mut graph = Graph::new(ir, contract);
    graph.ensure_unique()?;
    graph.single_globals = prune::single_relation_globals(&graph)?;
    graph.flat_arms = prune::flat_arms(&graph)?;
    for _ in 0..PASSES {
        let emptied = dummy::run(&mut graph, true)?;
        let reduced = reduce::run(&mut graph)?;
        graph.sort()?;
        let pushed = push::run(&mut graph)?;
        let implied = implied::run(&mut graph)?;
        let lifted = lift::run(&mut graph)?;
        let settled = !(emptied || reduced || pushed || implied || lifted);
        let pruned = prune::run(&mut graph, settled)?;
        if !(emptied || reduced || pushed || implied || lifted || pruned) {
            // A filter that can no longer move will never have its computed columns substituted.
            if dummy::run(&mut graph, false)? {
                continue;
            }
            dummy::fold_all(&mut graph)?;
            dummy::discard_zero_limits(&mut graph)?;
            prune::remove_sorted(&mut graph)?;
            return graph.sort();
        }
    }
    anyhow::bail!("plan rewrite did not converge")
}
