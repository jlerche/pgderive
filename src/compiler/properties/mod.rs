//! Relational proofs shared by sink admission, positional windows and committed statistics.
mod joins;
mod model;
#[cfg(test)]
mod tests;
mod unary;
use super::{
    Compiled,
    partition::{Function, Mode},
    relational::{Node, Relational},
};
use crate::source::Contract;
use anyhow::{Context, Result, ensure};
pub use model::Properties;
use model::{Column, Identity};
use std::collections::{BTreeMap, BTreeSet};

pub struct Analysis {
    edges: BTreeMap<String, Properties>,
}
impl Analysis {
    pub(crate) fn infer(contract: &Contract, ir: &Relational) -> Result<Self> {
        contract.validate()?;
        let mut analysis = Self { edges: BTreeMap::new() };
        for node in &ir.nodes {
            let facts = analysis.transfer(contract, ir, node)?;
            ensure!(
                analysis.edges.insert(node.id().into(), facts).is_none(),
                "duplicate property edge"
            );
        }
        Ok(analysis)
    }
    pub(crate) fn edge(&self, id: &str) -> Result<&Properties> {
        self.edges.get(id).context("missing relational property input")
    }
    pub(crate) fn committed_statistics(&self, ir: &Relational) -> Result<BTreeSet<String>> {
        let mut result = BTreeSet::new();
        for node in &ir.nodes {
            if let Node::Statistics { id, input, spec } = node
                && matches!(spec.mode, Mode::Grouped { .. })
                && spec.aggregates.iter().all(|a| {
                    matches!(a.function, Function::Count | Function::Sum | Function::Average)
                })
                && self.edge(input)?.unique_rows()
            {
                result.insert(id.clone());
            }
        }
        Ok(result)
    }
    fn source(contract: &Contract, ir: &Relational, ordinal: usize) -> Result<Properties> {
        let source = ir.sources.get(ordinal).context("missing property source")?;
        let relation = contract
            .relations
            .iter()
            .find(|r| r.schema == source.schema && r.table == source.table)
            .context("property source differs from contract")?;
        Self::source_relation(relation, ordinal)
    }
    pub(in crate::compiler) fn source_relation(
        relation: &crate::source::Relation,
        ordinal: usize,
    ) -> Result<Properties> {
        let mut result = Properties::default();
        let mut primary = BTreeSet::new();
        for column in &relation.columns {
            let name = super::relational::field(ordinal, &column.name);
            let identity = Identity::Source(ordinal, column.position);
            if column.primary {
                primary.insert(identity.clone());
            }
            result.columns.insert(
                name.clone(),
                Column {
                    reference: super::ColumnRef {
                        right: false,
                        name,
                        oid: column.oid,
                        nullable: column.nullable,
                    },
                    modifier: column.modifier,
                    collation: column.collation,
                    identity,
                },
            );
        }
        ensure!(!primary.is_empty(), "property source requires complete primary key");
        result.add_key(primary);
        Ok(result)
    }
    pub(crate) fn prove_sink(&self, compiled: &Compiled, names: &[String]) -> Result<()> {
        if names.is_empty() {
            return Ok(());
        }
        let output = compiled.relational.nodes.last().context("missing property output")?;
        let facts = self.edge(output.id())?;
        let mut selected = Vec::new();
        for name in names {
            let field = compiled
                .output()
                .columns
                .iter()
                .find(|c| &c.label == name)
                .with_context(|| format!("sink key column {name:?} is not an output column"))?;
            let column =
                facts.columns.get(&field.column.name).context("sink key value is not proven")?;
            ensure!(!column.reference.nullable, "sink key column {name:?} may be NULL");
            ensure!(
                column.sink_compatible(),
                "sink key column {name:?} has unsupported equality semantics"
            );
            selected.push(field.column.name.clone());
        }
        ensure!(
            facts.covers(&selected),
            "sink key uniqueness is not proven by relational properties"
        );
        Ok(())
    }
}
/// Composition removes the current Output, so its predecessor retains hidden row identity.
pub(in crate::compiler) fn occurrence(ir: &Relational, contract: &Contract) -> Result<Properties> {
    let analysis = Analysis::infer(contract, ir)?;
    let node = ir.nodes.last().context("missing occurrence output")?;
    let input = match node {
        Node::Output { input, .. } => input.as_str(),
        _ => node.id(),
    };
    Ok(analysis.edge(input)?.clone())
}
