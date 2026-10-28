use super::{Analysis, Column, Identity, Properties};
use crate::{
    compiler::{
        ColumnRef,
        partition::{Function, Mode, Partition},
        relational::{Binding, Node, Relational},
        scalar::Computed,
    },
    source::Contract,
};
use anyhow::{Result, ensure};
use std::collections::BTreeSet;
impl Analysis {
    pub(super) fn transfer(
        &self,
        contract: &Contract,
        ir: &Relational,
        node: &Node,
    ) -> Result<Properties> {
        match node {
            Node::Source { source, .. } => Self::source(contract, ir, *source),
            Node::Join { id, left, right } => {
                super::joins::inner(self.edge(left)?, self.edge(right)?, id)
            }
            Node::Union { id, left, right } => {
                Ok(super::joins::union(self.edge(left)?, self.edge(right)?, id))
            }
            Node::LeftJoin { id, input, .. } => self.left_join(ir, id, input),
            Node::Existence { input, .. } => self.semi_join(ir, input),
            _ => self.unary(ir, node),
        }
    }
    fn unary(&self, ir: &Relational, node: &Node) -> Result<Properties> {
        Ok(match node {
            Node::Filter { input, predicate, .. } => {
                let mut facts = self.edge(input)?.clone();
                facts.non_null(predicate.non_null_columns());
                facts
            }
            Node::Map { id, input, computed } => mapped(self.edge(input)?.clone(), id, computed)?,
            Node::Rebind { id, input, fields } => rebind(self.edge(input)?, id, fields)?,
            Node::PartitionBy { input, keys, .. } | Node::EquijoinKey { input, keys, .. } => {
                let mut facts = self.edge(input)?.clone();
                facts.navigation = keys.iter().map(|c| c.name.clone()).collect();
                if matches!(node, Node::EquijoinKey { .. }) {
                    facts.non_null(facts.navigation.clone());
                }
                facts
            }
            Node::CrossKey { input, .. } => {
                let mut facts = self.edge(input)?.clone();
                facts.navigation.clear();
                facts
            }
            Node::Output { id, input } => {
                let mut facts =
                    project(self.edge(input)?, id, ir.output.columns.iter().map(|c| &c.column))?;
                facts.navigation.clear();
                facts
            }
            Node::Partition { .. }
            | Node::Finalize { .. }
            | Node::OrderedFinalize { .. }
            | Node::Statistics { .. }
            | Node::SetDistinct { .. } => self.grouping(ir, node)?,
            Node::Expand { id, input, series } => expanded(self.edge(input)?, id, series)?,
            Node::TopKInput { input, .. } | Node::PrefixInput { input, .. } => {
                self.edge(input)?.clone()
            }
            Node::TopK { id, input, spec } => {
                let mut facts = self.edge(input)?.clone();
                let field =
                    ColumnRef { name: spec.field.clone(), right: false, oid: 20, nullable: false };
                facts.columns.insert(field.name.clone(), Column::derived(id, field));
                facts
            }
            Node::PrefixWindow { id, left, spec, .. } => partition(self.edge(left)?, id, spec)?,
            Node::Source { .. }
            | Node::Join { .. }
            | Node::Union { .. }
            | Node::LeftJoin { .. }
            | Node::Existence { .. } => anyhow::bail!("non-unary property node"),
            Node::Lookup { .. }
            | Node::LeftJoinInput { .. }
            | Node::LookupInput { .. }
            | Node::PrefixStatistics { .. }
            | Node::RightCounts { .. }
            | Node::RightPresence { .. }
            | Node::DistinctInput { .. }
            | Node::DistinctCounts { .. }
            | Node::DistinctMembership { .. }
            | Node::OrderedInput { .. }
            | Node::OrderedFirst { .. }
            | Node::OrderedGroup { .. } => Properties::default(),
        })
    }
}
impl Analysis {
    fn grouping(&self, _ir: &Relational, node: &Node) -> Result<Properties> {
        Ok(match node {
            Node::Partition { id, input, spec }
            | Node::Finalize { id, input, spec }
            | Node::OrderedFinalize { id, input, spec } => partition(self.edge(input)?, id, spec)?,
            Node::Statistics { id, input, spec } => grouped(self.edge(input)?, id, spec, false)?,
            Node::SetDistinct { id, input, columns } => {
                let mut facts = project(self.edge(input)?, id, columns.iter())?;
                facts.add_key(facts.columns.values().map(|c| c.identity.clone()).collect());
                facts
            }
            _ => anyhow::bail!("non-grouping property node"),
        })
    }
}
fn expanded(
    input: &Properties,
    id: &str,
    series: &crate::compiler::expansion::Series,
) -> Result<Properties> {
    let mut facts = input.clone();
    let reference = ColumnRef {
        right: false,
        name: crate::compiler::expansion::FIELD.into(),
        oid: 23,
        nullable: false,
    };
    ensure!(!facts.columns.contains_key(&reference.name), "property expansion field collision");
    let column = Column::derived(id, reference);
    facts.keys = std::mem::take(&mut facts.keys)
        .into_iter()
        .map(|mut key| {
            key.insert(column.identity.clone());
            key
        })
        .collect();
    facts.columns.insert(column.reference.name.clone(), column);
    let count = u64::try_from((i64::from(series.end) - i64::from(series.start) + 1).max(0))?;
    facts.maximum =
        facts.maximum.and_then(|n| n.checked_mul(count)).or_else(|| (count == 0).then_some(0));
    Ok(facts)
}

fn project<'a>(
    input: &Properties,
    id: &str,
    fields: impl Iterator<Item = &'a ColumnRef>,
) -> Result<Properties> {
    let mut facts = input.clone();
    facts.columns.clear();
    for field in fields {
        let mut value = input
            .columns
            .get(&field.name)
            .cloned()
            .unwrap_or_else(|| Column::derived(id, field.clone()));
        ensure!(value.reference.oid == field.oid, "property projection type changed");
        value.reference.name.clone_from(&field.name);
        facts.columns.insert(field.name.clone(), value);
    }
    facts.prune();
    Ok(facts)
}
fn rebind(input: &Properties, id: &str, fields: &[Binding]) -> Result<Properties> {
    let mut facts = input.clone();
    facts.columns.clear();
    for field in fields {
        let mut value = input
            .columns
            .get(&field.input.name)
            .cloned()
            .unwrap_or_else(|| Column::derived(id, field.input.clone()));
        ensure!(value.reference.oid == field.output.oid, "property rebind changes type");
        value.reference = ColumnRef {
            nullable: value.reference.nullable || field.output.nullable,
            ..field.output.clone()
        };
        ensure!(
            facts.columns.insert(field.output.name.clone(), value).is_none(),
            "duplicate property rebind field"
        );
    }
    facts.navigation.clear();
    facts.prune();
    Ok(facts)
}
fn mapped(mut facts: Properties, id: &str, computed: &[Computed]) -> Result<Properties> {
    for item in computed {
        ensure!(
            !facts.columns.contains_key(&item.column.name),
            "property computed field overwrites input"
        );
        let mut field = item
            .expression
            .as_column()
            .and_then(|c| facts.columns.get(&c.name))
            .filter(|c| c.reference.oid == item.column.oid)
            .cloned()
            .unwrap_or_else(|| Column::derived(id, item.column.clone()));
        field.reference.name.clone_from(&item.column.name);
        facts.columns.insert(item.column.name.clone(), field);
    }
    Ok(facts)
}
fn grouped(input: &Properties, id: &str, spec: &Partition, finalized: bool) -> Result<Properties> {
    let Mode::Grouped { keys } = &spec.mode else { return Ok(Properties::default()) };
    let mut facts = Properties::default();
    let mut key = BTreeSet::new();
    for column in keys {
        let mut field = input
            .columns
            .get(&column.name)
            .cloned()
            .unwrap_or_else(|| Column::derived(id, column.clone()));
        field.identity = Identity::Derived(id.into(), column.name.clone());
        key.insert(field.identity.clone());
        facts.columns.insert(column.name.clone(), field);
    }
    facts.add_key(key);
    facts.navigation = keys.iter().map(|c| c.name.clone()).collect();
    if keys.is_empty() {
        facts.maximum = Some(1);
    }
    if finalized {
        append_aggregates(&mut facts, id, spec)?;
    }
    Ok(facts)
}
fn partition(input: &Properties, id: &str, spec: &Partition) -> Result<Properties> {
    if matches!(spec.mode, Mode::Grouped { .. }) {
        return grouped(input, id, spec, true);
    }
    let mut facts = input.clone();
    append_aggregates(&mut facts, id, spec)?;
    Ok(facts)
}
fn append_aggregates(facts: &mut Properties, id: &str, spec: &Partition) -> Result<()> {
    for aggregate in &spec.aggregates {
        let oid = aggregate.navigation.as_ref().map_or_else(
            || {
                crate::compiler::partition::result_type(
                    &aggregate.function,
                    aggregate.argument.as_ref(),
                )
            },
            |navigation| Ok(navigation.oid),
        )?;
        let nullable = !matches!(
            aggregate.function,
            Function::Count
                | Function::CountDistinct
                | Function::Rank
                | Function::DenseRank
                | Function::RowNumber
        );
        let field = ColumnRef { name: aggregate.field.clone(), right: false, oid, nullable };
        facts.columns.insert(field.name.clone(), Column::derived(id, field));
    }
    facts.prune();
    Ok(())
}
