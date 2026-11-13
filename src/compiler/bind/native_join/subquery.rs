//! Bound subquery marker inputs use the same typed scalar and tagged join infrastructure.
use super::{Builder, Column, Edge, keys, resolve};
use crate::compiler::{
    ColumnRef, Predicate,
    expression::Expr,
    parser::{Name, Parsed},
    relational::Node,
    scalar,
    subquery::Mark,
    syntax::Compare,
    typed,
};
use anyhow::{Result, ensure};
use pg_query::Node as Ast;
pub(super) struct MarkArguments<'a> {
    pub(super) probe: &'a Ast,
    pub(super) alias: String,
    pub(super) condition: Option<&'a Ast>,
    pub(super) semijoin: bool,
    pub(super) quantified: Option<Compare>,
    pub(super) residual_inequality: bool,
}

impl Builder<'_> {
    pub(super) fn mark(
        &mut self,
        mut left: Edge,
        parsed: Box<Parsed>,
        arguments: MarkArguments<'_>,
    ) -> Result<Edge> {
        let MarkArguments { probe, alias, condition, semijoin, quantified, residual_inequality } =
            arguments;
        let resolve_left = |name: &Name| Ok(resolve(name, &left.columns)?.value.clone());
        let start = self.nodes.len();
        let right = self.derived(parsed, alias.clone())?;
        ensure!(
            condition.is_some() || right.columns.len() == 1,
            "IN subquery requires exactly one output"
        );
        let value = right.columns[0].value.clone();
        ensure!(
            matches!(value.oid, 16 | 20 | 21 | 23 | 25 | 1043 | 1082 | 1700 | 2950 | 1114 | 1184),
            "unsupported IN native type"
        );
        if quantified.is_some() || residual_inequality {
            super::validate_scalar_input(&self.nodes[start..])?;
        }
        let probe = if quantified.is_some() {
            integral_probe(probe, value.oid, &resolve_left)?
        } else {
            typed::operand(probe, &resolve_left)?.coerce(value.oid)?
        };
        validate_probe(&probe, &left.columns, right.columns[0].collation, residual_inequality)?;
        let mut computed = Vec::new();
        let probe = match probe.as_column() {
            Some(column) => column.clone(),
            None => {
                scalar::register(probe, &mut computed, &format!("@mark_{}_probe_", self.joins))?
            }
        };
        self.map(&mut left, computed, &format!("mark_{}_probe", self.joins));
        let columns = left.columns.clone();
        let id = format!("tree_join_{}", self.joins);
        let bound = keys::condition(condition, &left.columns, &right.columns, &id)?;
        validate_correlation(&bound, quantified.is_some(), residual_inequality)?;
        let comparison = Predicate::new(Expr::Compare(
            quantified.unwrap_or(Compare::Eq),
            typed::Typed::column(probe.clone()),
            typed::Typed::column(value.clone()),
        ));
        let edge = self.join(left, right, bound, crate::compiler::left_join::Kind::Left)?;
        let Some(Node::LeftJoin { id, input, spec }) = self.nodes.pop() else {
            anyhow::bail!("missing marker tagged join");
        };
        let output = ColumnRef {
            name: format!("@{id}_membership"),
            right: false,
            oid: 16,
            nullable: true,
            collation: 0,
            modifier: None,
        };
        self.nodes.push(Node::Mark {
            id: id.clone(),
            input,
            spec: Box::new(Mark {
                probe,
                value,
                output: output.clone(),
                comparison,
                correlation: spec.predicate,
                semijoin,
            }),
        });
        let mut columns = columns;
        columns.push(Column {
            label: "\0value".into(),
            qualifiers: vec![alias],
            value: output,
            collation: 0,
            modifier: -1,
            unqualified: true,
        });
        ensure!(edge.id == id, "marker join edge differs");
        Ok(Edge { id, columns })
    }
}
fn integral_probe(
    probe: &Ast,
    rhs_oid: u32,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<typed::Typed> {
    let probe = typed::Typed::bind_contextual(probe, resolve, 0)?;
    ensure!(
        matches!(rhs_oid, 20 | 21 | 23) && matches!(probe.oid(), 0 | 705 | 20 | 21 | 23),
        "quantified comparisons require integral operands"
    );
    if matches!(probe.oid(), 0 | 705) { probe.coerce_common(rhs_oid) } else { Ok(probe) }
}
fn validate_probe(
    probe: &typed::Typed,
    left: &[Column],
    rhs_collation: u32,
    residual_inequality: bool,
) -> Result<()> {
    ensure!(
        !residual_inequality || probe.total_scalar_input(),
        "inequality correlation requires a total membership probe"
    );
    if matches!(probe.oid(), 25 | 1043) {
        let collation = probe.collation(&|value| {
            Ok(left
                .iter()
                .find(|column| column.value.name == value.name)
                .map_or(0, |column| column.collation))
        })?;
        ensure!(collation != 0 && rhs_collation != 0, "IN requires resolved text collations");
        typed::collation::common(collation, rhs_collation)?;
    }
    Ok(())
}
fn validate_correlation(
    bound: &keys::Bound,
    quantified: bool,
    residual_inequality: bool,
) -> Result<()> {
    ensure!(
        !quantified
            || if residual_inequality {
                bound.predicate.as_ref().is_some_and(Predicate::is_inequality_expression)
            } else {
                bound.predicate.is_none()
            },
        "quantified correlation requires indexable equality"
    );
    ensure!(
        !residual_inequality || !bound.keys.is_empty(),
        "inequality correlation requires an indexable equality key"
    );
    Ok(())
}
