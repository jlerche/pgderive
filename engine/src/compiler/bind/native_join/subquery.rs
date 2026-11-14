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
use anyhow::{Context, Result, ensure};
use pg_query::{Node as Ast, NodeEnum};
pub(super) struct MarkArguments<'a> {
    pub(super) probe: &'a Ast,
    pub(super) alias: String,
    pub(super) condition: Option<&'a Ast>,
    pub(super) semijoin: bool,
    pub(super) quantified: Option<Compare>,
    pub(super) has_residual: bool,
}

impl Builder<'_> {
    pub(super) fn mark(
        &mut self,
        mut left: Edge,
        parsed: Box<Parsed>,
        arguments: MarkArguments<'_>,
    ) -> Result<Edge> {
        let MarkArguments { probe, alias, condition, semijoin, quantified, has_residual } =
            arguments;
        let start = self.nodes.len();
        let right = self.derived(parsed, alias.clone())?;
        if quantified.is_some() || has_residual {
            super::validate_scalar_input(&self.nodes[start..])?;
        }
        let mut computed = Vec::new();
        let (probe, value, comparison) = bind_comparison(
            probe,
            &right,
            &left.columns,
            (quantified.as_ref(), has_residual),
            (&mut computed, self.joins),
        )?;
        self.map(&mut left, computed, &format!("mark_{}_probe", self.joins));
        let columns = left.columns.clone();
        let id = format!("tree_join_{}", self.joins);
        let bound = keys::condition(condition, &left.columns, &right.columns, &id)?;
        validate_correlation(&bound, quantified.is_some(), has_residual)?;
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
fn bind_comparison(
    ast: &Ast,
    right: &Edge,
    left: &[Column],
    (quantified, has_residual): (Option<&Compare>, bool),
    (computed, join): (&mut Vec<scalar::Computed>, usize),
) -> Result<(ColumnRef, ColumnRef, Predicate)> {
    let row = matches!(ast.node, Some(NodeEnum::RowExpr(_)));
    let items = match &ast.node {
        Some(NodeEnum::RowExpr(row)) => row.args.as_slice(),
        _ => std::slice::from_ref(ast),
    };
    let width = items.len();
    ensure!((1..=8).contains(&width), "membership row requires 1 to 8 fields");
    let output_width =
        right.columns.iter().take_while(|column| !column.label.starts_with('\0')).count();
    ensure!(output_width == width, "membership subquery output width differs from probe");
    let mut columns = Vec::with_capacity(width);
    let resolve_left = |name: &Name| Ok(resolve(name, left)?.value.clone());
    for (item, field) in items.iter().zip(&right.columns) {
        let value = membership_value(field)?;
        let probe = if quantified.is_some() {
            quantified_probe(item, value.oid, &resolve_left)?
        } else {
            typed::operand(item, &resolve_left)?.coerce(value.oid)?
        };
        validate_probe(&probe, left, field.collation, (has_residual, quantified.is_some()))?;
        let probe = if let Some(column) = probe.as_column() {
            column.clone()
        } else {
            scalar::register(probe, computed, &format!("@mark_{join}_probe_"))?
        };
        columns.push((probe, value));
    }
    let (probe, value) = columns.first().context("missing membership columns")?.clone();
    let op = quantified.cloned().unwrap_or(Compare::Eq);
    let predicate = if row {
        row_comparison(&op, columns)?
    } else {
        comparison(op, probe.clone(), value.clone())
    };
    Ok((probe, value, Predicate::new(predicate)))
}

const fn comparison(op: Compare, left: ColumnRef, right: ColumnRef) -> Expr {
    Expr::Compare(op, typed::Typed::column(left), typed::Typed::column(right))
}

fn row_comparison(op: &Compare, mut columns: Vec<(ColumnRef, ColumnRef)>) -> Result<Expr> {
    if matches!(op, Compare::Eq | Compare::Ne) {
        let equal =
            Expr::And(columns.into_iter().map(|(l, r)| comparison(Compare::Eq, l, r)).collect());
        return Ok(if *op == Compare::Eq { equal } else { Expr::Not(Box::new(equal)) });
    }
    let (left, right) = columns.pop().context("missing row comparison fields")?;
    let last = comparison(op.clone(), left, right);
    let strict = if matches!(op, Compare::Lt | Compare::Le) { Compare::Lt } else { Compare::Gt };
    Ok(columns.into_iter().rev().fold(last, |rest, (left, right)| {
        let equal = comparison(Compare::Eq, left.clone(), right.clone());
        Expr::Or(vec![comparison(strict.clone(), left, right), Expr::And(vec![equal, rest])])
    }))
}

fn membership_value(field: &Column) -> Result<ColumnRef> {
    let value = field.value.clone();
    ensure!(
        matches!(
            value.oid,
            16 | 20 | 21 | 23 | 25 | 1043 | 1082 | 1700 | 2950 | 1114 | 1184 | 1186
        ),
        "unsupported IN native type"
    );
    Ok(value)
}
fn quantified_probe(
    probe: &Ast,
    rhs_oid: u32,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<typed::Typed> {
    let probe = typed::Typed::bind_contextual(probe, resolve, 0)?;
    let probe_oid = probe.oid();
    let unknown = matches!(probe_oid, 0 | 705);
    ensure!(
        (matches!(rhs_oid, 20 | 21 | 23 | 1700)
            && (unknown || matches!(probe_oid, 20 | 21 | 23 | 1700)))
            || (matches!(rhs_oid, 25 | 1043) && (unknown || matches!(probe_oid, 25 | 1043)))
            || (matches!(rhs_oid, 16 | 1186 | 2950) && (unknown || probe_oid == rhs_oid))
            || (matches!(rhs_oid, 1082 | 1114 | 1184)
                && (unknown || matches!(probe_oid, 1082 | 1114 | 1184))),
        "quantified comparisons require compatible exact numeric, text, Boolean, UUID, date/time, or interval operands"
    );
    if matches!(probe.oid(), 0 | 705) { probe.coerce_common(rhs_oid) } else { Ok(probe) }
}
fn validate_probe(
    probe: &typed::Typed,
    left: &[Column],
    rhs_collation: u32,
    (has_residual, quantified): (bool, bool),
) -> Result<()> {
    ensure!(
        !(has_residual || quantified) || probe.total_scalar_input(),
        "inequality correlation or quantified comparison requires a total membership probe"
    );
    if matches!(probe.oid(), 25 | 1043) {
        let resolve = |value: &ColumnRef| {
            Ok(left
                .iter()
                .find(|column| column.value.name == value.name)
                .map_or(0, |column| column.collation))
        };
        if quantified {
            use typed::collation::Derivation;
            let collation = probe
                .derive_collation(&resolve)?
                .merge(if rhs_collation == 0 {
                    Derivation::Indeterminate
                } else {
                    Derivation::Implicit(rhs_collation)
                })?
                .oid()?;
            ensure!(
                matches!(collation, 950 | 951),
                "quantified text comparison requires deterministic C/POSIX collation"
            );
            return Ok(());
        }
        let collation = probe.collation(&resolve)?;
        ensure!(collation != 0 && rhs_collation != 0, "IN requires resolved text collations");
        typed::collation::common(collation, rhs_collation)?;
    }
    Ok(())
}
fn validate_correlation(bound: &keys::Bound, quantified: bool, has_residual: bool) -> Result<()> {
    if has_residual {
        ensure!(
            bound.predicate.as_ref().is_some_and(Predicate::is_total_comparison_expression),
            "correlation residual requires total comparison operands"
        );
    } else if quantified {
        ensure!(bound.predicate.is_none(), "quantified correlation requires indexable equality");
    }
    Ok(())
}
