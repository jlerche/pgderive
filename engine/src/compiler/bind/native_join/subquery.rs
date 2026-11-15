//! Bound subquery marker inputs use the same typed scalar and tagged join infrastructure.
use super::{Builder, Column, Edge, keys, resolve};
use crate::compiler::{
    ColumnRef, Predicate,
    expression::Expr,
    parser::{
        Name, Parsed,
        native_join::{MembershipCutoff, MembershipDistinct},
    },
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
    pub(super) skip: Option<std::num::NonZeroUsize>,
    pub(super) distinct: Vec<String>,
    pub(super) cutoff: Option<MembershipCutoff>,
}

impl Builder<'_> {
    pub(super) fn mark(
        &mut self,
        mut left: Edge,
        parsed: Box<Parsed>,
        arguments: MarkArguments<'_>,
    ) -> Result<Edge> {
        let MarkArguments {
            probe,
            alias,
            condition,
            semijoin,
            quantified,
            has_residual,
            skip,
            distinct,
            cutoff,
        } = arguments;
        let ordered = cutoff.is_some()
            || matches!(&*parsed, Parsed::Ordered(value) if !value.correlation_keys.is_empty());
        let start = self.nodes.len();
        let mut right = self.derived(parsed, alias.clone())?;
        if quantified.is_some() || has_residual || ordered {
            super::validate_scalar_input(&self.nodes[start..])?;
        }
        let mut distinct = distinct_columns(&right, distinct)?;
        let mut computed = Vec::new();
        let (probe, value, comparison) = bind_comparison(
            probe,
            &right,
            &left.columns,
            (quantified.as_ref(), has_residual || ordered),
            (&mut computed, self.joins),
        )?;
        self.map(&mut left, computed, &format!("mark_{}_probe", self.joins));
        let columns = left.columns.clone();
        let id = format!("tree_join_{}", self.joins);
        let bound = keys::condition(condition, &left.columns, &right.columns, &id)?;
        validate_correlation(&bound, quantified.is_some() || ordered, has_residual)?;
        let cutoff = if let Some(cutoff) = cutoff {
            let (cutoff, keys) = self.mark_cutoff(&mut right, cutoff, &bound)?;
            distinct = keys;
            Some(cutoff)
        } else {
            None
        };
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
                skip,
                distinct,
                cutoff,
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
impl Builder<'_> {
    fn mark_cutoff(
        &mut self,
        right: &mut Edge,
        cutoff: MembershipCutoff,
        correlation: &keys::Bound,
    ) -> Result<(crate::compiler::topk::Spec, Vec<ColumnRef>)> {
        let order = cutoff
            .order
            .iter()
            .map(|(name, descending, nulls_first)| {
                let column = resolve(&Name(vec![name.clone()]), &right.columns)?.value.clone();
                ensure!(
                    crate::compiler::partition::orderable(column.oid),
                    "unsupported ORDER BY native type"
                );
                if matches!(column.oid, 25 | 1043) {
                    crate::compiler::partition::require_bytewise(column.collation)?;
                }
                Ok(crate::compiler::partition::Order {
                    column,
                    descending: *descending,
                    nulls_first: *nulls_first,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let distinct = self.validate_mark_order(right, &order, correlation, &cutoff)?;
        let bounds = cutoff.bounds;
        let spec = crate::compiler::topk::Spec {
            order,
            field: None,
            limit: bounds.limit.map_or(usize::MAX, |limit| limit + bounds.offset),
            offset: bounds.offset,
            with_ties: bounds.with_ties,
            identity: Vec::new(),
        };
        let id = format!("tree_join_{}_membership_order", self.joins);
        let input = std::mem::replace(&mut right.id, id.clone());
        self.nodes.push(Node::TopKInput { id, input, spec: spec.clone() });
        Ok((spec, distinct))
    }
    fn validate_mark_order(
        &self,
        right: &Edge,
        order: &[crate::compiler::partition::Order],
        correlation: &keys::Bound,
        cutoff: &MembershipCutoff,
    ) -> Result<Vec<ColumnRef>> {
        use crate::compiler::{properties::Analysis, relational::Relational};
        let temporary = Relational {
            sources: self.sources.clone(),
            nodes: self.nodes.clone(),
            projection: Vec::new(),
            output: crate::compiler::Projected { columns: Vec::new() },
        };
        let analysis = Analysis::infer(self.contract, &temporary)?;
        let facts = analysis.edge(&right.id)?;
        let payload = right
            .columns
            .iter()
            .take_while(|value| !value.label.starts_with('\0'))
            .map(|value| value.value.clone())
            .collect::<Vec<_>>();
        let order_names = order.iter().map(|value| value.column.name.clone()).collect::<Vec<_>>();
        let keys = match &cutoff.distinct {
            MembershipDistinct::None => Vec::new(),
            MembershipDistinct::Values => {
                let names = payload.iter().map(|column| column.name.clone()).collect::<Vec<_>>();
                ensure!(
                    order_names.iter().all(|field| facts.same_identity(field, &names)),
                    "SELECT DISTINCT ORDER BY expressions must appear in the output"
                );
                payload.clone()
            }
            MembershipDistinct::On(labels) => {
                let keys = distinct_columns(right, labels.clone())?;
                check_distinct_prefix(facts, &order_names, &keys)?;
                keys
            }
        };
        let mut peers = order_names;
        peers.extend(correlation.keys.iter().map(|(_, rhs)| rhs.name.clone()));
        let bounds = &cutoff.bounds;
        if bounds.limit != Some(0)
            && (bounds.offset != 0 || bounds.limit.is_some() && !bounds.with_ties)
        {
            prove_selected_values(facts, &peers, &payload)?;
        }
        if matches!(cutoff.distinct, MembershipDistinct::On(_)) && bounds.limit != Some(0) {
            peers.extend(keys.iter().map(|column| column.name.clone()));
            prove_selected_values(facts, &peers, &payload)?;
        }
        Ok(keys)
    }
}
fn prove_selected_values(
    facts: &crate::compiler::properties::Properties,
    peers: &[String],
    payload: &[ColumnRef],
) -> Result<()> {
    ensure!(
        facts.proves_key(peers)
            || payload.iter().all(|value| facts.same_identity(&value.name, peers)),
        "correlated row cutoff requires ORDER BY that determines the selected values"
    );
    Ok(())
}

fn check_distinct_prefix(
    facts: &crate::compiler::properties::Properties,
    order: &[String],
    keys: &[ColumnRef],
) -> Result<()> {
    let mut remaining = keys.iter().map(|column| column.name.clone()).collect::<Vec<_>>();
    let names = remaining.clone();
    for field in order {
        if remaining.is_empty() {
            break;
        }
        ensure!(
            facts.same_identity(field, &names),
            "DISTINCT ON expressions must match initial ORDER BY expressions"
        );
        remaining.retain(|key| !facts.same_identity(key, std::slice::from_ref(field)));
    }
    Ok(())
}
fn distinct_columns(right: &Edge, labels: Vec<String>) -> Result<Vec<ColumnRef>> {
    labels
        .into_iter()
        .map(|label| {
            let column = resolve(&Name(vec![label]), &right.columns)?.value.clone();
            ensure!(
                crate::compiler::partition::orderable(column.oid),
                "EXISTS OFFSET requires native scalar equality for its grouping keys"
            );
            Ok(column)
        })
        .collect()
}
fn bind_comparison(
    ast: &Ast,
    right: &Edge,
    left: &[Column],
    (quantified, requires_total_probe): (Option<&Compare>, bool),
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
        validate_probe(
            &probe,
            left,
            field.collation,
            (requires_total_probe, quantified.is_some()),
        )?;
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
    (requires_total_probe, quantified): (bool, bool),
) -> Result<()> {
    ensure!(
        !(requires_total_probe || quantified) || probe.total_scalar_input(),
        "this correlated or quantified comparison requires a total membership probe"
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
                crate::compiler::typed::collation::bytewise(collation),
                "quantified text comparison requires a built-in bytewise collation"
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
