//! Resolve correlated scopes and lower a native left predecessor circuit.
use super::{Compiled, Name, Scope, qualifier, relational::qualify, resolve, scope};
use crate::{
    compiler::{
        ColumnRef, Program,
        lookup::{Input, Lookup},
        parser::lookup::Parsed,
        partition::Order,
        relational::{Node, Relational, Source, field},
        syntax::{Compare, Expr, Scalar},
    },
    source::Contract,
};
use anyhow::{Context, Result, ensure};
struct Conditions {
    keys: Vec<(ColumnRef, ColumnRef)>,
    bound: Option<(ColumnRef, ColumnRef, bool)>,
}
pub(super) fn bind(parsed: Parsed, contract: &Contract) -> Result<Compiled> {
    let scopes = [scope(parsed.left, contract)?, scope(parsed.right, contract)?];
    ensure!(scopes[0].relation.oid != scopes[1].relation.oid, "lookup self joins unsupported");
    ensure!(qualifier(&scopes[0]) != qualifier(&scopes[1]), "duplicate correlated qualifier");
    ensure!(qualifier(&scopes[0]) != parsed.alias, "duplicate outer lookup qualifier");
    let correlated = |name: &Name| correlated(name, &scopes);
    let mut conditions = Conditions { keys: Vec::new(), bound: None };
    condition(parsed.condition, &correlated, &mut conditions)?;
    ensure!((1..=8).contains(&conditions.keys.len()), "lookup requires 1..=8 equality keys");
    conditions.keys.sort();
    conditions.keys.dedup();
    for (left, right) in &conditions.keys {
        compatible_keys(left, right, &scopes)?;
    }
    let (left_bound, right_bound, inclusive) =
        conditions.bound.context("missing predecessor bound")?;
    ensure!(
        left_bound.oid == right_bound.oid && ordered(left_bound.oid),
        "incompatible lookup bound types"
    );
    let order = order(parsed.order, &correlated, &right_bound, &scopes[1])?;
    let labels = labels(parsed.inner_columns, &correlated)?;
    let outer = |name: &Name| outer(name, &parsed.alias, &labels, &scopes[0]);
    let (columns, terminal) = super::relational::output(parsed.columns, &outer)?;
    let predicate = parsed
        .predicate
        .map(|value| {
            crate::compiler::expression::bind(value, &outer).map(crate::compiler::Predicate::new)
        })
        .transpose()?;
    let left_keys =
        conditions.keys.iter().map(|(left, _)| qualify(left.clone())).collect::<Vec<_>>();
    let right_keys = conditions.keys.into_iter().map(|(_, right)| qualify(right)).collect();
    let spec = Lookup {
        left_keys: left_keys.clone(),
        left_bound: qualify(left_bound),
        right_bound: qualify(right_bound),
        inclusive,
        order,
        right_fields: scopes[1]
            .relation
            .columns
            .iter()
            .map(|column| field(1, &column.name))
            .collect(),
    };
    let nodes = nodes((left_keys, right_keys), spec, predicate);
    let sources = scopes
        .iter()
        .map(|scope| Source {
            schema: scope.relation.schema.clone(),
            table: scope.relation.table.clone(),
        })
        .collect();
    let output = crate::compiler::Projected {
        schema: scopes[0].relation.schema.clone(),
        table: scopes[0].relation.table.clone(),
        columns,
        terminal,
    };
    Ok(Compiled { program: Program::Relational { relational: Relational { sources, nodes, output } }, predicates: None,
        revision: Some("sql-predecessor-v1:pg-query-6.2.1:pg-17.7:native-bag-v1:row-text-v1:json-exact-number-v1:signed-union-v1:affected-partition-v1:sql-limit-one-v1:where-3vl-v1:terminal-builtins-v1".into()) })
}
fn correlated(name: &Name, scopes: &[Scope<'_>; 2]) -> Result<ColumnRef> {
    // The inner local relation shadows an unqualified outer column.
    if let Ok(mut column) = resolve(name, &scopes[1..]) {
        column.right = true;
        return Ok(column);
    }
    resolve(name, &scopes[..1])
}
fn condition(
    value: Expr,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    state: &mut Conditions,
) -> Result<()> {
    match value {
        Expr::And(values) => {
            for value in values {
                condition(value, resolve, state)?;
            }
        }
        Expr::Compare(op, Scalar::Column(left), Scalar::Column(right)) => {
            let left = resolve(&left)?;
            let right = resolve(&right)?;
            ensure!(left.right != right.right, "lookup condition must correlate both sources");
            if matches!(op, Compare::Eq) {
                state.keys.push(if left.right { (right, left) } else { (left, right) });
            } else {
                let inclusive = matches!(op, Compare::Le | Compare::Ge);
                ensure!(
                    (left.right && matches!(op, Compare::Lt | Compare::Le))
                        || (!left.right && matches!(op, Compare::Gt | Compare::Ge)),
                    "lookup requires right bound <=/< left bound"
                );
                ensure!(state.bound.is_none(), "lookup requires exactly one bound");
                state.bound = Some(if left.right {
                    (right, left, inclusive)
                } else {
                    (left, right, inclusive)
                });
            }
        }
        _ => anyhow::bail!("lookup WHERE requires equality keys AND one predecessor bound"),
    }
    Ok(())
}
fn compatible_keys(left: &ColumnRef, right: &ColumnRef, scopes: &[Scope<'_>; 2]) -> Result<()> {
    let native = |side: usize, name: &str| {
        scopes[side]
            .relation
            .columns
            .iter()
            .find(|column| column.name == name)
            .context("missing lookup key")
    };
    let lhs = native(0, &left.name)?;
    let rhs = native(1, &right.name)?;
    ensure!(
        matches!(lhs.oid, 16 | 20 | 21 | 23 | 2950 | 1114 | 1184)
            && lhs.oid == rhs.oid
            && lhs.modifier == rhs.modifier
            && lhs.collation == rhs.collation,
        "unsupported or incompatible lookup key types"
    );
    Ok(())
}
const fn ordered(oid: u32) -> bool {
    matches!(oid, 20 | 21 | 23 | 1114 | 1184)
}
fn order(
    values: Vec<(Name, bool, bool)>,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    bound: &ColumnRef,
    right: &Scope<'_>,
) -> Result<Vec<Order>> {
    let values = values
        .into_iter()
        .map(|(name, descending, nulls_first)| {
            let column = resolve(&name)?;
            ensure!(
                column.right && ordered(column.oid),
                "lookup ordering requires native right integral/temporal columns"
            );
            Ok(Order { column: qualify(column), descending, nulls_first })
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        values
            .first()
            .is_some_and(|value| value.column == qualify(bound.clone()) && value.descending),
        "lookup ordering must start with right bound DESC"
    );
    ensure!(
        right
            .relation
            .columns
            .iter()
            .filter(|column| column.primary)
            .all(|column| values.iter().any(|value| value.column.name == field(1, &column.name))),
        "lookup ordering must include complete right primary key"
    );
    Ok(values)
}
type Labels = Vec<(String, ColumnRef)>;
fn labels(
    columns: crate::compiler::parser::Columns,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<Labels> {
    columns
        .into_iter()
        .map(|(name, label, transform)| {
            ensure!(
                transform == crate::catalog::terminal::Transform::Identity,
                "lookup inner expressions require native column projection"
            );
            let mut column = resolve(&name)?;
            ensure!(column.right, "lookup inner projection requires right columns");
            column = qualify(column);
            column.nullable = true;
            Ok((label, column))
        })
        .collect()
}
fn outer(name: &Name, alias: &str, labels: &Labels, left: &Scope<'_>) -> Result<ColumnRef> {
    let mut candidates = Vec::new();
    if let Ok(column) = resolve(name, std::slice::from_ref(left)) {
        candidates.push(qualify(column));
    }
    let label = match name.0.as_slice() {
        [label] => Some(label),
        [qualifier, label] if qualifier == alias => Some(label),
        _ => None,
    };
    if let Some(label) = label {
        candidates.extend(
            labels.iter().filter(|(name, _)| name == label).map(|(_, column)| column.clone()),
        );
    }
    ensure!(candidates.len() <= 1, "ambiguous outer lookup column");
    candidates.pop().context("unknown outer lookup column")
}
fn nodes(
    keys: (Vec<ColumnRef>, Vec<ColumnRef>),
    spec: Lookup,
    predicate: Option<crate::compiler::Predicate>,
) -> Vec<Node> {
    let mut nodes = vec![
        Node::Source { id: "left_source".into(), source: 0 },
        Node::Source { id: "right_source".into(), source: 1 },
        Node::LookupInput {
            id: "left".into(),
            input: "left_source".into(),
            spec: Input { side: 0, keys: keys.0 },
        },
        Node::LookupInput {
            id: "right".into(),
            input: "right_source".into(),
            spec: Input { side: 1, keys: keys.1 },
        },
        Node::Union { id: "lookup_input".into(), left: "left".into(), right: "right".into() },
        Node::Lookup { id: "lookup".into(), input: "lookup_input".into(), spec },
    ];
    let input = predicate.map_or("lookup", |predicate| {
        nodes.push(Node::Filter { id: "qualified".into(), input: "lookup".into(), predicate });
        "qualified"
    });
    nodes.push(Node::Output { id: "project".into(), input: input.into() });
    nodes
}
