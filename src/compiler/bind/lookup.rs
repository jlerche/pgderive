//! Resolve correlated scopes and lower a native left predecessor circuit.
use super::{Compiled, Name, Scope, qualifier, relational::qualify, resolve, scope};
use crate::{
    compiler::{
        ColumnRef,
        expression::Expr,
        lookup::{Input, Lookup},
        parser::lookup::Parsed,
        partition::Order,
        relational::{Node, Relational, Source, field},
        syntax::Compare,
    },
    source::Contract,
};
use anyhow::{Context, Result, ensure};
struct Conditions {
    keys: Vec<(ColumnRef, ColumnRef)>,
    bound: Option<(ColumnRef, ColumnRef, bool)>,
    computed: Vec<crate::compiler::scalar::Computed>,
}
pub(super) fn bind(parsed: Parsed, contract: &Contract) -> Result<Compiled> {
    let scopes = [scope(parsed.left, contract)?, scope(parsed.right, contract)?];
    ensure!(scopes[0].relation.oid != scopes[1].relation.oid, "lookup self joins unsupported");
    ensure!(qualifier(&scopes[0]) != qualifier(&scopes[1]), "duplicate correlated qualifier");
    ensure!(qualifier(&scopes[0]) != parsed.alias, "duplicate outer lookup qualifier");
    let correlated = |name: &Name| correlated(name, &scopes);
    let mut conditions = Conditions { keys: Vec::new(), bound: None, computed: Vec::new() };
    condition(crate::compiler::expression::bind(&parsed.condition, &correlated)?, &mut conditions)?;
    ensure!((1..=8).contains(&conditions.keys.len()), "lookup requires 1..=8 equality keys");
    conditions.keys.sort();
    conditions.keys.dedup();
    conditions.computed.sort_unstable_by(|left, right| left.column.cmp(&right.column));
    for (left, right) in &conditions.keys {
        compatible_keys(left, right, &scopes, &conditions.computed)?;
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
    let columns = super::relational::output(parsed.columns, &outer)?;
    let predicate = parsed
        .predicate
        .map(|value| {
            crate::compiler::expression::bind(&value, &outer).map(crate::compiler::Predicate::new)
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
    let nodes = nodes((left_keys, right_keys), spec, predicate, conditions.computed);
    let sources = scopes
        .iter()
        .map(|scope| Source {
            schema: scope.relation.schema.clone(),
            table: scope.relation.table.clone(),
        })
        .collect();
    let output = crate::compiler::Projected { columns };
    Ok(Compiled { result_columns: Vec::new(), relational: Relational { sources, nodes, output } })
}
fn correlated(name: &Name, scopes: &[Scope<'_>; 2]) -> Result<ColumnRef> {
    // The inner local relation shadows an unqualified outer column.
    if let Ok(mut column) = resolve(name, &scopes[1..]) {
        column.right = true;
        return Ok(column);
    }
    resolve(name, &scopes[..1])
}
fn condition(value: Expr, state: &mut Conditions) -> Result<()> {
    match value {
        Expr::And(values) => {
            for value in values {
                condition(value, state)?;
            }
        }
        Expr::Compare(Compare::Eq, left, right)
            if left.as_remainder().is_some() || right.as_remainder().is_some() =>
        {
            let (input, divisor, key) = if let Some(value) = left.as_remainder() {
                (&value.input, value.divisor, right.as_column())
            } else {
                let value = right.as_remainder().context("missing lookup MOD")?;
                (&value.input, value.divisor, left.as_column())
            };
            let key = key.context("lookup MOD key requires native column")?;
            ensure!(
                !input.right && key.right,
                "lookup MOD key must correlate left input with right column"
            );
            let column =
                crate::compiler::remainder::bind(input.clone(), divisor, &mut state.computed)?;
            state.keys.push((column, key.clone()));
        }
        Expr::Compare(op, left, right) => {
            let left = left.as_column().context("lookup requires native columns")?.clone();
            let right = right.as_column().context("lookup requires native columns")?.clone();
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
fn compatible_keys(
    left: &ColumnRef,
    right: &ColumnRef,
    scopes: &[Scope<'_>; 2],
    computed: &[crate::compiler::scalar::Computed],
) -> Result<()> {
    let native = |side: usize, name: &str| {
        scopes[side]
            .relation
            .columns
            .iter()
            .find(|column| column.name == name)
            .context("missing lookup key")
    };
    let rhs = native(1, &right.name)?;
    if computed.iter().any(|value| value.column == qualify(left.clone())) {
        ensure!(
            left.oid == rhs.oid && rhs.modifier == -1 && rhs.collation == 0,
            "incompatible computed lookup key type"
        );
        return Ok(());
    }
    let lhs = native(0, &left.name)?;
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
        .map(|(name, label)| {
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
    computed: Vec<crate::compiler::scalar::Computed>,
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
    if !computed.is_empty() {
        nodes.insert(
            2,
            Node::Map { id: "left_scalar".into(), input: "left_source".into(), computed },
        );
        if let Some(Node::LookupInput { input, .. }) = nodes.get_mut(3) {
            *input = "left_scalar".into();
        }
    }
    let input = predicate.map_or("lookup", |predicate| {
        nodes.push(Node::Filter { id: "qualified".into(), input: "lookup".into(), predicate });
        "qualified"
    });
    nodes.push(Node::Output { id: "project".into(), input: input.into() });
    nodes
}
