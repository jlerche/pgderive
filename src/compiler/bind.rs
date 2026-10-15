mod derived;
mod lookup;
mod membership;
mod native_join;
mod native_scope;
mod partition;
mod relational;
mod scoped_join;
mod terminal;
mod topk;
use super::{
    ColumnRef, Compiled,
    parser::{Name, Parsed, Table},
};
use crate::{
    source::{Contract, Relation},
    worker::Query,
};
use anyhow::{Context, Result, ensure};
struct Scope<'a> {
    table: Table,
    relation: &'a Relation,
}
pub(super) fn bind(parsed: Parsed, contract: &Contract) -> Result<Compiled> {
    match parsed {
        Parsed::Membership { parsed, distinct } => membership::bind(parsed, distinct, contract),
        Parsed::NativeJoin(parsed) => native_join::bind(parsed, contract),
        Parsed::NativeScope { source, columns, predicate }
        | Parsed::Projection { source, columns, predicate } => {
            native_scope::bind(source, columns, predicate, contract)
        }
        Parsed::Terminal { source, targets, predicate } => {
            terminal::bind(source, &targets, predicate.as_deref(), contract)
        }
        Parsed::ScopedJoin(parsed) => scoped_join::bind(parsed, contract),
        Parsed::Derived(parsed) => derived::bind_derived(parsed, contract),
        Parsed::Lookup(parsed) => lookup::bind(parsed, contract),
        Parsed::Grouped(parsed) => grouped(parsed, contract),
        Parsed::Partition(parsed) => partition::bind(parsed, contract),
    }
}
fn grouped(parsed: super::parser::Grouped, contract: &Contract) -> Result<Compiled> {
    let left = scope(parsed.left, contract)?;
    let right = scope(parsed.right, contract)?;
    ensure!(left.relation.oid != right.relation.oid, "SQL resolve: self joins are unsupported");
    ensure!(
        qualifier(&left) != qualifier(&right),
        "SQL resolve: duplicate table qualifier; use aliases"
    );
    let scopes = [left, right];
    let group = resolve(&parsed.group, &scopes)?;
    ensure!(
        group == resolve(&parsed.grouping, &scopes)?,
        "SQL type: SELECT group must equal GROUP BY column"
    );
    ensure!(!group.right, "SQL lower: grouping column must come from left input");
    // Text equality/grouping depends on PostgreSQL collation, which the string
    // arrangement cannot execute. Restrict SQL equality/grouping to native scalars.
    ensure!(
        matches!(group.oid, 16 | 20 | 21 | 23 | 2950 | 1114 | 1184),
        "SQL type: text/varchar grouping requires a collation runtime"
    );
    let sum = resolve(&parsed.sum, &scopes)?;
    ensure!(
        sum.right && matches!(sum.oid, 20 | 21 | 23),
        "SQL type: SUM requires a right integral column"
    );
    let lhs = resolve(&parsed.keys.0, &scopes)?;
    let rhs = resolve(&parsed.keys.1, &scopes)?;
    ensure!(lhs.right != rhs.right, "SQL type: join equality must connect both inputs");
    ensure!(
        matches!(lhs.oid, 16 | 20 | 21 | 23 | 2950 | 1114 | 1184),
        "SQL type: text/varchar join requires a collation runtime"
    );
    let (lhs, rhs) = if lhs.right { (rhs, lhs) } else { (lhs, rhs) };
    let selectors = Query {
        left_schema: scopes[0].relation.schema.clone(),
        left_table: scopes[0].relation.table.clone(),
        left_key: lhs.name,
        group: group.name,
        right_schema: scopes[1].relation.schema.clone(),
        right_table: scopes[1].relation.table.clone(),
        right_key: rhs.name,
        sum: sum.name,
    };
    selectors.validate(contract).context("SQL type checking")?;
    let predicates = parsed
        .predicates
        .map(|expr| {
            super::expression::bind(expr, &|name| Ok(relational::qualify(resolve(name, &scopes)?)))
        })
        .transpose()?;
    let mut compiled = legacy(selectors, contract)?;
    if let Some(predicate) = predicates {
        use super::relational::Node;
        let group = compiled
            .relational
            .nodes
            .iter_mut()
            .find_map(|node| match node {
                Node::PartitionBy { input, .. } => Some(input),
                _ => None,
            })
            .context("missing grouped input")?;
        let input = std::mem::replace(group, "qualified".into());
        let position = compiled
            .relational
            .nodes
            .iter()
            .position(|node| matches!(node, Node::PartitionBy { .. }))
            .context("missing grouped input")?;
        compiled.relational.nodes.insert(
            position,
            Node::Filter {
                id: "qualified".into(),
                input,
                predicate: super::Predicate::new(predicate),
            },
        );
    }
    compiled.relational.output.columns.iter_mut().zip(parsed.labels).for_each(|(column, label)| {
        column.label = label;
    });
    Ok(compiled)
}
pub(super) fn legacy(selectors: Query, contract: &Contract) -> Result<Compiled> {
    use super::{
        partition::{Aggregate, Function, Mode, Partition},
        projection::OutputColumn,
        relational::{Relational, Source},
    };
    let relation = |schema: &str, table: &str| {
        contract
            .relations
            .iter()
            .find(|relation| relation.schema == schema && relation.table == table)
            .context("missing grouped source")
    };
    let left = relation(&selectors.left_schema, &selectors.left_table)?;
    let right = relation(&selectors.right_schema, &selectors.right_table)?;
    let group = grouped_column(left, 0, &selectors.group)?;
    let spec = Partition {
        mode: Mode::Grouped { keys: vec![group.clone()] },
        aggregates: vec![
            Aggregate {
                function: Function::Count,
                argument: None,
                filter: None,
                field: "@aggregate_0".into(),
                navigation: None,
            },
            Aggregate {
                function: Function::Sum,
                argument: Some(grouped_column(right, 1, &selectors.sum)?),
                filter: None,
                field: "@aggregate_1".into(),
                navigation: None,
            },
        ],
    };
    let nodes = grouped_nodes(
        [
            grouped_column(left, 0, &selectors.left_key)?,
            grouped_column(right, 1, &selectors.right_key)?,
        ],
        group.clone(),
        spec,
    );
    let output = super::Projected {
        schema: left.schema.clone(),
        table: left.table.clone(),
        columns: vec![
            OutputColumn { column: group, label: selectors.group },
            OutputColumn {
                column: ColumnRef {
                    name: "@aggregate_0".into(),
                    right: false,
                    oid: 20,
                    nullable: false,
                },
                label: "count".into(),
            },
            OutputColumn {
                column: ColumnRef {
                    name: "@aggregate_1".into(),
                    right: false,
                    oid: 20,
                    nullable: true,
                },
                label: "sum".into(),
            },
        ],
        terminal: None,
    };
    let sources = [left, right]
        .map(|relation| Source { schema: relation.schema.clone(), table: relation.table.clone() })
        .into();
    Ok(Compiled { relational: Relational { sources, nodes, output } })
}

fn grouped_column(relation: &Relation, source: usize, name: &str) -> Result<ColumnRef> {
    let column = relation
        .columns
        .iter()
        .find(|column| column.name == name)
        .context("missing grouped column")?;
    Ok(ColumnRef {
        name: super::relational::field(source, name),
        right: false,
        oid: column.oid,
        nullable: column.nullable,
    })
}

fn grouped_nodes(
    [left, right]: [ColumnRef; 2],
    group: ColumnRef,
    spec: super::partition::Partition,
) -> Vec<super::relational::Node> {
    use super::relational::Node;
    vec![
        Node::Source { id: "left_source".into(), source: 0 },
        Node::Source { id: "right_source".into(), source: 1 },
        Node::EquijoinKey { id: "left".into(), input: "left_source".into(), keys: vec![left] },
        Node::EquijoinKey { id: "right".into(), input: "right_source".into(), keys: vec![right] },
        Node::Join { id: "join".into(), left: "left".into(), right: "right".into() },
        Node::PartitionBy { id: "group".into(), input: "join".into(), keys: vec![group] },
        Node::Statistics { id: "statistics".into(), input: "group".into(), spec: spec.clone() },
        Node::Finalize { id: "finalized".into(), input: "statistics".into(), spec },
        Node::Output { id: "output".into(), input: "finalized".into() },
    ]
}

fn scope(table: Table, contract: &Contract) -> Result<Scope<'_>> {
    let relation = contract
        .relations
        .iter()
        .find(|relation| table.name.0 == [relation.schema.clone(), relation.table.clone()])
        .context("SQL resolve: relation absent from native publication")?;
    Ok(Scope { table, relation })
}
fn qualifier(scope: &Scope<'_>) -> String {
    scope.table.alias.clone().unwrap_or_else(|| scope.relation.table.clone())
}
fn resolve(name: &Name, scopes: &[Scope<'_>]) -> Result<ColumnRef> {
    let mut matches = Vec::new();
    for (index, scope) in scopes.iter().enumerate() {
        let qualified = match name.0.as_slice() {
            [_] => true,
            [table, _] => table == &qualifier(scope),
            [schema, table, _] => {
                scope.table.alias.is_none()
                    && schema == &scope.relation.schema
                    && table == &scope.relation.table
            }
            _ => false,
        };
        if qualified
            && let Some(column) =
                scope.relation.columns.iter().find(|column| Some(&column.name) == name.0.last())
        {
            matches.push(ColumnRef {
                right: index == 1,
                name: column.name.clone(),
                oid: column.oid,
                nullable: column.nullable,
            });
        }
    }
    ensure!(matches.len() <= 1, "SQL resolve: ambiguous column {}", name.0.join("."));
    matches.pop().with_context(|| format!("SQL resolve: unknown column {}", name.0.join(".")))
}

fn terminal(
    mut transforms: Vec<crate::catalog::terminal::OutputMap>,
    columns: &[crate::compiler::projection::OutputColumn],
) -> Result<Option<crate::catalog::Terminal>> {
    use crate::catalog::terminal::OutputMap;
    for (transform, column) in transforms.iter_mut().zip(columns) {
        if *transform == OutputMap::Identity && column.column.oid == 1700 {
            *transform = OutputMap::Numeric;
        }
    }
    if transforms.iter().all(|transform| *transform == OutputMap::Identity) {
        return Ok(None);
    }
    Ok(Some(crate::catalog::Terminal::new(
        transforms,
        columns.iter().map(|output| output.column.oid).collect(),
    )?))
}
