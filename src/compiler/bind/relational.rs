use super::{ColumnRef, Compiled, Name, Table, qualifier, resolve, scope};
use crate::source::Contract;
use anyhow::{Context, Result, ensure};

pub(super) fn bind(
    tables: (Table, Table),
    keys: &(Name, Name),
    columns: crate::compiler::parser::Columns,
    predicate: Option<crate::compiler::syntax::Expr>,
    contract: &Contract,
) -> Result<Compiled> {
    use crate::compiler::relational::{Relational, Source};
    let scopes = [scope(tables.0, contract)?, scope(tables.1, contract)?];
    ensure!(
        scopes[0].relation.oid != scopes[1].relation.oid,
        "SQL resolve: self joins are unsupported"
    );
    ensure!(
        qualifier(&scopes[0]) != qualifier(&scopes[1]),
        "SQL resolve: duplicate table qualifier"
    );
    let (left, right) = keys_for(keys, &scopes)?;
    let resolve = |name: &Name| Ok(qualify(resolve(name, &scopes)?));
    let (columns, terminal) = output(columns, &resolve)?;
    let predicates =
        predicate.map(|expr| crate::compiler::expression::bind(expr, &resolve)).transpose()?;
    let sources = scopes
        .iter()
        .map(|scope| Source {
            schema: scope.relation.schema.clone(),
            table: scope.relation.table.clone(),
        })
        .collect();
    let nodes = nodes(qualify(left), qualify(right));
    let output = crate::compiler::Projected {
        schema: scopes[0].relation.schema.clone(),
        table: scopes[0].relation.table.clone(),
        columns,
        terminal,
    };
    Ok(Compiled { program: crate::compiler::Program::Relational { relational: Relational { sources, nodes, output } }, predicates, revision: Some("sql-relational-v1:pg-query-6.2.1:pg-17.7:row-text-v1:json-v2:raw-record-v1:native-bag-v1:where-3vl-v1:integral-remainder-v1:terminal-builtins-v1".into()) })
}
pub(super) fn keys_for(
    keys: &(Name, Name),
    scopes: &[super::Scope<'_>; 2],
) -> Result<(ColumnRef, ColumnRef)> {
    let left = resolve(&keys.0, scopes)?;
    let right = resolve(&keys.1, scopes)?;
    ensure!(left.right != right.right, "SQL type: join must connect both inputs");
    let (left, right) = if left.right { (right, left) } else { (left, right) };
    let native = |side: usize, name: &str| {
        scopes[side]
            .relation
            .columns
            .iter()
            .find(|column| column.name == name)
            .context("SQL resolve: missing key column")
    };
    let lhs = native(0, &left.name)?;
    let rhs = native(1, &right.name)?;
    ensure!(
        matches!(lhs.oid, 16 | 20 | 21 | 23 | 2950 | 1114 | 1184)
            && lhs.oid == rhs.oid
            && lhs.modifier == rhs.modifier
            && lhs.collation == rhs.collation,
        "SQL type: unsupported or incompatible join key types"
    );
    Ok((left, right))
}
pub(super) fn qualify(mut column: ColumnRef) -> ColumnRef {
    column.name = crate::compiler::relational::field(usize::from(column.right), &column.name);
    column.right = false;
    column
}
type Output = (Vec<crate::compiler::projection::OutputColumn>, Option<crate::catalog::Terminal>);
pub(super) fn output(
    columns: crate::compiler::parser::Columns,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<Output> {
    let transforms = columns.iter().map(|(_, _, transform)| transform.clone()).collect::<Vec<_>>();
    let columns = columns
        .into_iter()
        .map(|(name, label, _)| {
            Ok(crate::compiler::projection::OutputColumn { column: resolve(&name)?, label })
        })
        .collect::<Result<Vec<_>>>()?;
    let terminal = super::terminal(transforms, &columns)?;
    Ok((columns, terminal))
}
fn nodes(left: ColumnRef, right: ColumnRef) -> Vec<crate::compiler::relational::Node> {
    use crate::compiler::relational::Node;
    let nodes = vec![
        Node::Source { id: "left_source".into(), source: 0 },
        Node::Source { id: "right_source".into(), source: 1 },
        Node::KeyBy { id: "left".into(), input: "left_source".into(), key: left },
        Node::KeyBy { id: "right".into(), input: "right_source".into(), key: right },
        Node::Join { id: "join".into(), left: "left".into(), right: "right".into() },
        Node::Project { id: "project".into(), input: "join".into() },
    ];
    nodes
}
