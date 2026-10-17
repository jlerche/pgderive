use super::Name;
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(in crate::compiler) fn node(value: &Node) -> Result<&NodeEnum> {
    value.node.as_ref().context("missing AST node")
}
pub(in crate::compiler) fn optional(value: Option<&Node>) -> Result<&Node> {
    value.context("missing expression")
}
pub(in crate::compiler) fn names(values: &[Node]) -> Result<Name> {
    let parts = values
        .iter()
        .map(|value| {
            let NodeEnum::String(string) = node(value)? else {
                anyhow::bail!("expected identifier, wildcard unsupported");
            };
            Ok(string.sval.clone())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Name(parts))
}
pub(in crate::compiler) fn column(value: &Node) -> Result<Name> {
    let NodeEnum::ColumnRef(column) = node(value)? else {
        anyhow::bail!("expected column reference");
    };
    ensure!((1..=3).contains(&column.fields.len()), "invalid column qualification");
    names(&column.fields)
}
pub(in crate::compiler) fn target(value: &Node) -> Result<&Node> {
    let NodeEnum::ResTarget(target) = node(value)? else {
        anyhow::bail!("expected output target");
    };
    ensure!(target.indirection.is_empty(), "output indirection unsupported");
    optional(target.val.as_deref())
}
pub(in crate::compiler) const fn unmodified_type(kind: &pg::TypeName) -> bool {
    kind.typmods.is_empty() && kind.array_bounds.is_empty() && !kind.setof && !kind.pct_type
}
pub(in crate::compiler) fn builtin_name(names: &[String]) -> Option<&str> {
    match names {
        [name] => Some(name),
        [schema, name] if schema == "pg_catalog" => Some(name),
        _ => None,
    }
}
