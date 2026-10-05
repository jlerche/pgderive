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
pub(in crate::compiler) fn aggregate<'a>(
    value: &'a Node,
    name: &str,
    star: bool,
) -> Result<Option<&'a Node>> {
    let NodeEnum::FuncCall(call) = node(value)? else {
        anyhow::bail!("expected {name} aggregate");
    };
    let pg::FuncCall {
        funcname,
        args,
        agg_order,
        agg_filter,
        over,
        agg_within_group,
        agg_star,
        agg_distinct,
        func_variadic,
        funcformat,
        location: _,
    } = call.as_ref();
    ensure!(
        names(funcname)?.0 == [name]
            && *agg_star == star
            && args.len() == usize::from(!star)
            && agg_order.is_empty()
            && agg_filter.is_none()
            && over.is_none()
            && !agg_within_group
            && !agg_distinct
            && !func_variadic
            && *funcformat == i32::from(pg::CoercionForm::CoerceExplicitCall),
        "unsupported {name} aggregate or modifier"
    );
    Ok(args.first())
}
