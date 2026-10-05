use super::Name;
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(super) fn node(value: &Node) -> Result<&NodeEnum> {
    value.node.as_ref().context("missing AST node")
}
pub(super) fn optional(value: Option<&Node>) -> Result<&Node> {
    value.context("missing expression")
}
pub(super) fn names(values: &[Node]) -> Result<Name> {
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
pub(super) fn column(value: &Node) -> Result<Name> {
    let NodeEnum::ColumnRef(column) = node(value)? else {
        anyhow::bail!("expected column reference");
    };
    ensure!((1..=3).contains(&column.fields.len()), "invalid column qualification");
    names(&column.fields)
}
pub(super) fn target(value: &Node) -> Result<&Node> {
    let NodeEnum::ResTarget(target) = node(value)? else {
        anyhow::bail!("expected output target");
    };
    ensure!(target.indirection.is_empty(), "output indirection unsupported");
    optional(target.val.as_deref())
}
pub(super) fn aggregate<'a>(value: &'a Node, name: &str, star: bool) -> Result<Option<&'a Node>> {
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
pub(super) fn predicates(value: Option<&Node>) -> Result<Vec<(Name, bool)>> {
    let mut pending: Vec<_> = value.into_iter().collect();
    let mut result = Vec::new();
    let mut visited = 0;
    while let Some(value) = pending.pop() {
        visited += 1;
        ensure!(visited <= 512, "WHERE exceeds AST budget");
        match node(value)? {
            NodeEnum::BoolExpr(expr) => {
                ensure!(
                    expr.boolop == i32::from(pg::BoolExprType::AndExpr)
                        && expr.xpr.is_none()
                        && expr.args.len() >= 2,
                    "WHERE supports only AND"
                );
                pending.extend(&expr.args);
            }
            NodeEnum::NullTest(test) => {
                ensure!(!test.argisrow && test.xpr.is_none(), "row NULL tests unsupported");
                let not = match pg::NullTestType::try_from(test.nulltesttype)? {
                    pg::NullTestType::IsNull => false,
                    pg::NullTestType::IsNotNull => true,
                    pg::NullTestType::Undefined => anyhow::bail!("invalid NULL test"),
                };
                result.push((column(optional(test.arg.as_deref())?)?, not));
            }
            _ => anyhow::bail!("WHERE requires column IS [NOT] NULL"),
        }
    }
    Ok(result)
}
