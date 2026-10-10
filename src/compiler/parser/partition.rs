use super::{Name, Table, column, node, optional, target};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
pub(in crate::compiler) struct Parsed {
    pub source: Table,
    pub targets: Vec<Target>,
    pub groups: Vec<Name>,
    pub predicate: Option<crate::compiler::syntax::Expr>,
}
pub(in crate::compiler) enum Value {
    Column(Name),
    Aggregate(Aggregate),
}
pub(in crate::compiler) struct Target {
    pub value: Value,
    pub label: String,
}
pub(in crate::compiler) struct Aggregate {
    pub function: crate::compiler::partition::Function,
    pub argument: Option<Name>,
    pub filter: Option<crate::compiler::syntax::Expr>,
    pub window: Option<Window>,
}
pub(in crate::compiler) struct Window {
    pub keys: Vec<Name>,
    pub order: Vec<(Name, bool, bool)>,
    pub frame: crate::compiler::partition::Frame,
}
pub(super) fn eligible(select: &pg::SelectStmt) -> Result<bool> {
    let [source] = select.from_clause.as_slice() else {
        return Ok(false);
    };
    if !matches!(node(source)?, NodeEnum::RangeVar(_)) {
        return Ok(false);
    }
    Ok(!select.group_clause.is_empty()
        || select.target_list.iter().any(|target_node| {
            target(target_node).ok().and_then(|value| node(value).ok()).is_some_and(
                |value| matches!(value, NodeEnum::FuncCall(call) if call.over.is_some()),
            )
        }))
}
pub(super) fn parse(select: &pg::SelectStmt) -> Result<Parsed> {
    ensure!((1..=64).contains(&select.target_list.len()), "partition output count unsupported");
    let source = super::table(select.from_clause.first().context("missing source")?)?;
    let targets = select.target_list.iter().map(parse_target).collect::<Result<_>>()?;
    let groups = select.group_clause.iter().map(column).collect::<Result<_>>()?;
    Ok(Parsed {
        source,
        targets,
        groups,
        predicate: crate::compiler::syntax::predicate(select.where_clause.as_deref())?,
    })
}
fn parse_target(value: &Node) -> Result<Target> {
    let NodeEnum::ResTarget(output) = node(value)? else {
        anyhow::bail!("unsupported target");
    };
    ensure!(output.indirection.is_empty(), "output indirection unsupported");
    let expression = target(value)?;
    let (value, label) = if let NodeEnum::FuncCall(call) = node(expression)? {
        let name = super::expressions::names(&call.funcname)?.0;
        let function = match name.as_slice() {
            [name] => name,
            [schema, name] if schema == "pg_catalog" => name,
            _ => anyhow::bail!("unsupported aggregate namespace"),
        };
        (Value::Aggregate(aggregate(call, function)?), function.clone())
    } else {
        let name = column(expression)?;
        let label = name.0.last().context("missing column")?.clone();
        (Value::Column(name), label)
    };
    Ok(Target { value, label: if output.name.is_empty() { label } else { output.name.clone() } })
}
fn aggregate(call: &pg::FuncCall, name: &str) -> Result<Aggregate> {
    use crate::compiler::partition::Function;
    let function = match name {
        "count" => Function::Count,
        "sum" => Function::Sum,
        "avg" => Function::Average,
        "min" => Function::Min,
        "max" => Function::Max,
        _ => anyhow::bail!("unsupported aggregate"),
    };
    ensure!(
        !call.agg_distinct
            && !call.agg_within_group
            && !call.func_variadic
            && call.agg_order.is_empty()
            && call.funcformat == i32::from(pg::CoercionForm::CoerceExplicitCall),
        "unsupported aggregate modifier"
    );
    let argument = if call.agg_star {
        ensure!(matches!(function, Function::Count) && call.args.is_empty(), "star requires COUNT");
        None
    } else {
        let [argument] = call.args.as_slice() else {
            anyhow::bail!("aggregate requires one column");
        };
        Some(column(argument)?)
    };
    Ok(Aggregate {
        function,
        argument,
        filter: crate::compiler::syntax::predicate(call.agg_filter.as_deref())?,
        window: call.over.as_deref().map(window).transpose()?,
    })
}
fn window(value: &pg::WindowDef) -> Result<Window> {
    ensure!(value.name.is_empty() && value.refname.is_empty(), "named windows unsupported");
    // PostgreSQL 17 parsenodes.h: accept ROWS plus boundary flags; reject RANGE/GROUPS/exclusions.
    ensure!(
        value.frame_options & 4 != 0
            && value.frame_options
                & !(1 | 4 | 16 | 32 | 256 | 512 | 1024 | 2048 | 4096 | 8192 | 16384)
                == 0,
        "window requires an explicit supported ROWS frame"
    );
    let keys = value.partition_clause.iter().map(column).collect::<Result<_>>()?;
    let order = value.order_clause.iter().map(sort).collect::<Result<_>>()?;
    let start = bound(value.frame_options, value.start_offset.as_deref(), true)?;
    let end = bound(value.frame_options, value.end_offset.as_deref(), false)?;

    Ok(Window { keys, order, frame: crate::compiler::partition::Frame { start, end } })
}
fn sort(value: &Node) -> Result<(Name, bool, bool)> {
    let NodeEnum::SortBy(sort) = node(value)? else {
        anyhow::bail!("invalid window order");
    };
    ensure!(sort.use_op.is_empty(), "custom sort operators unsupported");
    let descending = match pg::SortByDir::try_from(sort.sortby_dir)? {
        pg::SortByDir::SortbyDefault | pg::SortByDir::SortbyAsc => false,
        pg::SortByDir::SortbyDesc => true,
        _ => anyhow::bail!("unsupported ordering"),
    };
    let nulls_first = match pg::SortByNulls::try_from(sort.sortby_nulls)? {
        pg::SortByNulls::SortbyNullsDefault => descending,
        pg::SortByNulls::SortbyNullsFirst => true,
        pg::SortByNulls::SortbyNullsLast => false,
        pg::SortByNulls::Undefined => anyhow::bail!("unsupported NULL ordering"),
    };
    Ok((column(optional(sort.node.as_deref())?)?, descending, nulls_first))
}
fn bound(flags: i32, offset: Option<&Node>, start: bool) -> Result<Option<i64>> {
    if flags & if start { 32 } else { 256 } != 0 {
        return Ok(None);
    }
    if flags & if start { 512 } else { 1024 } != 0 {
        return Ok(Some(0));
    }
    let preceding = flags & if start { 2048 } else { 4096 } != 0;
    let following = flags & if start { 8192 } else { 16384 } != 0;
    ensure!(preceding || following, "unsupported frame boundary");
    let NodeEnum::AConst(value) = node(optional(offset)?)? else {
        anyhow::bail!("frame offset requires nonnegative integer constant");
    };
    let Some(pg::a_const::Val::Ival(value)) = &value.val else {
        anyhow::bail!("frame offset requires integer");
    };
    ensure!(value.ival >= 0, "negative frame offset");
    Ok(Some(if preceding { -i64::from(value.ival) } else { i64::from(value.ival) }))
}
