use super::{Name, Table, column, node, optional, target, windows};
use crate::compiler::partition::Function;
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
pub(in crate::compiler) struct Parsed {
    pub source: Source,
    pub expansion: Option<super::expansion::Series>,
    pub targets: Vec<Target>,
    pub visible: usize,
    pub groups: Vec<super::scalar::Key>,
    pub predicate: Option<crate::compiler::syntax::Expr>,
    pub having: Option<crate::compiler::syntax::Expr>,
}
pub(in crate::compiler) enum Source {
    Unit,
    Join(super::native_join::Tree),
    Native(Table),
    Derived { inner: Box<super::Parsed>, alias: String },
}
#[derive(Clone)]
pub(in crate::compiler) enum Value {
    Typed(Box<Node>),
    Aggregate(Box<Aggregate>),
}
#[derive(Clone)]
pub(in crate::compiler) struct Target {
    pub value: Value,
    pub label: String,
}
#[derive(Clone)]
pub(in crate::compiler) struct Aggregate {
    pub function: crate::compiler::partition::Function,
    pub argument: Option<Argument>,
    pub filter: Option<crate::compiler::syntax::Expr>,
    pub window: Option<Window>,
    pub navigation: Option<super::navigation::Navigation>,
    pub position: Option<Box<Node>>,
}
#[derive(Clone)]
pub(in crate::compiler) enum Argument {
    Column(Name),
    Expression(Box<Node>),
}
#[derive(Clone)]
pub(in crate::compiler) struct Window {
    pub keys: Vec<super::scalar::Key>,
    pub order: Vec<(super::scalar::Key, bool, bool)>,
    pub frame: crate::compiler::partition::Frame,
    pub ranking: bool,
    pub peers: Option<bool>,
}
pub(super) fn parse(select: &pg::SelectStmt, visible: usize) -> Result<Parsed> {
    ensure!((1..=64).contains(&select.target_list.len()), "partition output count unsupported");
    let (source, expansion) = if select.from_clause.is_empty() {
        (Source::Unit, None)
    } else {
        let [from] = select.from_clause.as_slice() else {
            anyhow::bail!("SELECT requires one source");
        };
        if let NodeEnum::RangeSubselect(derived) = node(from)? {
            let (inner, alias) = super::derived::source_query(derived)?;
            (Source::Derived { inner: Box::new(inner), alias }, None)
        } else if matches!(node(from)?, NodeEnum::JoinExpr(_)) && !super::expansion::eligible(from)?
        {
            (Source::Join(super::native_join::tree(from, &mut 0, 0)?), None)
        } else {
            let (table, expansion) = super::expansion::source(from)?;
            (Source::Native(table), expansion)
        }
    };
    let windows = windows::Definitions::new(&select.window_clause)?;
    let targets = targets_with_windows(select, &windows)?;
    windows.require_used()?;
    let groups = select
        .group_clause
        .iter()
        .map(|value| group_key(value, &targets[..visible]))
        .collect::<Result<_>>()?;
    Ok(Parsed {
        source,
        expansion,
        targets,
        visible,
        groups,
        predicate: crate::compiler::syntax::predicate(select.where_clause.as_deref())?,
        having: crate::compiler::syntax::predicate(select.having_clause.as_deref())?,
    })
}
fn group_key(value: &Node, visible: &[Target]) -> Result<super::scalar::Key> {
    let NodeEnum::AConst(value) = node(value)? else {
        return super::scalar::key(value);
    };
    let Some(pg::a_const::Val::Ival(value)) = &value.val else {
        anyhow::bail!("GROUP BY constant requires an output ordinal");
    };
    let index = usize::try_from(value.ival)?.checked_sub(1).context("GROUP BY ordinal is zero")?;
    group_target(visible.get(index).context("GROUP BY ordinal exceeds output width")?)
}

pub(in crate::compiler) fn group_target(target: &Target) -> Result<super::scalar::Key> {
    let Value::Typed(value) = &target.value else {
        anyhow::bail!("GROUP BY cannot reference an aggregate or window target");
    };
    // Bind the selected expression in the input scope.
    Ok(super::scalar::Key::Typed(value.clone()))
}

pub(super) fn parse_targets(select: &pg::SelectStmt) -> Result<Vec<Target>> {
    targets_with_windows(select, &windows::Definitions::new(&select.window_clause)?)
}

fn targets_with_windows(
    select: &pg::SelectStmt,
    windows: &windows::Definitions<'_>,
) -> Result<Vec<Target>> {
    select.target_list.iter().map(|value| target_with_windows(value, windows)).collect()
}

pub(in crate::compiler) fn parse_target(value: &Node) -> Result<Target> {
    target_with_windows(value, &windows::Definitions::default())
}

fn target_with_windows(value: &Node, windows: &windows::Definitions<'_>) -> Result<Target> {
    let NodeEnum::ResTarget(output) = node(value)? else {
        anyhow::bail!("unsupported target");
    };
    ensure!(output.indirection.is_empty(), "output indirection unsupported");
    let expression = target(value)?;
    let (value, label) = if let NodeEnum::FuncCall(call) = node(expression)? {
        let name = super::expressions::names(&call.funcname)?.0;
        if let Some((name, function)) = super::expressions::builtin_name(&name)
            .and_then(|name| aggregate_function(name).map(|function| (name, function)))
        {
            (Value::Aggregate(Box::new(aggregate(call, function, windows)?)), name.to_owned())
        } else {
            (
                Value::Typed(Box::new(expression.clone())),
                name.last().context("missing function name")?.clone(),
            )
        }
    } else {
        let label = super::label::output(expression)?;
        (Value::Typed(Box::new(expression.clone())), label)
    };
    Ok(Target { value, label: if output.name.is_empty() { label } else { output.name.clone() } })
}
fn group_function(name: &str) -> Option<Function> {
    Some(match name {
        "count" => Function::Count,
        "sum" => Function::Sum,
        "avg" => Function::Average,
        "min" => Function::Min,
        "max" => Function::Max,
        "bool_and" | "every" => Function::BoolAnd,
        "bool_or" => Function::BoolOr,
        _ => return None,
    })
}

pub(in crate::compiler) fn group_aggregate(value: &Node) -> bool {
    let Some(NodeEnum::FuncCall(call)) = &value.node else { return false };
    call.over.is_none()
        && super::expressions::names(&call.funcname).ok().is_some_and(|name| {
            super::expressions::builtin_name(&name.0).and_then(group_function).is_some()
        })
}

fn aggregate_function(name: &str) -> Option<Function> {
    group_function(name).or(match name {
        "rank" => Some(Function::Rank),
        "dense_rank" => Some(Function::DenseRank),
        "row_number" => Some(Function::RowNumber),
        "lag" => Some(Function::Lag),
        "lead" => Some(Function::Lead),
        "first_value" => Some(Function::FirstValue),
        "last_value" => Some(Function::LastValue),
        "nth_value" => Some(Function::NthValue),
        _ => None,
    })
}

fn distinct_function(function: Function, call: &pg::FuncCall) -> Result<Function> {
    ensure!(call.over.is_none() && !call.agg_star, "unsupported aggregate modifier");
    Ok(match function {
        Function::Count => Function::CountDistinct,
        Function::Sum => Function::SumDistinct,
        Function::Average => Function::AverageDistinct,
        Function::Min => Function::MinDistinct,
        Function::Max => Function::MaxDistinct,
        // Boolean results are invariant to duplicate inputs.
        Function::BoolAnd | Function::BoolOr => function,
        _ => anyhow::bail!("unsupported aggregate modifier"),
    })
}

fn aggregate(
    call: &pg::FuncCall,
    function: Function,
    windows: &windows::Definitions<'_>,
) -> Result<Aggregate> {
    let function = if call.agg_distinct { distinct_function(function, call)? } else { function };
    ensure!(
        !call.agg_within_group
            && !call.func_variadic
            && call.agg_order.is_empty()
            && call.funcformat == i32::from(pg::CoercionForm::CoerceExplicitCall),
        "unsupported aggregate modifier"
    );
    let ranking = function.ranking();
    let navigation = if matches!(function, Function::Lag | Function::Lead) {
        Some(super::navigation::parse(call)?)
    } else {
        None
    };
    let position = window_value_position(call, &function)?;
    let argument = if let Some(navigation) = &navigation {
        Some(Argument::Column(navigation.argument.clone()))
    } else if ranking {
        ensure!(
            call.over.is_some()
                && call.args.is_empty()
                && !call.agg_star
                && call.agg_filter.is_none(),
            "ranking requires OVER, no arguments or FILTER"
        );
        None
    } else if call.agg_star {
        ensure!(matches!(function, Function::Count) && call.args.is_empty(), "star requires COUNT");
        None
    } else {
        let argument = match call.args.as_slice() {
            [argument] => argument,
            [argument, _] if position.is_some() => argument,
            _ => anyhow::bail!("aggregate requires one scalar argument"),
        };
        if matches!(node(argument)?, NodeEnum::ColumnRef(_)) {
            Some(Argument::Column(column(argument)?))
        } else {
            Some(Argument::Expression(Box::new(argument.clone())))
        }
    };
    Ok(Aggregate {
        function,
        argument,
        filter: crate::compiler::syntax::predicate(call.agg_filter.as_deref())?,
        window: call
            .over
            .as_deref()
            .map(|value| {
                let value = windows.resolve(value)?;
                if navigation.is_some() {
                    super::navigation::window(value)
                } else {
                    window(value, ranking)
                }
            })
            .transpose()?,
        navigation,
        position: position.cloned().map(Box::new),
    })
}
fn window_value_position<'a>(
    call: &'a pg::FuncCall,
    function: &Function,
) -> Result<Option<&'a Node>> {
    if !function.frame_value() {
        return Ok(None);
    }
    ensure!(
        call.over.is_some() && !call.agg_star && call.agg_filter.is_none(),
        "frame value functions require OVER and disallow star and FILTER"
    );
    if matches!(function, Function::NthValue) {
        let [_, position] = call.args.as_slice() else {
            anyhow::bail!("NTH_VALUE requires a value and position");
        };
        return Ok(Some(position));
    }
    Ok(None)
}
pub(super) fn window(spec: windows::Spec<'_>, ranking: bool) -> Result<Window> {
    let value = spec.frame;
    if ranking {
        ensure!(
            value.frame_options == windows::DEFAULT_FRAME,
            "ranking currently requires the default frame"
        );
        return Ok(Window {
            keys: spec.partition.iter().map(super::scalar::key).collect::<Result<_>>()?,
            order: spec.order.iter().map(sort_key).collect::<Result<_>>()?,
            frame: crate::compiler::partition::Frame { start: None, end: Some(0) },
            ranking,
            peers: None,
        });
    }
    // PostgreSQL 17 parsenodes.h: accept standard boundaries, reject exclusions.
    ensure!(
        value.frame_options & (2 | 4 | 8) != 0
            && value.frame_options
                & !(1 | 2 | 4 | 8 | 16 | 32 | 256 | 512 | 1024 | 2048 | 4096 | 8192 | 16384)
                == 0,
        "unsupported window frame"
    );
    let peers =
        if value.frame_options & 4 != 0 { None } else { Some(value.frame_options & 8 != 0) };
    ensure!(
        peers != Some(false) || value.frame_options & (2048 | 4096 | 8192 | 16384) == 0,
        "RANGE offsets require typed distance semantics"
    );
    let keys = spec.partition.iter().map(super::scalar::key).collect::<Result<_>>()?;
    let order: Vec<_> = spec.order.iter().map(sort_key).collect::<Result<_>>()?;
    ensure!(peers != Some(true) || !order.is_empty(), "GROUPS requires ORDER BY");
    let start = bound(value.frame_options, value.start_offset.as_deref(), true)?;
    let end = bound(value.frame_options, value.end_offset.as_deref(), false)?;

    Ok(Window {
        keys,
        order,
        frame: crate::compiler::partition::Frame { start, end },
        ranking,
        peers,
    })
}
fn sort_key(value: &Node) -> Result<(super::scalar::Key, bool, bool)> {
    let (value, descending, nulls_first) = sort_expression(value)?;
    Ok((super::scalar::key(value)?, descending, nulls_first))
}
pub(super) fn sort_expression(value: &Node) -> Result<(&Node, bool, bool)> {
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
    Ok((optional(sort.node.as_deref())?, descending, nulls_first))
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

pub(super) fn sort(value: &Node) -> Result<(Name, bool, bool)> {
    let (key, descending, nulls_first) = sort_key(value)?;
    let super::scalar::Key::Column(name) = key else {
        anyhow::bail!("lookup ordering requires native columns")
    };
    Ok((name, descending, nulls_first))
}
