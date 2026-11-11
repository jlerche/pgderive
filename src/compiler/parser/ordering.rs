//! SQL sort scope, with hidden targets resolved through the ordinary target binder.
use super::{expressions::column, node, partition, query_visible};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(in crate::compiler) struct Parsed {
    pub inner: Box<super::Parsed>,
    pub order: Vec<Order>,
    pub visible: usize,
    pub distinct: bool,
    pub distinct_on: Vec<usize>,
    pub bounds: Bounds,
}
pub(in crate::compiler) struct Order {
    pub index: usize,
    pub descending: bool,
    pub nulls_first: bool,
    pub bytewise: bool,
}

pub(super) fn parse(select: &pg::SelectStmt) -> Result<Option<Parsed>> {
    let distinct_on = select.distinct_clause.iter().any(|value| value.node.is_some());
    if !distinct_on
        && select.sort_clause.is_empty()
        && select.limit_count.is_none()
        && select.limit_offset.is_none()
    {
        return Ok(None);
    }
    ensure!(select.sort_clause.len() <= 16, "ORDER BY exceeds 16 keys");
    let bounds = bounds(select)?;
    let visible = select.target_list.len();
    let targets = partition::parse_targets(select)?;
    let mut plain = select.clone();
    let keys = if distinct_on { distinct_keys(select, &targets, &mut plain)? } else { Vec::new() };
    let order = select
        .sort_clause
        .iter()
        .map(|value| {
            let (expression, descending, nulls_first) = partition::sort_expression(value)?;
            let (unwrapped, bytewise) = byte_order(expression)?;
            let expression = if distinct_on { expression } else { unwrapped };
            let index = include_target(expression, &targets, &mut plain, bytewise, distinct_on)?;
            Ok(Order { index, descending, nulls_first, bytewise })
        })
        .collect::<Result<_>>()?;
    plain.sort_clause.clear();
    plain.limit_count = None;
    plain.limit_offset = None;
    plain.limit_option = i32::from(pg::LimitOption::Default);
    let inner = query_visible(&plain, visible)?;
    if distinct_on {
        ensure!(
            matches!(&inner, super::Parsed::Partition(value) if matches!(value.source, partition::Source::Native(_)) && value.expansion.is_none()),
            "DISTINCT ON requires one publication table"
        );
    }
    Ok(Some(Parsed {
        inner: Box::new(inner),
        visible,
        distinct: !plain.distinct_clause.is_empty(),
        distinct_on: keys,
        order,
        bounds,
    }))
}

fn distinct_keys(
    select: &pg::SelectStmt,
    targets: &[partition::Target],
    plain: &mut pg::SelectStmt,
) -> Result<Vec<usize>> {
    ensure!(
        matches!(select.from_clause.as_slice(), [source] if matches!(source.node, Some(NodeEnum::RangeVar(_))))
            && select.group_clause.is_empty()
            && select.having_clause.is_none()
            && select.window_clause.is_empty(),
        "DISTINCT ON requires one publication table without grouping or windows"
    );
    for value in &select.target_list {
        column(super::target(value)?).context("DISTINCT ON payload requires plain columns")?;
    }
    ensure!(select.distinct_clause.len() <= 16, "DISTINCT ON exceeds 16 keys");
    plain.distinct_clause.clear();
    select
        .distinct_clause
        .iter()
        .map(|value| {
            let (_, collated) = byte_order(value)?;
            include_target(value, targets, plain, collated, true)
        })
        .collect()
}

fn include_target(
    expression: &Node,
    targets: &[partition::Target],
    plain: &mut pg::SelectStmt,
    collated: bool,
    single_table: bool,
) -> Result<usize> {
    if !collated && let Some(index) = target_index(expression, targets, single_table)? {
        return Ok(index);
    }
    let index = plain.target_list.len();
    plain.target_list.push(Node {
        node: Some(NodeEnum::ResTarget(Box::new(pg::ResTarget {
            val: Some(Box::new(expression.clone())),
            ..pg::ResTarget::default()
        }))),
    });
    Ok(index)
}

fn byte_order(value: &Node) -> Result<(&Node, bool)> {
    let NodeEnum::CollateClause(collate) = node(value)? else { return Ok((value, false)) };
    let name = super::expressions::names(&collate.collname)?.0;
    ensure!(
        matches!(name.as_slice(), [schema, name] if schema == "pg_catalog" && matches!(name.as_str(), "C" | "POSIX"))
            || matches!(name.as_slice(), [name] if matches!(name.as_str(), "C" | "POSIX")),
        "ORDER BY COLLATE requires built-in C or POSIX"
    );
    Ok((super::optional(collate.arg.as_deref())?, true))
}

fn count(value: &Node) -> Result<Option<usize>> {
    // PostgreSQL represents LIMIT ALL and LIMIT NULL with the same NULL
    // A_Const. OFFSET NULL uses that node too and means zero at its caller.
    if matches!(node(value)?, NodeEnum::AConst(value) if value.isnull) {
        return Ok(None);
    }
    let value = crate::compiler::typed::native_literal(value)?
        .as_integer_literal()
        .context("LIMIT/OFFSET requires a nonnegative integer literal or NULL")?;
    ensure!((0..=1_000_000).contains(&value), "LIMIT/OFFSET must be in 0..=1000000");
    Ok(Some(usize::try_from(value)?))
}

fn same_expression(
    left: &partition::Target,
    right: &partition::Target,
    single_table: bool,
) -> bool {
    let (partition::Value::Typed(left), partition::Value::Typed(right)) =
        (&left.value, &right.value)
    else {
        return false;
    };
    if single_table && let (Ok(left), Ok(right)) = (column(left), column(right)) {
        return left.0.last() == right.0.last();
    }
    locationless(left) == locationless(right)
}

fn locationless(value: &Node) -> String {
    const FIELD: &str = "location: ";
    let text = format!("{value:?}");
    let mut output = String::with_capacity(text.len());
    let mut rest = text.as_str();
    let mut quoted = false;
    while let Some(character) = rest.chars().next() {
        if !quoted && rest.starts_with(FIELD) {
            rest = rest[FIELD.len()..]
                .trim_start_matches(|digit: char| digit == '-' || digit.is_ascii_digit());
            continue;
        }
        let escaped = quoted && character == '\\';
        quoted ^= character == '"';
        output.push(character);
        rest = &rest[character.len_utf8()..];
        if let Some(next) = rest.chars().next().filter(|_| escaped) {
            output.push(next);
            rest = &rest[next.len_utf8()..];
        }
    }
    output
}

fn target_index(
    value: &Node,
    targets: &[partition::Target],
    single_table: bool,
) -> Result<Option<usize>> {
    if let NodeEnum::AConst(value) = node(value)? {
        let Some(pg::a_const::Val::Ival(value)) = &value.val else {
            anyhow::bail!("ORDER BY constant requires an output ordinal")
        };
        let index =
            usize::try_from(value.ival)?.checked_sub(1).context("ORDER BY ordinal is zero")?;
        ensure!(index < targets.len(), "ORDER BY ordinal exceeds output width");
        return Ok(Some(index));
    }
    if matches!(node(value)?, NodeEnum::ColumnRef(_)) {
        let name = column(value)?;
        if let [label] = name.0.as_slice() {
            let mut candidates = targets.iter().enumerate().filter(|(_, t)| t.label == *label);
            let Some((index, first)) = candidates.next() else { return Ok(None) };
            ensure!(
                candidates.all(|(_, other)| same_expression(first, other, single_table)),
                "ambiguous ORDER BY output label"
            );
            return Ok(Some(index));
        }
    }
    // Hidden expressions bind against the input scope, where SELECT aliases are
    // deliberately unavailable inside expressions, as in PostgreSQL.
    Ok(None)
}

pub(in crate::compiler) struct Bounds {
    pub limit: Option<usize>,
    pub offset: usize,
    pub with_ties: bool,
}

pub(in crate::compiler) fn bounds(select: &pg::SelectStmt) -> Result<Bounds> {
    let with_ties = match pg::LimitOption::try_from(select.limit_option)? {
        pg::LimitOption::Default | pg::LimitOption::Count => false,
        pg::LimitOption::WithTies => true,
        pg::LimitOption::Undefined => anyhow::bail!("invalid LIMIT option"),
    };
    ensure!(!with_ties || !select.sort_clause.is_empty(), "FETCH WITH TIES requires ORDER BY");
    let limit = select.limit_count.as_deref().map(count).transpose()?.flatten();
    let offset = select.limit_offset.as_deref().map(count).transpose()?.flatten().unwrap_or(0);
    if let Some(limit) = limit {
        ensure!(
            limit.checked_add(offset).is_some_and(|total| total <= 1_000_000),
            "LIMIT + OFFSET exceeds 1000000 occurrence budget"
        );
    }
    Ok(Bounds { limit, offset, with_ties: with_ties && limit.is_some() })
}
