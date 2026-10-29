//! SQL sort scope, with hidden targets resolved through the ordinary target binder.
use super::{expressions::column, node, partition, query};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(in crate::compiler) struct Parsed {
    pub inner: Box<super::Parsed>,
    pub order: Vec<Order>,
    pub visible: usize,
    pub distinct: bool,
    pub limit: Option<usize>,
    pub offset: usize,
}
pub(in crate::compiler) struct Order {
    pub index: usize,
    pub descending: bool,
    pub nulls_first: bool,
    pub bytewise: bool,
}

pub(super) fn parse(select: &pg::SelectStmt) -> Result<Option<Parsed>> {
    if select.sort_clause.is_empty()
        && select.limit_count.is_none()
        && select.limit_offset.is_none()
    {
        return Ok(None);
    }
    ensure!(select.sort_clause.len() <= 16, "ORDER BY exceeds 16 keys");
    let (limit, offset) = bounds(select)?;
    let visible = select.target_list.len();
    let targets =
        select.target_list.iter().map(partition::parse_target).collect::<Result<Vec<_>>>()?;
    let mut plain = select.clone();
    let mut order = Vec::new();
    for value in &select.sort_clause {
        let (expression, descending, nulls_first) = partition::sort_expression(value)?;
        let (expression, bytewise) = byte_order(expression)?;
        let index = if let Some(index) =
            if bytewise { None } else { target_index(expression, &targets)? }
        {
            index
        } else {
            let index = plain.target_list.len();
            plain.target_list.push(Node {
                node: Some(NodeEnum::ResTarget(Box::new(pg::ResTarget {
                    val: Some(Box::new(expression.clone())),
                    ..pg::ResTarget::default()
                }))),
            });
            index
        };
        order.push(Order { index, descending, nulls_first, bytewise });
    }
    plain.sort_clause.clear();
    plain.limit_count = None;
    plain.limit_offset = None;
    plain.limit_option = i32::from(pg::LimitOption::Default);
    Ok(Some(Parsed {
        inner: Box::new(query(&plain)?),
        visible,
        distinct: !plain.distinct_clause.is_empty(),
        order,
        limit,
        offset,
    }))
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

fn target_index(value: &Node, targets: &[partition::Target]) -> Result<Option<usize>> {
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
            let first = candidates.next().map(|(index, _)| index);
            ensure!(candidates.next().is_none(), "ambiguous ORDER BY output label");
            return Ok(first);
        }
    }
    // Hidden expressions bind against the input scope, where SELECT aliases are
    // deliberately unavailable inside expressions, as in PostgreSQL.
    Ok(None)
}

pub(in crate::compiler) fn bounds(select: &pg::SelectStmt) -> Result<(Option<usize>, usize)> {
    ensure!(
        matches!(
            pg::LimitOption::try_from(select.limit_option)?,
            pg::LimitOption::Default | pg::LimitOption::Count
        ),
        "FETCH WITH TIES unsupported"
    );
    let limit = select.limit_count.as_deref().map(count).transpose()?.flatten();
    let offset = select.limit_offset.as_deref().map(count).transpose()?.flatten().unwrap_or(0);
    if let Some(limit) = limit {
        ensure!(
            limit.checked_add(offset).is_some_and(|total| total <= 1_000_000),
            "LIMIT + OFFSET exceeds 1000000 occurrence budget"
        );
    }
    Ok((limit, offset))
}
