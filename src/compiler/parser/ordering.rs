//! SQL sort scope, with hidden targets resolved through the ordinary target binder.
use super::{expressions::column, node, partition, query};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(in crate::compiler) struct Parsed {
    pub inner: Box<super::Parsed>,
    pub order: Vec<(usize, bool, bool)>,
    pub visible: usize,
    pub distinct: bool,
    pub limit: Option<usize>,
}

pub(super) fn parse(select: &pg::SelectStmt) -> Result<Option<Parsed>> {
    if select.sort_clause.is_empty() && select.limit_count.is_none() {
        return Ok(None);
    }
    ensure!(
        (1..=16).contains(&select.sort_clause.len()),
        "ORDER BY requires 1..=16 keys; LIMIT requires ORDER BY"
    );
    ensure!(select.limit_offset.is_none(), "ORDER BY OFFSET unsupported");
    ensure!(
        matches!(
            pg::LimitOption::try_from(select.limit_option)?,
            pg::LimitOption::Default | pg::LimitOption::Count
        ),
        "FETCH WITH TIES unsupported"
    );
    let limit = select.limit_count.as_deref().map(limit).transpose()?;
    let visible = select.target_list.len();
    let targets =
        select.target_list.iter().map(partition::parse_target).collect::<Result<Vec<_>>>()?;
    let mut plain = select.clone();
    let mut order = Vec::new();
    for value in &select.sort_clause {
        let (expression, descending, nulls_first) = partition::sort_expression(value)?;
        let index = if let Some(index) = target_index(expression, &targets)? {
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
        order.push((index, descending, nulls_first));
    }
    plain.sort_clause.clear();
    plain.limit_count = None;
    plain.limit_option = i32::from(pg::LimitOption::Default);
    Ok(Some(Parsed {
        inner: Box::new(query(&plain)?),
        visible,
        distinct: !plain.distinct_clause.is_empty(),
        order,
        limit,
    }))
}

fn limit(value: &Node) -> Result<usize> {
    let value = crate::compiler::typed::native_literal(value)?
        .as_integer_literal()
        .context("LIMIT requires a nonnegative integer literal")?;
    ensure!((0..=1_000_000).contains(&value), "LIMIT must be in 0..=1000000");
    usize::try_from(value).map_err(Into::into)
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
