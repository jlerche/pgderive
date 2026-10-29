//! SQL UNION ALL scopes; set-operation sorting can name only exposed columns.
use super::{Parsed, expressions::column, node, partition, query};
use anyhow::{Context, Result, ensure};
use pg_query::{NodeEnum, protobuf as pg};
pub(in crate::compiler) struct Union {
    pub left: Box<Parsed>,
    pub right: Box<Parsed>,
    pub order: Vec<Order>,
    pub limit: Option<usize>,
    pub offset: usize,
}
pub(in crate::compiler) struct Order {
    pub reference: Reference,
    pub descending: bool,
    pub nulls_first: bool,
}
pub(in crate::compiler) enum Reference {
    Label(String),
    Ordinal(usize),
}
pub(super) fn parse(select: &pg::SelectStmt) -> Result<Option<Parsed>> {
    if select.op == i32::from(pg::SetOperation::SetopNone) {
        return Ok(None);
    }
    ensure!(
        select.op == i32::from(pg::SetOperation::SetopUnion) && select.all,
        "only UNION ALL set operations are supported"
    );
    ensure!(
        select.target_list.is_empty()
            && select.from_clause.is_empty()
            && select.where_clause.is_none()
            && select.having_clause.is_none()
            && select.group_clause.is_empty()
            && select.window_clause.is_empty()
            && select.distinct_clause.is_empty()
            && select.into_clause.is_none()
            && select.locking_clause.is_empty()
            && select.values_lists.is_empty()
            && select.with_clause.is_none(),
        "unsupported set-operation clause"
    );
    let branch = |value: Option<&pg::SelectStmt>| -> Result<Parsed> {
        query(value.context("missing UNION operand")?)
    };
    let (limit, offset) = super::ordering::bounds(select)?;
    ensure!(select.sort_clause.len() <= 16, "UNION ORDER BY exceeds 16 keys");
    let order = select
        .sort_clause
        .iter()
        .map(|value| {
            let (value, descending, nulls_first) = partition::sort_expression(value)?;
            let reference = if let NodeEnum::AConst(value) = node(value)? {
                let Some(pg::a_const::Val::Ival(value)) = &value.val else {
                    anyhow::bail!("set ORDER BY requires a label or ordinal")
                };
                Reference::Ordinal(
                    usize::try_from(value.ival)?
                        .checked_sub(1)
                        .context("ORDER BY ordinal is zero")?,
                )
            } else {
                let name = column(value)?;
                let [label] = name.0.as_slice() else {
                    anyhow::bail!("set ORDER BY cannot use source qualifiers or expressions")
                };
                Reference::Label(label.clone())
            };
            Ok(Order { reference, descending, nulls_first })
        })
        .collect::<Result<_>>()?;
    Ok(Some(Parsed::Union(Union {
        left: Box::new(branch(select.larg.as_deref())?),
        right: Box::new(branch(select.rarg.as_deref())?),
        order,
        limit,
        offset,
    })))
}
