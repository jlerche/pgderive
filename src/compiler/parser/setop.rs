//! SQL positional set scopes; set-operation sorting can name only exposed columns.
use super::{Parsed, expressions::column, node, partition, query};
use anyhow::{Context, Result, ensure};
use pg_query::{NodeEnum, protobuf as pg};
pub(in crate::compiler) struct SetOperation {
    pub kind: crate::compiler::setop::Kind,
    pub all: bool,
    pub left: Box<Parsed>,
    pub right: Box<Parsed>,
    pub order: Vec<Order>,
    pub bounds: super::ordering::Bounds,
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
    let kind = match pg::SetOperation::try_from(select.op)? {
        pg::SetOperation::SetopUnion => crate::compiler::setop::Kind::Union,
        pg::SetOperation::SetopIntersect => crate::compiler::setop::Kind::Intersect,
        pg::SetOperation::SetopExcept => crate::compiler::setop::Kind::Except,
        pg::SetOperation::SetopNone | pg::SetOperation::Undefined => {
            anyhow::bail!("invalid set operation")
        }
    };
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
        query(value.context("missing set operand")?)
    };
    let bounds = super::ordering::bounds(select)?;
    ensure!(select.sort_clause.len() <= 16, "set ORDER BY exceeds 16 keys");
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
    Ok(Some(Parsed::SetOperation(SetOperation {
        kind,
        all: select.all,
        left: Box::new(branch(select.larg.as_deref())?),
        right: Box::new(branch(select.rarg.as_deref())?),
        order,
        bounds,
    })))
}
