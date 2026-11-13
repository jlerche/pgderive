//! `PostgreSQL` output names for unaliased targets, following `FigureColname`.
use super::expressions::{column, names, node, optional};
use anyhow::{Context, Result};
use pg_query::{Node, NodeEnum, protobuf as pg};

const UNNAMED: &str = "?column?";
const NONE: u8 = 0;
const WEAK: u8 = 1;
const STRONG: u8 = 2;

pub(super) fn output(value: &Node) -> Result<String> {
    Ok(figure(value)?.1)
}

fn figure(value: &Node) -> Result<(u8, String)> {
    Ok(match node(value)? {
        NodeEnum::ColumnRef(_) => {
            (STRONG, column(value)?.0.last().cloned().context("empty column label")?)
        }
        NodeEnum::FuncCall(call) => {
            (STRONG, names(&call.funcname)?.0.last().cloned().context("empty function label")?)
        }
        NodeEnum::CoalesceExpr(_) => (STRONG, "coalesce".into()),
        NodeEnum::MinMaxExpr(expr) => {
            let name = match pg::MinMaxOp::try_from(expr.op)? {
                pg::MinMaxOp::IsGreatest => "greatest",
                pg::MinMaxOp::IsLeast => "least",
                pg::MinMaxOp::Undefined => anyhow::bail!("invalid GREATEST/LEAST operator"),
            };
            (STRONG, name.into())
        }
        NodeEnum::AExpr(expr) if expr.kind == i32::from(pg::AExprKind::AexprNullif) => {
            (STRONG, "nullif".into())
        }
        NodeEnum::TypeCast(cast) => {
            let inner = figure(optional(cast.arg.as_deref())?)?;
            if inner.0 > WEAK {
                inner
            } else {
                let name = cast.type_name.as_ref().context("missing cast type")?;
                (WEAK, names(&name.names)?.0.last().cloned().context("empty cast label")?)
            }
        }
        NodeEnum::CollateClause(clause) => figure(optional(clause.arg.as_deref())?)?,
        NodeEnum::CaseExpr(case) => {
            let otherwise = case.defresult.as_deref().map(figure).transpose()?.unwrap_or_default();
            if otherwise.0 > WEAK { otherwise } else { (WEAK, "case".into()) }
        }
        NodeEnum::SubLink(link) => sublink(link)?,
        _ => (NONE, UNNAMED.into()),
    })
}

fn sublink(link: &pg::SubLink) -> Result<(u8, String)> {
    Ok(match pg::SubLinkType::try_from(link.sub_link_type)? {
        pg::SubLinkType::ExistsSublink => (STRONG, "exists".into()),
        pg::SubLinkType::ArraySublink => (STRONG, "array".into()),
        pg::SubLinkType::ExprSublink => {
            let NodeEnum::SelectStmt(select) = node(optional(link.subselect.as_deref())?)? else {
                return Ok((NONE, UNNAMED.into()));
            };
            scalar_label(select)?
        }
        _ => (NONE, UNNAMED.into()),
    })
}

fn scalar_label(mut select: &pg::SelectStmt) -> Result<(u8, String)> {
    while let Some(left) = select.larg.as_deref() {
        select = left;
    }
    if !select.values_lists.is_empty() {
        return Ok((STRONG, "column1".into()));
    }
    let Some(NodeEnum::ResTarget(target)) =
        select.target_list.first().and_then(|target| target.node.as_ref())
    else {
        return Ok((NONE, UNNAMED.into()));
    };
    let label = if target.name.is_empty() {
        figure(optional(target.val.as_deref())?)?.1
    } else {
        target.name.clone()
    };
    Ok((STRONG, label))
}
