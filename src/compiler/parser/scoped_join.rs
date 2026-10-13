//! Inner equijoins between independently compiled derived relational scopes.
use super::{Columns, Name, node, optional};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(in crate::compiler) struct Parsed {
    pub left: (Box<super::Parsed>, String),
    pub right: (Box<super::Parsed>, String),
    pub keys: Option<Vec<(Name, Name)>>,
    pub columns: Columns,
    pub predicate: Option<crate::compiler::syntax::Expr>,
}
pub(super) fn parse(select: &pg::SelectStmt) -> Result<Option<Parsed>> {
    let [from] = select.from_clause.as_slice() else { return Ok(None) };
    let NodeEnum::JoinExpr(join) = node(from)? else { return Ok(None) };
    let (NodeEnum::RangeSubselect(left), NodeEnum::RangeSubselect(right)) =
        (node(optional(join.larg.as_deref())?)?, node(optional(join.rarg.as_deref())?)?)
    else {
        return Ok(None);
    };
    if right.lateral || left.lateral || join.jointype != i32::from(pg::JoinType::JoinInner) {
        return Ok(None);
    }
    ensure!(
        join.jointype == i32::from(pg::JoinType::JoinInner)
            && !join.is_natural
            && join.using_clause.is_empty()
            && join.join_using_alias.is_none()
            && join.alias.is_none()
            && join.rtindex == 0
            && select.group_clause.is_empty(),
        "scoped join requires an unaliased inner equijoin without grouping"
    );
    let left = super::derived::source_query(left)?;
    let right = super::derived::source_query(right)?;
    let keys = if let Some(quals) = join.quals.as_deref() {
        let mut keys = Vec::new();
        equalities(quals, &mut keys)?;
        ensure!((1..=16).contains(&keys.len()), "scoped join requires 1..=16 equalities");
        Some(keys)
    } else {
        None
    };
    Ok(Some(Parsed {
        left: (Box::new(left.0), left.1),
        right: (Box::new(right.0), right.1),
        keys,
        columns: super::projection_columns(&select.target_list)?,
        predicate: crate::compiler::syntax::predicate(select.where_clause.as_deref())?,
    }))
}
fn equalities(value: &Node, keys: &mut Vec<(Name, Name)>) -> Result<()> {
    match node(value)? {
        NodeEnum::BoolExpr(boolean) if boolean.boolop == i32::from(pg::BoolExprType::AndExpr) => {
            for value in &boolean.args {
                equalities(value, keys)?;
            }
        }
        NodeEnum::AExpr(eq) => {
            ensure!(
                eq.kind == i32::from(pg::AExprKind::AexprOp)
                    && super::expressions::names(&eq.name)?.0 == ["="],
                "scoped ON requires ordinary column equality"
            );
            keys.push((
                super::column(optional(eq.lexpr.as_deref())?)?,
                super::column(optional(eq.rexpr.as_deref())?)?,
            ));
        }
        _ => anyhow::bail!("scoped ON requires an AND of column equalities"),
    }
    Ok(())
}
