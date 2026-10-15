//! Native join trees, preserving source occurrences and outer-join association.
use super::{Columns, Name, Table, node, optional};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
pub(in crate::compiler) enum Tree {
    Source(Table),
    Exists { left: Box<Self>, right: Table, keys: Vec<(Name, Name)>, anti: bool },
    Derived { source: Box<super::Parsed>, alias: String },
    Join { left: Box<Self>, right: Box<Self>, keys: Vec<(Name, Name)>, outer: bool },
}
pub(in crate::compiler) struct Parsed {
    pub tree: Tree,
    pub columns: Columns,
    pub predicate: Option<crate::compiler::syntax::Expr>,
}
pub(super) fn parse(select: &pg::SelectStmt) -> Result<Option<Parsed>> {
    let [from] = select.from_clause.as_slice() else { return Ok(None) };
    let NodeEnum::JoinExpr(_) = node(from)? else { return Ok(None) };
    if !native_tree(from)? {
        return Ok(None);
    }
    if !select.group_clause.is_empty() {
        return Ok(None);
    }
    let mut sources = 0;
    Ok(Some(Parsed {
        tree: tree(from, &mut sources, 0)?,
        columns: super::projection_columns(&select.target_list)?,
        predicate: crate::compiler::syntax::predicate(select.where_clause.as_deref())?,
    }))
}
fn tree(value: &Node, sources: &mut usize, depth: usize) -> Result<Tree> {
    ensure!(depth < 16, "join tree depth exceeded");
    match node(value)? {
        NodeEnum::RangeSubselect(value) => {
            ensure!(!value.lateral, "lateral derived joins unsupported");
            let (source, alias) = super::derived::source_query(value)?;
            Ok(Tree::Derived { source: Box::new(source), alias })
        }
        NodeEnum::RangeVar(_) => {
            *sources += 1;
            ensure!(*sources <= 16, "join source occurrence limit exceeded");
            Ok(Tree::Source(super::table(value)?))
        }
        NodeEnum::JoinExpr(join) => {
            ensure!(
                matches!(
                    pg::JoinType::try_from(join.jointype)?,
                    pg::JoinType::JoinInner | pg::JoinType::JoinLeft
                ) && !join.is_natural
                    && join.using_clause.is_empty()
                    && join.join_using_alias.is_none()
                    && join.alias.is_none()
                    && join.rtindex == 0,
                "unsupported native join shape"
            );
            let left = tree(optional(join.larg.as_deref())?, sources, depth + 1)?;
            let right = tree(optional(join.rarg.as_deref())?, sources, depth + 1)?;
            let mut keys = Vec::new();
            if let Some(condition) = join.quals.as_deref() {
                equalities(condition, &mut keys)?;
            }
            ensure!(keys.len() <= 16, "join key count exceeded");
            Ok(Tree::Join {
                left: Box::new(left),
                right: Box::new(right),
                keys,
                outer: join.jointype == i32::from(pg::JoinType::JoinLeft),
            })
        }
        _ => anyhow::bail!("native join tree requires ordinary native tables"),
    }
}
pub(super) fn equalities(value: &Node, keys: &mut Vec<(Name, Name)>) -> Result<()> {
    match node(value)? {
        NodeEnum::BoolExpr(value) if value.boolop == i32::from(pg::BoolExprType::AndExpr) => {
            ensure!(value.xpr.is_none(), "invalid join conjunction");
            for value in &value.args {
                equalities(value, keys)?;
            }
        }
        NodeEnum::AExpr(value) => {
            ensure!(
                value.kind == i32::from(pg::AExprKind::AexprOp)
                    && super::expressions::names(&value.name)?.0 == ["="],
                "ON requires column equality"
            );
            keys.push((
                super::column(optional(value.lexpr.as_deref())?)?,
                super::column(optional(value.rexpr.as_deref())?)?,
            ));
        }
        _ => anyhow::bail!("ON requires an AND of ordinary column equalities"),
    }
    Ok(())
}

fn native_tree(value: &Node) -> Result<bool> {
    Ok(match node(value)? {
        NodeEnum::RangeVar(_) => true,
        NodeEnum::RangeSubselect(value) => !value.lateral,
        NodeEnum::JoinExpr(join) => {
            native_tree(optional(join.larg.as_deref())?)?
                && native_tree(optional(join.rarg.as_deref())?)?
        }
        _ => false,
    })
}
