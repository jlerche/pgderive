//! Native join trees, preserving source occurrences and outer-join association.
use super::{Table, node, optional};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
pub(in crate::compiler) enum Tree {
    Source(Table),
    Exists {
        left: Box<Self>,
        right: Table,
        keys: Vec<(super::scalar::Key, super::scalar::Key)>,
        anti: bool,
    },
    Derived {
        source: Box<super::Parsed>,
        alias: String,
    },
    Scalar {
        left: Box<Self>,
        right: Box<super::Parsed>,
        alias: String,
        checked: bool,
        condition: Option<Box<Node>>,
    },
    Mark {
        left: Box<Self>,
        right: Box<super::Parsed>,
        probe: Box<Node>,
        alias: String,
        condition: Option<Box<Node>>,
        semijoin: bool,
        quantified: Option<crate::compiler::syntax::Compare>,
        residual_inequality: bool,
    },
    Join {
        left: Box<Self>,
        right: Box<Self>,
        kind: crate::compiler::left_join::Kind,
        clause: JoinClause,
    },
}
pub(in crate::compiler) enum JoinClause {
    Condition(Option<Box<Node>>),
    Using(Vec<String>),
    Natural,
}
pub(super) fn tree(value: &Node, sources: &mut usize, depth: usize) -> Result<Tree> {
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
                    pg::JoinType::JoinInner
                        | pg::JoinType::JoinLeft
                        | pg::JoinType::JoinRight
                        | pg::JoinType::JoinFull
                ) && join.join_using_alias.is_none()
                    && join.alias.is_none()
                    && join.rtindex == 0,
                "unsupported native join shape"
            );
            let left = tree(optional(join.larg.as_deref())?, sources, depth + 1)?;
            let right = tree(optional(join.rarg.as_deref())?, sources, depth + 1)?;
            Ok(Tree::Join {
                left: Box::new(left),
                right: Box::new(right),
                kind: match pg::JoinType::try_from(join.jointype)? {
                    pg::JoinType::JoinLeft => crate::compiler::left_join::Kind::Left,
                    pg::JoinType::JoinRight => crate::compiler::left_join::Kind::Right,
                    pg::JoinType::JoinFull => crate::compiler::left_join::Kind::Full,
                    _ => crate::compiler::left_join::Kind::Inner,
                },
                clause: join_clause(join)?,
            })
        }
        _ => anyhow::bail!("native join tree requires ordinary native tables"),
    }
}
fn join_clause(join: &pg::JoinExpr) -> Result<JoinClause> {
    if join.is_natural || !join.using_clause.is_empty() {
        ensure!(join.quals.is_none(), "NATURAL/USING joins cannot specify ON");
        if join.is_natural {
            ensure!(join.using_clause.is_empty(), "NATURAL JOIN cannot specify USING");
            return Ok(JoinClause::Natural);
        }
        return Ok(JoinClause::Using(super::expressions::names(&join.using_clause)?.0));
    }
    Ok(JoinClause::Condition(join.quals.clone()))
}

pub(super) fn computed_equalities(
    value: &Node,
    keys: &mut Vec<(super::scalar::Key, super::scalar::Key)>,
    depth: usize,
) -> Result<()> {
    ensure!(depth < 16, "join condition nesting limit exceeded");
    match node(value)? {
        NodeEnum::BoolExpr(value) if value.boolop == i32::from(pg::BoolExprType::AndExpr) => {
            ensure!(value.xpr.is_none(), "invalid join conjunction");
            for value in &value.args {
                computed_equalities(value, keys, depth + 1)?;
            }
        }
        NodeEnum::AExpr(value) => {
            ensure!(
                value.kind == i32::from(pg::AExprKind::AexprOp)
                    && super::expressions::names(&value.name)?.0 == ["="],
                "ON requires ordinary equality"
            );
            ensure!(keys.len() < 16, "join key count exceeded");
            keys.push((
                super::scalar::key(optional(value.lexpr.as_deref())?)?,
                super::scalar::key(optional(value.rexpr.as_deref())?)?,
            ));
        }
        _ => anyhow::bail!("ON requires an AND of ordinary equalities"),
    }
    Ok(())
}
