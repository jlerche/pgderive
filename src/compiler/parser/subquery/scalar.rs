//! Scalar aggregation decorrelation through existing global/grouped SQL lowering.
use super::{Rewrite, ast, correlation};
use crate::compiler::parser::{Parsed, native_join::Tree, query, target};
use anyhow::{Result, ensure};
use pg_query::{Node, protobuf as pg};
pub(super) struct Scalar {
    pub value: Node,
    pub present: Node,
}
impl Rewrite<'_> {
    pub(super) fn scalar(&mut self, mut select: pg::SelectStmt) -> Result<Scalar> {
        validate(&select)?;
        let correlation = correlation::extract(&mut select)?;
        self.validation.push(query(&select)?);
        let original = target(&select.target_list[0])?.clone();
        let alias = self.alias()?;
        if correlation.pairs.is_empty() {
            select.target_list.push(ast::target(ast::count(), ast::PRESENT));
            self.attach_scalar(named(&select)?, alias.clone())?;
            return Ok(Scalar {
                value: ast::column(&alias, ast::VALUE),
                present: ast::column(&alias, ast::PRESENT),
            });
        }
        let mut empty = select.clone();
        empty.where_clause = Some(Box::new(ast::boolean(false)));
        empty.target_list.push(ast::target(ast::count(), ast::PRESENT));
        let (limit, offset) = crate::compiler::parser::ordering::bounds(&select)?;
        if limit == Some(0) || offset > 0 {
            self.attach_scalar(named(&empty)?, alias.clone())?;
            return Ok(Scalar {
                value: ast::column(&alias, ast::VALUE),
                present: ast::column(&alias, ast::PRESENT),
            });
        }
        ensure!(
            select.sort_clause.is_empty() && limit.is_none(),
            "correlated scalar ordering/LIMIT is unsupported except suppressed global results"
        );
        let having = select.having_clause.take();
        let value = having.as_ref().map_or_else(
            || original.clone(),
            |condition| ast::case((**condition).clone(), original.clone(), ast::null()),
        );
        let present = having
            .map_or_else(ast::count, |condition| ast::case(*condition, ast::count(), ast::null()));
        select.target_list = vec![
            ast::target(value, ast::VALUE),
            ast::target(present, ast::PRESENT),
            ast::target(ast::count(), ast::ROWS),
        ];
        let mut conditions = Vec::new();
        for (index, (outer, inner)) in correlation.pairs.into_iter().enumerate() {
            let label = format!("\0key_{index}");
            select.target_list.push(ast::target(inner.clone(), &label));
            select.group_clause.push(inner);
            conditions.push(ast::comparison(outer, ast::column(&alias, &label), "="));
        }
        let left = self.take_tree()?;
        self.tree = Some(Tree::Join {
            left: Box::new(left),
            right: Box::new(Tree::Derived {
                source: Box::new(named(&select)?),
                alias: alias.clone(),
            }),
            clause: super::super::native_join::JoinClause::Condition(ast::logical(
                conditions, true,
            )),
            kind: crate::compiler::left_join::Kind::Left,
        });
        let fallback = self.alias()?;
        self.attach_scalar(named(&empty)?, fallback.clone())?;
        let missing = ast::is_null(ast::column(&alias, ast::ROWS));
        Ok(Scalar {
            value: ast::case(
                missing.clone(),
                ast::column(&fallback, ast::VALUE),
                ast::column(&alias, ast::VALUE),
            ),
            present: ast::case(
                missing,
                ast::column(&fallback, ast::PRESENT),
                ast::column(&alias, ast::PRESENT),
            ),
        })
    }
    fn attach_scalar(
        &mut self,
        right: crate::compiler::parser::Parsed,
        alias: String,
    ) -> Result<()> {
        self.tree =
            Some(Tree::Scalar { left: Box::new(self.take_tree()?), right: Box::new(right), alias });
        Ok(())
    }
}

fn validate(select: &pg::SelectStmt) -> Result<()> {
    ensure!(
        select.target_list.len() == 1
            && select.group_clause.is_empty()
            && select.op == i32::from(pg::SetOperation::SetopNone),
        "scalar subquery requires one global aggregate output without GROUP BY/set operations"
    );
    ensure!(
        correlation::aggregate(target(&select.target_list[0])?)
            || select.having_clause.as_deref().is_some_and(correlation::aggregate),
        "scalar subquery requires a global aggregate; arbitrary cardinality is unsupported"
    );
    Ok(())
}

// Internal labels are assigned only after ordinary SQL alias/ordinal resolution.
fn named(select: &pg::SelectStmt) -> Result<Parsed> {
    Ok(Parsed::Renamed {
        inner: Box::new(query(select)?),
        labels: vec![ast::VALUE.into(), ast::PRESENT.into()],
    })
}
