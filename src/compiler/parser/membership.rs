//! Narrow native DISTINCT and equality-correlated existence composition.
use super::{
    Parsed,
    native_join::{self, Tree},
    node, optional,
};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(super) fn parse(select: &pg::SelectStmt) -> Result<Option<Parsed>> {
    let distinct = !select.distinct_clause.is_empty();
    if distinct {
        ensure!(
            select.distinct_clause.len() == 1 && select.distinct_clause[0].node.is_none(),
            "DISTINCT ON unsupported"
        );
    }
    let existence = select.where_clause.as_deref().map(exists).transpose()?.flatten();
    if !distinct && existence.is_none() {
        return Ok(None);
    }
    let mut checked = select.clone();
    checked.distinct_clause.clear();
    super::validate_query(&checked)?;
    ensure!(select.group_clause.is_empty(), "membership grouping unsupported");
    let [source] = select.from_clause.as_slice() else {
        anyhow::bail!("membership requires one native source")
    };
    let mut tree = Tree::Source(super::table(source)?);
    let predicate = if let Some((query, anti)) = existence {
        super::validate_query(query)?;
        ensure!(query.group_clause.is_empty(), "EXISTS grouping unsupported");
        let [right] = query.from_clause.as_slice() else {
            anyhow::bail!("EXISTS requires one native source")
        };
        let mut keys = Vec::new();
        native_join::computed_equalities(optional(query.where_clause.as_deref())?, &mut keys, 0)?;
        ensure!((1..=16).contains(&keys.len()), "EXISTS correlation key budget exceeded");
        ensure!(query.target_list.len() == 1, "EXISTS requires SELECT 1");
        let NodeEnum::AConst(value) = node(super::target(&query.target_list[0])?)? else {
            anyhow::bail!("EXISTS requires SELECT 1")
        };
        ensure!(
            matches!(&value.val, Some(pg::a_const::Val::Ival(value)) if value.ival == 1),
            "EXISTS requires SELECT 1"
        );
        tree = Tree::Exists { left: Box::new(tree), right: super::table(right)?, keys, anti };
        None
    } else {
        crate::compiler::syntax::predicate(select.where_clause.as_deref())?
    };
    Ok(Some(Parsed::Membership {
        parsed: native_join::Parsed {
            tree,
            columns: super::projection_columns(&select.target_list)?,
            predicate,
        },
        distinct,
    }))
}
fn exists(value: &Node) -> Result<Option<(&pg::SelectStmt, bool)>> {
    let (value, anti) = match node(value)? {
        NodeEnum::BoolExpr(value) if value.boolop == i32::from(pg::BoolExprType::NotExpr) => {
            let [argument] = value.args.as_slice() else { return Ok(None) };
            (argument, true)
        }
        _ => (value, false),
    };
    let NodeEnum::SubLink(value) = node(value)? else { return Ok(None) };
    ensure!(
        value.sub_link_type == i32::from(pg::SubLinkType::ExistsSublink)
            && value.testexpr.is_none()
            && value.oper_name.is_empty(),
        "only EXISTS sublinks supported"
    );
    let NodeEnum::SelectStmt(query) = node(optional(value.subselect.as_deref())?)? else {
        anyhow::bail!("EXISTS requires SELECT")
    };
    Ok(Some((query, anti)))
}
