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
    if existence.is_some() {
        checked.where_clause = None;
    }
    let mut parsed = super::partition::parse(&checked)?;
    // Keep the graph/column identities of existing column-only registrations.
    if let Ok(columns) = super::projection_columns(&select.target_list) {
        parsed.targets = columns
            .into_iter()
            .map(|(name, label)| super::partition::Target {
                value: super::partition::Value::Column(name),
                label,
            })
            .collect();
    }
    if existence.is_none() && parsed.expansion.is_none() {
        parsed.source = match parsed.source {
            super::partition::Source::Native(table) => {
                super::partition::Source::Join(Tree::Source(table))
            }
            other => other,
        };
    }

    if let Some((query, anti)) = existence {
        parsed.source = super::partition::Source::Join(existence_source(select, query, anti)?);
    }
    Ok(Some(Parsed::Membership { parsed, distinct }))
}
fn existence_source(select: &pg::SelectStmt, query: &pg::SelectStmt, anti: bool) -> Result<Tree> {
    super::validate_query(query)?;
    ensure!(query.group_clause.is_empty(), "EXISTS grouping unsupported");
    let [source] = select.from_clause.as_slice() else {
        anyhow::bail!("membership requires one native source")
    };
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
    Ok(Tree::Exists {
        left: Box::new(Tree::Source(super::table(source)?)),
        right: super::table(right)?,
        keys,
        anti,
    })
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
