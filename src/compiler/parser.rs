use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(super) mod derived;
pub(super) mod expansion;
pub(super) mod expressions;
pub(super) mod lookup;
mod membership;
pub(super) mod native_join;
pub(super) mod navigation;
pub(super) mod offset;
pub(super) mod partition;
pub(super) mod scalar;
pub(super) mod scoped_join;
use expressions::{column, node, optional, target};

#[derive(Clone)]
pub(super) struct Name(pub(super) Vec<String>);
#[derive(Clone)]
pub(super) struct Table {
    pub(super) name: Name,
    pub(super) alias: Option<String>,
}
pub(super) type Columns = Vec<(Name, String)>;
pub(super) enum Parsed {
    Membership { parsed: partition::Parsed, distinct: bool },
    ScopedJoin(scoped_join::Parsed),
    Lookup(lookup::Parsed),
    Partition(partition::Parsed),
}

pub(super) fn parse(sql: &str) -> Result<Parsed> {
    ensure!(sql.len() <= 16_384 && !sql.contains('\0'), "SQL exceeds size limit or contains NUL");
    scan_budget(sql)?;
    let parsed = pg_query::parse(sql).context("SQL parse (PostgreSQL 17.7)")?;
    ensure!(parsed.protobuf.version == 170_007, "unexpected PostgreSQL parser version");
    let [statement] = parsed.protobuf.stmts.as_slice() else {
        anyhow::bail!("SQL requires exactly one statement");
    };
    let NodeEnum::SelectStmt(select) = node(statement.stmt.as_ref().context("empty statement")?)?
    else {
        anyhow::bail!("SQL requires SELECT");
    };
    query(select).context("SQL lower: unsupported SELECT")
}

// Bound native parser recursion before parsing, using PostgreSQL's scanner so
// parentheses inside comments, strings and quoted identifiers do not count.
fn scan_budget(sql: &str) -> Result<()> {
    let scanned = pg_query::scan(sql).context("SQL scan")?;
    ensure!(scanned.tokens.len() <= 2_048, "SQL exceeds token budget");
    let mut depth = 0_u32;
    for token in scanned.tokens {
        if token.token == i32::from(pg::Token::Ascii40) {
            depth += 1;
            ensure!(depth <= 64, "SQL exceeds nesting budget");
        } else if token.token == i32::from(pg::Token::Ascii41) {
            depth = depth.saturating_sub(1);
        }
    }
    Ok(())
}

fn validate_query(select: &pg::SelectStmt) -> Result<()> {
    let pg::SelectStmt {
        distinct_clause,
        into_clause,
        target_list: _,
        from_clause: _,
        where_clause: _,
        group_clause: _,
        group_distinct,
        having_clause,
        window_clause,
        values_lists,
        sort_clause,
        limit_offset,
        limit_count,
        limit_option,
        locking_clause,
        with_clause,
        op,
        all,
        larg,
        rarg,
    } = select;
    ensure!(
        distinct_clause.is_empty()
            && into_clause.is_none()
            && !group_distinct
            && having_clause.is_none()
            && window_clause.is_empty()
            && values_lists.is_empty()
            && sort_clause.is_empty()
            && limit_offset.is_none()
            && limit_count.is_none()
            && *limit_option == i32::from(pg::LimitOption::Default)
            && locking_clause.is_empty()
            && with_clause.is_none()
            && *op == i32::from(pg::SetOperation::SetopNone)
            && !all
            && larg.is_none()
            && rarg.is_none(),
        "unsupported SELECT clause"
    );
    Ok(())
}
fn query(select: &pg::SelectStmt) -> Result<Parsed> {
    if let Some(parsed) = membership::parse(select)? {
        return Ok(parsed);
    }
    validate_query(select)?;
    if let Some(join) = scoped_join::parse(select)? {
        return Ok(Parsed::ScopedJoin(join));
    }
    if let Some(lookup) = lookup::parse(select)? {
        return Ok(Parsed::Lookup(lookup));
    }
    partition::parse(select).map(Parsed::Partition)
}

fn table(value: &Node) -> Result<Table> {
    let NodeEnum::RangeVar(range) = node(value)? else {
        anyhow::bail!("join inputs must be named tables");
    };
    let pg::RangeVar { catalogname, schemaname, relname, inh, relpersistence, alias, location: _ } =
        range;
    ensure!(
        catalogname.is_empty() && !schemaname.is_empty() && *inh && relpersistence == "p",
        "FROM requires schema.table without ONLY or database qualifier"
    );
    let alias = alias
        .as_ref()
        .map(|alias| {
            ensure!(alias.colnames.is_empty(), "column aliases unsupported");
            Ok(alias.aliasname.clone())
        })
        .transpose()?;
    Ok(Table { name: Name(vec![schemaname.clone(), relname.clone()]), alias })
}

fn projection_columns(targets: &[Node]) -> Result<Columns> {
    ensure!((1..=64).contains(&targets.len()), "projection requires 1..=64 columns");
    targets
        .iter()
        .map(|value| {
            let (name, default_label) = projection_target(target(value)?)?;
            let NodeEnum::ResTarget(value) = node(value)? else {
                anyhow::bail!("expected output target");
            };
            let label = if value.name.is_empty() { default_label } else { value.name.clone() };
            Ok((name, label))
        })
        .collect()
}

fn projection_target(value: &Node) -> Result<(Name, String)> {
    let name = column(value)?;
    let label = name.0.last().context("missing column name")?.clone();
    Ok((name, label))
}
