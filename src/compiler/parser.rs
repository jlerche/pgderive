use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(super) mod derived;
pub(super) mod expansion;
pub(super) mod expressions;
pub(super) mod lookup;
pub(super) mod navigation;
pub(super) mod partition;
pub(super) mod scalar;
use expressions::{aggregate, column, node, optional, target};

#[derive(Clone)]
pub(super) struct Name(pub(super) Vec<String>);
pub(super) struct Table {
    pub(super) name: Name,
    pub(super) alias: Option<String>,
}
pub(super) type Columns = Vec<(Name, String, crate::catalog::terminal::Transform)>;
pub(super) enum Parsed {
    Derived(derived::Derived),
    Lookup(lookup::Parsed),
    Grouped(Grouped),
    Partition(partition::Parsed),
    JoinProjection {
        left: Table,
        right: Table,
        keys: (Name, Name),
        columns: Columns,
        predicate: Option<super::syntax::Expr>,
    },
    Projection {
        source: Table,
        columns: Columns,
        predicate: Option<super::syntax::Expr>,
    },
}
pub(super) struct Grouped {
    pub(super) group: Name,
    pub(super) sum: Name,
    pub(super) left: Table,
    pub(super) right: Table,
    pub(super) keys: (Name, Name),
    pub(super) predicates: Option<super::syntax::Expr>,
    pub(super) grouping: Name,
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
    validate_query(select)?;
    if let Some(lookup) = lookup::parse(select)? {
        return Ok(Parsed::Lookup(lookup));
    }
    if partition::eligible(select)? {
        return Ok(Parsed::Partition(partition::parse(select)?));
    }
    if let Some(derived) = derived::parse(select)? {
        return Ok(Parsed::Derived(derived));
    }
    let pg::SelectStmt { target_list, from_clause, where_clause, group_clause, .. } = select;
    if group_clause.is_empty() {
        return projection(target_list, from_clause, where_clause.as_deref());
    }
    grouped_query(select)
}
fn grouped_query(select: &pg::SelectStmt) -> Result<Parsed> {
    let pg::SelectStmt { target_list, from_clause, where_clause, group_clause, .. } = select;
    let [group, count, sum] = target_list.as_slice() else {
        anyhow::bail!("outputs must be group column, COUNT(*), SUM(column)");
    };
    let [grouping] = group_clause.as_slice() else {
        anyhow::bail!("exactly one GROUP BY column required");
    };
    let [from] = from_clause.as_slice() else {
        anyhow::bail!("exactly one two-table join required");
    };
    aggregate(target(count)?, "count", true)?;
    let sum = aggregate(target(sum)?, "sum", false)?;
    let (left, right, keys) = join(from)?;
    Ok(Parsed::Grouped(Grouped {
        group: column(target(group)?)?,
        sum: column(sum.context("SUM argument absent")?)?,
        left,
        right,
        keys,
        predicates: super::syntax::predicate(where_clause.as_deref())?,
        grouping: column(grouping)?,
    }))
}

type Joined = (Table, Table, (Name, Name));
fn join(from: &Node) -> Result<Joined> {
    let NodeEnum::JoinExpr(join) = node(from)? else {
        anyhow::bail!("FROM requires an inner equijoin");
    };
    let pg::JoinExpr {
        jointype,
        is_natural,
        larg,
        rarg,
        using_clause,
        join_using_alias,
        quals,
        alias,
        rtindex,
    } = join.as_ref();
    ensure!(
        *jointype == i32::from(pg::JoinType::JoinInner)
            && !is_natural
            && using_clause.is_empty()
            && join_using_alias.is_none()
            && alias.is_none()
            && *rtindex == 0,
        "unsupported join modifier"
    );
    let NodeEnum::AExpr(eq) = node(optional(quals.as_deref())?)? else {
        anyhow::bail!("ON requires column = column");
    };
    let pg::AExpr { kind, name, lexpr, rexpr, location: _ } = eq.as_ref();
    ensure!(
        *kind == i32::from(pg::AExprKind::AexprOp) && expressions::names(name)?.0 == ["="],
        "ON requires ordinary equality"
    );
    Ok((
        table(optional(larg.as_deref())?)?,
        table(optional(rarg.as_deref())?)?,
        (column(optional(lexpr.as_deref())?)?, column(optional(rexpr.as_deref())?)?),
    ))
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

fn projection(targets: &[Node], from: &[Node], predicate: Option<&Node>) -> Result<Parsed> {
    ensure!(!targets.is_empty() && targets.len() <= 64, "projection requires 1..=64 columns");
    let [source] = from else {
        anyhow::bail!("projection requires one source table");
    };
    let columns = projection_columns(targets)?;
    let predicate = super::syntax::predicate(predicate)?;
    if matches!(node(source)?, NodeEnum::JoinExpr(_)) {
        let (left, right, keys) = join(source)?;
        Ok(Parsed::JoinProjection { left, right, keys, columns, predicate })
    } else {
        Ok(Parsed::Projection { source: table(source)?, columns, predicate })
    }
}
fn projection_columns(targets: &[Node]) -> Result<Columns> {
    ensure!((1..=64).contains(&targets.len()), "projection requires 1..=64 columns");
    targets
        .iter()
        .map(|value| {
            let (name, transform, default_label) = projection_target(target(value)?)?;
            let NodeEnum::ResTarget(value) = node(value)? else {
                anyhow::bail!("expected output target");
            };
            let label = if value.name.is_empty() { default_label } else { value.name.clone() };
            Ok((name, label, transform))
        })
        .collect()
}

fn projection_target(value: &Node) -> Result<(Name, crate::catalog::terminal::Transform, String)> {
    use crate::catalog::terminal::Transform;
    if let NodeEnum::FuncCall(call) = node(value)? {
        let name = expressions::names(&call.funcname)?.0;
        let function = match name.as_slice() {
            [name] => name,
            [schema, name] if schema == "pg_catalog" => name,
            _ => anyhow::bail!("unsupported terminal function namespace"),
        };
        let transform = match function.as_str() {
            "abs" => Transform::Abs,
            "length" => Transform::Length,
            _ => anyhow::bail!("unsupported terminal function"),
        };
        // Reuse the exact aggregate-call modifier checks with one ordinary argument.
        let mut plain = call.as_ref().clone();
        plain.funcname = vec![pg_query::Node {
            node: Some(NodeEnum::String(pg::String { sval: function.clone() })),
        }];
        let normalized = Node { node: Some(NodeEnum::FuncCall(Box::new(plain))) };
        let operand =
            aggregate(&normalized, function, false)?.context("missing function operand")?;
        return Ok((column(operand)?, transform, function.clone()));
    }
    let name = column(value)?;
    let label = name.0.last().context("missing column name")?.clone();
    Ok((name, Transform::Identity, label))
}
