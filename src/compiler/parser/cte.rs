//! Lexically expand pure nonrecursive WITH bindings before relational lowering.
use super::expressions::{names, node};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

struct Definition {
    order: usize,
    materialized: bool,
    query: pg::SelectStmt,
    columns: Vec<Node>,
    bytes: usize,
    height: usize,
    count: usize,
}
type Scope = BTreeMap<String, Arc<Definition>>;
pub(super) type Definitions = Vec<(pg::SelectStmt, Vec<String>)>;
#[derive(Default)]
struct Expansion {
    remaining: usize,
    selects: usize,
    validation: Definitions,
    declared: usize,
    references: Vec<usize>,
    materialized: BTreeSet<usize>,
    referenced: BTreeSet<usize>,
}

pub(super) fn expand(query: &mut pg::SelectStmt) -> Result<Definitions> {
    // PostgreSQL decides inlining by lexical reference count, known only after a full walk.
    let mut counting = Expansion { remaining: 1_048_576, ..Expansion::default() };
    counting.select(&mut query.clone(), &Scope::new(), 0)?;
    let at_least = |least: usize| -> BTreeSet<usize> {
        let counts = counting.references.iter().enumerate();
        counts.filter(|(_, count)| **count >= least).map(|(order, _)| order).collect()
    };
    let mut expansion = Expansion {
        remaining: 1_048_576,
        materialized: at_least(2),
        referenced: at_least(1),
        ..Expansion::default()
    };
    expansion.select(query, &Scope::new(), 0)?;
    Ok(expansion.validation)
}
impl Expansion {
    fn charge(&mut self, bytes: usize) -> Result<()> {
        self.remaining =
            self.remaining.checked_sub(bytes).context("CTE expansion byte budget exceeded")?;
        Ok(())
    }
    fn select(
        &mut self,
        select: &mut pg::SelectStmt,
        inherited: &Scope,
        depth: usize,
    ) -> Result<()> {
        self.selects += 1;
        ensure!(depth < 64 && self.selects <= 128, "CTE/query expansion depth or count exceeded");
        let mut scope = inherited.clone();
        if let Some(with) = select.with_clause.take() {
            ensure!(!with.recursive, "recursive WITH unsupported");
            ensure!(with.ctes.len() <= 64, "WITH exceeds 64 bindings");
            let mut declared = BTreeSet::new();
            for cte in with.ctes {
                let NodeEnum::CommonTableExpr(cte) = node(&cte)? else {
                    anyhow::bail!("invalid WITH binding")
                };
                ensure!(declared.insert(cte.ctename.clone()), "duplicate WITH binding");
                ensure!(
                    cte.search_clause.is_none() && cte.cycle_clause.is_none(),
                    "recursive WITH clauses unsupported"
                );
                ensure!(cte.aliascolnames.len() <= 64, "CTE column alias budget exceeded");
                names(&cte.aliascolnames)?;
                let NodeEnum::SelectStmt(query) =
                    node(cte.ctequery.as_deref().context("missing WITH query")?)?
                else {
                    anyhow::bail!("WITH requires a read-only SELECT")
                };
                let mut query = (**query).clone();
                self.select(&mut query, &scope, depth + 1)?;
                let order = self.declared;
                self.declared += 1;
                self.references.push(0);
                let materialization = pg::CteMaterialize::try_from(cte.ctematerialized)?;
                let materialized = materialization == pg::CteMaterialize::Always
                    || (materialization != pg::CteMaterialize::Never
                        && self.materialized.contains(&order));
                let bytes = serde_json::to_vec(&query)?.len();
                self.charge(bytes)?;
                // A referenced materialized body is planned even if no reader of it ever runs.
                let mut validated = query.clone();
                if materialized && self.referenced.contains(&order) {
                    validated.with_clause = Some(pg::WithClause::default());
                }
                self.validation.push((validated, names(&cte.aliascolnames)?.0));
                let (height, count) = shape(&query);
                scope.insert(
                    cte.ctename.clone(),
                    Arc::new(Definition {
                        order,
                        materialized,
                        query,
                        columns: cte.aliascolnames.clone(),
                        bytes,
                        height,
                        count,
                    }),
                );
            }
        }
        self.visit_select_body(select, &scope, depth)?;
        let (height, count) = shape(select);
        ensure!(height < 64 && count <= 128, "expanded CTE scope budget exceeded");
        Ok(())
    }
    fn visit_select_body(
        &mut self,
        select: &mut pg::SelectStmt,
        scope: &Scope,
        depth: usize,
    ) -> Result<()> {
        for value in select
            .from_clause
            .iter_mut()
            .chain(&mut select.target_list)
            .chain(&mut select.group_clause)
            .chain(&mut select.sort_clause)
            .chain(&mut select.values_lists)
        {
            self.visit(value, scope, depth + 1)?;
        }
        for value in [
            &mut select.where_clause,
            &mut select.having_clause,
            &mut select.limit_count,
            &mut select.limit_offset,
        ]
        .into_iter()
        .flatten()
        {
            self.visit(value, scope, depth + 1)?;
        }
        for child in [&mut select.larg, &mut select.rarg].into_iter().flatten() {
            self.select(child, scope, depth + 1)?;
        }
        Ok(())
    }
    fn replace_reference(&mut self, value: &mut Node, scope: &Scope, depth: usize) -> Result<bool> {
        if let Some(NodeEnum::RangeVar(range)) = &value.node
            && range.schemaname.is_empty()
            && range.catalogname.is_empty()
            && let Some(definition) = scope.get(&range.relname)
        {
            ensure!(range.inh, "ONLY cannot reference a CTE");
            self.references[definition.order] += 1;
            ensure!(depth + definition.height < 64, "expanded CTE depth budget exceeded");
            self.selects = self
                .selects
                .checked_add(definition.count)
                .context("CTE expansion scope overflow")?;
            ensure!(self.selects <= 128, "expanded CTE scope budget exceeded");
            self.charge(definition.bytes)?; // Charge before cloning an expanded body.
            let mut alias = range.alias.clone().unwrap_or_else(|| pg::Alias {
                aliasname: range.relname.clone(),
                ..pg::Alias::default()
            });
            let mut columns = definition.columns.clone();
            for (index, column) in alias.colnames.into_iter().enumerate() {
                if let Some(prior) = columns.get_mut(index) {
                    *prior = column;
                } else {
                    columns.push(column);
                }
            }
            alias.colnames = columns;
            value.node = Some(NodeEnum::RangeSubselect(Box::new(pg::RangeSubselect {
                alias: Some(alias),
                subquery: Some(Box::new(Node {
                    node: Some(NodeEnum::SelectStmt(Box::new(reference(definition)))),
                })),
                lateral: false,
            })));
            return Ok(true);
        }
        Ok(false)
    }
    fn visit(&mut self, value: &mut Node, scope: &Scope, depth: usize) -> Result<()> {
        ensure!(depth < 64, "CTE expression depth exceeded");
        if self.replace_reference(value, scope, depth)? {
            return Ok(());
        }
        match &mut value.node {
            Some(NodeEnum::SelectStmt(select)) => self.select(select, scope, depth),
            Some(NodeEnum::RangeSubselect(select)) => self.visit(
                select.subquery.as_deref_mut().context("missing derived query")?,
                scope,
                depth + 1,
            ),
            Some(NodeEnum::JoinExpr(join)) => {
                for child in [&mut join.larg, &mut join.rarg, &mut join.quals].into_iter().flatten()
                {
                    self.visit(child, scope, depth + 1)?;
                }
                Ok(())
            }
            Some(NodeEnum::ResTarget(target)) => {
                self.optional(target.val.as_deref_mut(), scope, depth)
            }
            Some(NodeEnum::SubLink(link)) => {
                self.optional(link.subselect.as_deref_mut(), scope, depth)?;
                self.optional(link.testexpr.as_deref_mut(), scope, depth)
            }
            // Expression subqueries are already fail-closed outside these ordinary containers.
            Some(NodeEnum::AExpr(expr)) => {
                self.optional(expr.lexpr.as_deref_mut(), scope, depth)?;
                self.optional(expr.rexpr.as_deref_mut(), scope, depth)
            }
            Some(NodeEnum::BoolExpr(expr)) => self.list(&mut expr.args, scope, depth),
            Some(NodeEnum::List(expr)) => self.list(&mut expr.items, scope, depth),
            Some(NodeEnum::FuncCall(expr)) => {
                self.list(&mut expr.args, scope, depth)?;
                self.optional(expr.agg_filter.as_deref_mut(), scope, depth)
            }
            Some(NodeEnum::CaseExpr(expr)) => {
                self.optional(expr.arg.as_deref_mut(), scope, depth)?;
                self.optional(expr.defresult.as_deref_mut(), scope, depth)?;
                self.list(&mut expr.args, scope, depth)
            }
            Some(NodeEnum::CaseWhen(expr)) => {
                self.optional(expr.expr.as_deref_mut(), scope, depth)?;
                self.optional(expr.result.as_deref_mut(), scope, depth)
            }
            Some(NodeEnum::CoalesceExpr(expr)) => self.list(&mut expr.args, scope, depth),
            Some(NodeEnum::MinMaxExpr(expr)) => self.list(&mut expr.args, scope, depth),
            Some(NodeEnum::TypeCast(expr)) => self.optional(expr.arg.as_deref_mut(), scope, depth),
            Some(NodeEnum::CollateClause(expr)) => {
                self.optional(expr.arg.as_deref_mut(), scope, depth)
            }
            Some(NodeEnum::NullTest(expr)) => self.optional(expr.arg.as_deref_mut(), scope, depth),
            Some(NodeEnum::BooleanTest(expr)) => {
                self.optional(expr.arg.as_deref_mut(), scope, depth)
            }
            Some(NodeEnum::SortBy(expr)) => self.optional(expr.node.as_deref_mut(), scope, depth),
            _ => Ok(()),
        }
    }
    fn optional(&mut self, value: Option<&mut Node>, scope: &Scope, depth: usize) -> Result<()> {
        if let Some(value) = value {
            self.visit(value, scope, depth + 1)?;
        }
        Ok(())
    }
    fn list(&mut self, values: &mut [Node], scope: &Scope, depth: usize) -> Result<()> {
        for value in values {
            self.visit(value, scope, depth + 1)?;
        }
        Ok(())
    }
}

/// An empty WITH marks an inlined body as materialized; expansion consumed every real WITH.
fn reference(definition: &Definition) -> pg::SelectStmt {
    let mut query = definition.query.clone();
    if definition.materialized {
        query.with_clause = Some(pg::WithClause::default());
    }
    query
}
fn combined(children: impl Iterator<Item = (usize, usize)>) -> (usize, usize) {
    children.fold((0, 0), |(height, count), next| (height.max(next.0), count + next.1))
}
fn shape(select: &pg::SelectStmt) -> (usize, usize) {
    let fields = select
        .from_clause
        .iter()
        .chain(&select.target_list)
        .chain(&select.group_clause)
        .chain(&select.sort_clause)
        .chain(&select.values_lists);
    let optional =
        [&select.where_clause, &select.having_clause, &select.limit_count, &select.limit_offset]
            .into_iter()
            .flatten()
            .map(Box::as_ref);
    let (height, count) =
        combined(fields.chain(optional).map(node_shape).chain(
            [select.larg.as_deref(), select.rarg.as_deref()].into_iter().flatten().map(shape),
        ));
    (height + 1, count + 1)
}
fn node_shape(value: &Node) -> (usize, usize) {
    match &value.node {
        Some(NodeEnum::SelectStmt(value)) => shape(value),
        Some(NodeEnum::RangeSubselect(value)) => {
            value.subquery.as_deref().map_or((0, 0), node_shape)
        }
        Some(NodeEnum::ResTarget(value)) => value.val.as_deref().map_or((0, 0), node_shape),
        Some(NodeEnum::SubLink(value)) => combined(
            [&value.subselect, &value.testexpr].into_iter().flatten().map(|v| node_shape(v)),
        ),
        Some(NodeEnum::JoinExpr(value)) => combined(
            [&value.larg, &value.rarg, &value.quals].into_iter().flatten().map(|v| node_shape(v)),
        ),
        Some(NodeEnum::AExpr(value)) => {
            combined([&value.lexpr, &value.rexpr].into_iter().flatten().map(|v| node_shape(v)))
        }
        Some(NodeEnum::BoolExpr(value)) => combined(value.args.iter().map(node_shape)),
        Some(NodeEnum::List(value)) => combined(value.items.iter().map(node_shape)),
        Some(NodeEnum::CoalesceExpr(value)) => combined(value.args.iter().map(node_shape)),
        Some(NodeEnum::MinMaxExpr(value)) => combined(value.args.iter().map(node_shape)),
        Some(NodeEnum::FuncCall(value)) => {
            combined(value.args.iter().chain(value.agg_filter.as_deref()).map(node_shape))
        }
        Some(NodeEnum::CaseExpr(value)) => combined(
            value
                .args
                .iter()
                .chain(value.arg.as_deref())
                .chain(value.defresult.as_deref())
                .map(node_shape),
        ),
        Some(NodeEnum::CaseWhen(value)) => {
            combined([&value.expr, &value.result].into_iter().flatten().map(|v| node_shape(v)))
        }
        Some(NodeEnum::TypeCast(value)) => value.arg.as_deref().map_or((0, 0), node_shape),
        Some(NodeEnum::CollateClause(value)) => value.arg.as_deref().map_or((0, 0), node_shape),
        Some(NodeEnum::NullTest(value)) => value.arg.as_deref().map_or((0, 0), node_shape),
        Some(NodeEnum::BooleanTest(value)) => value.arg.as_deref().map_or((0, 0), node_shape),
        Some(NodeEnum::SortBy(value)) => value.node.as_deref().map_or((0, 0), node_shape),
        _ => (0, 0),
    }
}
