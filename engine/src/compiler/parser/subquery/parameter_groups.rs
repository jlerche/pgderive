//! Group inner rows after applying the predicates for each distinct outer tuple.
use super::{
    ast,
    correlation::{Correlation, Residual, ResidualComparison},
    node, scalar_input,
};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

pub(super) const ALIAS: &str = "\0group_parameters";

pub(super) fn rewrite(
    outer: &pg::SelectStmt,
    inner: &mut pg::SelectStmt,
    correlation: Correlation,
) -> Result<Correlation> {
    ensure!(
        inner.distinct_clause.iter().all(|key| key.node.is_none()),
        "grouped residual correlation disallows DISTINCT ON"
    );
    scalar_input::validate(inner, scalar_input::Input::Quantified)?;
    let [source] = outer.from_clause.as_slice() else {
        anyhow::bail!("grouped residual correlation requires one outer native table");
    };
    ensure!(
        matches!(node(source)?, NodeEnum::RangeVar(_)),
        "grouped residual correlation requires one outer native table"
    );
    let mut domain = Domain::default();
    let mut keys = Correlation::default();
    let mut conditions = Vec::new();
    for (outer, inner) in correlation.pairs {
        ensure!(
            scalar_input::column(&outer) && scalar_input::column(&inner),
            "grouped correlation requires column equality keys"
        );
        let parameter = domain.add(outer.clone());
        conditions.push(ast::comparison(parameter.clone(), inner, "="));
        keys.pairs.push((outer, parameter));
    }
    for residual in correlation.residuals {
        conditions.push(domain.condition(residual, &mut keys.residuals)?);
    }
    inner.group_clause.extend(domain.parameters().skip(keys.pairs.len()));
    inner.from_clause = vec![Node {
        node: Some(NodeEnum::JoinExpr(Box::new(pg::JoinExpr {
            jointype: i32::from(pg::JoinType::JoinInner),
            larg: inner.from_clause.pop().map(Box::new),
            rarg: Some(Box::new(domain.relation(source.clone()))),
            quals: ast::logical(conditions, true),
            ..pg::JoinExpr::default()
        }))),
    }];
    // Equality parameters are appended by the existing membership lowering.
    Ok(keys)
}

#[derive(Default)]
pub(super) struct Domain {
    targets: Vec<Node>,
    names: Vec<String>,
}

impl Domain {
    pub(super) fn add(&mut self, expression: Node) -> Node {
        let name = format!("\0parameter_{}", self.targets.len());
        self.targets.push(ast::target(expression, &name));
        let column = ast::column(ALIAS, &name);
        self.names.push(name);
        column
    }

    fn parameters(&self) -> impl Iterator<Item = Node> {
        self.names.iter().map(|name| ast::column(ALIAS, name))
    }

    fn condition(&mut self, term: Residual, matchback: &mut Vec<Residual>) -> Result<Node> {
        match term {
            Residual::Comparison(term) => {
                ensure!(
                    scalar_input::column(&term.inner),
                    "grouped residual correlation requires a native inner column"
                );
                let parameter = self.add(term.outer.clone());
                matchback.push(Residual::Comparison(Box::new(ResidualComparison {
                    outer: term.outer,
                    inner: parameter.clone(),
                    operator: "=".into(),
                    kind: pg::AExprKind::AexprNotDistinct,
                    outer_first: true,
                })));
                let (left, right) = if term.outer_first {
                    (parameter, term.inner)
                } else {
                    (term.inner, parameter)
                };
                Ok(ast::comparison_with_kind(left, right, &term.operator, term.kind))
            }
            Residual::Logical { terms, kind } => {
                let args = terms
                    .into_iter()
                    .map(|term| self.condition(term, matchback))
                    .collect::<Result<Vec<_>>>()?;
                Ok(ast::boolean_expression(args, kind))
            }
        }
    }

    pub(super) fn relation(self, source: Node) -> Node {
        let select = pg::SelectStmt {
            distinct_clause: vec![Node { node: None }],
            target_list: self.targets,
            from_clause: vec![source],
            op: i32::from(pg::SetOperation::SetopNone),
            limit_option: i32::from(pg::LimitOption::Default),
            ..pg::SelectStmt::default()
        };
        Node {
            node: Some(NodeEnum::RangeSubselect(Box::new(pg::RangeSubselect {
                lateral: false,
                subquery: Some(Box::new(Node {
                    node: Some(NodeEnum::SelectStmt(Box::new(select))),
                })),
                alias: Some(pg::Alias { aliasname: ALIAS.into(), colnames: Vec::new() }),
            }))),
        }
    }
}
