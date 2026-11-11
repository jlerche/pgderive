//! Bound IN marker inputs use the same typed scalar and tagged join infrastructure.
use super::{Builder, Column, Edge, keys, resolve};
use crate::compiler::{
    ColumnRef, Predicate,
    expression::Expr,
    parser::{Name, Parsed},
    relational::Node,
    scalar,
    subquery::Mark,
    syntax::Compare,
    typed,
};
use anyhow::{Result, ensure};
use pg_query::Node as Ast;
impl Builder<'_> {
    pub(super) fn mark(
        &mut self,
        mut left: Edge,
        parsed: Box<Parsed>,
        arguments: (&Ast, String, Option<&Ast>, bool),
    ) -> Result<Edge> {
        let (probe, alias, condition, semijoin) = arguments;
        let resolve_left = |name: &Name| Ok(resolve(name, &left.columns)?.value.clone());
        let operand = typed::operand(probe, &resolve_left)?;
        let right = self.derived(parsed, alias.clone())?;
        ensure!(
            condition.is_some() || right.columns.len() == 1,
            "IN subquery requires exactly one output"
        );
        let value = right.columns[0].value.clone();
        ensure!(
            matches!(value.oid, 16 | 20 | 21 | 23 | 25 | 1043 | 1082 | 1700 | 2950 | 1114 | 1184),
            "unsupported IN native type"
        );
        let probe = operand.coerce(value.oid)?;
        if matches!(probe.oid(), 25 | 1043) {
            let collation = probe.collation(&|value| {
                Ok(left
                    .columns
                    .iter()
                    .find(|column| column.value.name == value.name)
                    .map_or(0, |column| column.collation))
            })?;
            ensure!(
                collation != 0 && right.columns[0].collation != 0,
                "IN requires resolved text collations"
            );
            typed::collation::common(collation, right.columns[0].collation)?;
        }
        let mut computed = Vec::new();
        let probe = match probe.as_column() {
            Some(column) => column.clone(),
            None => {
                scalar::register(probe, &mut computed, &format!("@mark_{}_probe_", self.joins))?
            }
        };
        self.map(&mut left, computed, &format!("mark_{}_probe", self.joins));
        let comparison = Predicate::new(Expr::Compare(
            Compare::Eq,
            typed::Typed::column(probe.clone()),
            typed::Typed::column(value.clone()),
        ));
        let columns = left.columns.clone();
        let id = format!("tree_join_{}", self.joins);
        let bound = keys::condition(condition, &left.columns, &right.columns, &id)?;
        let edge = self.join(left, right, bound, crate::compiler::left_join::Kind::Left)?;
        let Some(Node::LeftJoin { id, input, spec }) = self.nodes.pop() else {
            anyhow::bail!("missing marker tagged join");
        };
        let output = ColumnRef {
            name: format!("@{id}_membership"),
            right: false,
            oid: 16,
            nullable: true,
            collation: 0,
            modifier: None,
        };
        self.nodes.push(Node::Mark {
            id: id.clone(),
            input,
            spec: Box::new(Mark {
                probe,
                value,
                output: output.clone(),
                comparison,
                correlation: spec.predicate,
                semijoin,
            }),
        });
        let mut columns = columns;
        columns.push(Column {
            label: "\0value".into(),
            qualifiers: vec![alias],
            value: output,
            collation: 0,
            modifier: -1,
            unqualified: true,
        });
        ensure!(edge.id == id, "marker join edge differs");
        Ok(Edge { id, columns })
    }
}
