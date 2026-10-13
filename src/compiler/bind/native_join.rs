//! Resolve native source occurrences into an executable join tree.
use super::{Compiled, scope};
use crate::{
    compiler::{
        ColumnRef, Predicate, Program,
        parser::{
            Name,
            native_join::{Parsed, Tree},
        },
        relational::{Node, Relational, Source, field},
    },
    source::Contract,
};
use anyhow::{Context, Result, ensure};
#[derive(Clone)]
struct Column {
    label: String,
    qualifiers: Vec<String>,
    value: ColumnRef,
    modifier: i32,
    collation: u32,
}
struct Edge {
    id: String,
    columns: Vec<Column>,
}
struct Builder<'a> {
    contract: &'a Contract,
    sources: Vec<Source>,
    nodes: Vec<Node>,
    joins: usize,
    revisions: Vec<String>,
}
pub(super) fn bind(parsed: Parsed, contract: &Contract) -> Result<Compiled> {
    let mut builder = Builder {
        contract,
        sources: Vec::new(),
        nodes: Vec::new(),
        joins: 0,
        revisions: Vec::new(),
    };
    let edge = builder.tree(parsed.tree)?;
    let resolve = |name: &Name| Ok(resolve(name, &edge.columns)?.value.clone());
    let (columns, terminal) = super::relational::output(parsed.columns, &resolve)?;
    let input = if let Some(predicate) = parsed.predicate {
        builder.nodes.push(Node::Filter {
            id: "tree_filter".into(),
            input: edge.id.clone(),
            predicate: Predicate::new(crate::compiler::expression::bind(predicate, &resolve)?),
        });
        "tree_filter".into()
    } else {
        edge.id
    };
    builder.nodes.push(Node::Output { id: "project".into(), input });
    let first = builder.sources.first().context("missing join source")?;
    let output = crate::compiler::Projected {
        schema: first.schema.clone(),
        table: first.table.clone(),
        columns,
        terminal,
    };
    let revision = format!(
        "sql-native-join-tree-v1:source-occurrences-v1:compound-native-key-v1:retained-left-join-v1:pg-query-6.2.1:pg-17.7:raw-record-v1:json-v2:{}",
        builder.revisions.join(":")
    );
    Ok(Compiled {
        program: Program::Relational {
            relational: Relational { sources: builder.sources, nodes: builder.nodes, output },
        },
        predicates: None,
        revision: Some(revision),
    })
}
impl Builder<'_> {
    fn tree(&mut self, tree: Tree) -> Result<Edge> {
        match tree {
            Tree::Source(table) => self.source(table),
            Tree::Derived { source, alias } => self.derived(source, alias),
            Tree::Join { left, right, keys, outer } => {
                let left = self.tree(*left)?;
                let right = self.tree(*right)?;
                self.join(left, right, keys, outer)
            }
        }
    }
    fn derived(
        &mut self,
        source: Box<crate::compiler::parser::Parsed>,
        alias: String,
    ) -> Result<Edge> {
        let offset = self.sources.len();
        let prefix = format!("tree_scope_{offset}_");
        let scope = super::scoped_join::scope((source, alias), &prefix, offset, self.contract)?;
        let columns = scope
            .ir
            .output
            .columns
            .iter()
            .map(|output| Column {
                label: output.label.clone(),
                qualifiers: vec![scope.alias.clone()],
                value: output.column.clone(),
                modifier: -1,
                collation: 0,
            })
            .collect();
        self.revisions.push(scope.revision);
        self.sources.extend(scope.ir.sources);
        ensure!(self.sources.len() <= 16, "join source occurrence limit exceeded");
        self.nodes.extend(scope.ir.nodes);
        Ok(Edge { id: format!("{prefix}visible"), columns })
    }
    fn source(&mut self, table: crate::compiler::parser::Table) -> Result<Edge> {
        let native = scope(table, self.contract)?;
        let index = self.sources.len();
        ensure!(index < 16, "join source occurrence limit exceeded");
        let qualifiers = native.table.alias.as_ref().map_or_else(
            || vec![native.relation.schema.clone(), native.relation.table.clone()],
            |alias| vec![alias.clone()],
        );
        let columns = native
            .relation
            .columns
            .iter()
            .map(|column| Column {
                label: column.name.clone(),
                qualifiers: qualifiers.clone(),
                value: ColumnRef {
                    name: field(index, &column.name),
                    right: false,
                    oid: column.oid,
                    nullable: column.nullable,
                },
                modifier: column.modifier,
                collation: column.collation,
            })
            .collect();
        self.sources.push(Source {
            schema: native.relation.schema.clone(),
            table: native.relation.table.clone(),
        });
        let id = format!("tree_source_{index}");
        self.nodes.push(Node::Source { id: id.clone(), source: index });
        Ok(Edge { id, columns })
    }
    fn join(
        &mut self,
        left: Edge,
        mut right: Edge,
        names: Vec<(Name, Name)>,
        outer: bool,
    ) -> Result<Edge> {
        ensure!(
            !left.columns.iter().any(|left| right
                .columns
                .iter()
                .any(|right| left.qualifiers.last() == right.qualifiers.last())),
            "duplicate exposed table qualifier"
        );
        let keys = keys(names, &left.columns, &right.columns)?;
        let id = format!("tree_join_{}", self.joins);
        self.joins += 1;
        let left_key = format!("{id}_left");
        let right_key = format!("{id}_right");
        if outer {
            self.nodes.push(Node::LeftJoinInput {
                id: left_key.clone(),
                input: left.id,
                spec: crate::compiler::left_join::Input {
                    left: true,
                    keys: keys.iter().map(|pair| pair.0.clone()).collect(),
                },
            });
            self.nodes.push(Node::LeftJoinInput {
                id: right_key.clone(),
                input: right.id,
                spec: crate::compiler::left_join::Input {
                    left: false,
                    keys: keys.into_iter().map(|pair| pair.1).collect(),
                },
            });
            let input = format!("{id}_tagged");
            self.nodes.push(Node::Union { id: input.clone(), left: left_key, right: right_key });
            self.nodes.push(Node::LeftJoin {
                id: id.clone(),
                input,
                spec: crate::compiler::left_join::Spec {
                    right: right.columns.iter().map(|value| value.value.clone()).collect(),
                },
            });
            for column in &mut right.columns {
                column.value.nullable = true;
            }
        } else {
            self.key(left_key.clone(), left.id, keys.iter().map(|pair| pair.0.clone()).collect());
            self.key(right_key.clone(), right.id, keys.into_iter().map(|pair| pair.1).collect());
            self.nodes.push(Node::Join { id: id.clone(), left: left_key, right: right_key });
        }
        let mut columns = left.columns;
        columns.extend(right.columns);
        Ok(Edge { id, columns })
    }
    fn key(&mut self, id: String, input: String, keys: Vec<ColumnRef>) {
        self.nodes.push(if keys.is_empty() {
            Node::CrossKey { id, input }
        } else {
            Node::EquijoinKey { id, input, keys }
        });
    }
}
fn resolve<'a>(name: &Name, columns: &'a [Column]) -> Result<&'a Column> {
    let mut matches = columns.iter().filter(|column| {
        Some(&column.label) == name.0.last()
            && match name.0.as_slice() {
                [_] => true,
                [qualifier, _] => column.qualifiers.last() == Some(qualifier),
                [schema, table, _] => {
                    column.qualifiers.as_slice() == [schema.clone(), table.clone()]
                }
                _ => false,
            }
    });
    let result = matches.next().context("unknown join-tree column")?;
    ensure!(matches.next().is_none(), "ambiguous join-tree column");
    Ok(result)
}
fn keys(
    names: Vec<(Name, Name)>,
    left: &[Column],
    right: &[Column],
) -> Result<Vec<(ColumnRef, ColumnRef)>> {
    let mut all = left.to_vec();
    all.extend_from_slice(right);
    let mut keys = names
        .into_iter()
        .map(|(a, b)| {
            let a = resolve(&a, &all)?;
            let b = resolve(&b, &all)?;
            let a_left = left.iter().any(|value| value.value.name == a.value.name);
            let b_left = left.iter().any(|value| value.value.name == b.value.name);
            ensure!(a_left != b_left, "ON equality must connect both inputs");
            let (a, b) = if a_left { (a, b) } else { (b, a) };
            ensure!(
                a.value.oid == b.value.oid
                    && a.modifier == b.modifier
                    && a.collation == b.collation
                    && matches!(a.value.oid, 16 | 20 | 21 | 23 | 2950 | 1114 | 1184),
                "unsupported or incompatible join-tree key types"
            );
            Ok((a.value.clone(), b.value.clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    keys.sort_unstable();
    keys.dedup();
    Ok(keys)
}
