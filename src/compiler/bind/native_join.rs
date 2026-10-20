//! Resolve native source occurrences into an executable join tree.
mod keys;
use super::scope as native_source;
use crate::{
    compiler::{
        ColumnRef,
        parser::{Name, native_join::Tree},
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
    validated: bool,
}
pub(super) struct Scope {
    pub(super) ir: Relational,
    columns: Vec<Column>,
}
impl Scope {
    pub(super) fn resolve(&self, name: &Name) -> Result<ColumnRef> {
        Ok(resolve(name, &self.columns)?.value.clone())
    }
}
pub(super) fn scope(tree: Tree, contract: &Contract, validated: bool) -> Result<Scope> {
    let mut builder =
        Builder { contract, sources: Vec::new(), nodes: Vec::new(), joins: 0, validated };
    let edge = builder.tree(tree)?;
    let columns = edge
        .columns
        .iter()
        .map(|column| crate::compiler::projection::OutputColumn {
            label: column.label.clone(),
            column: column.value.clone(),
        })
        .collect();
    builder.nodes.push(Node::Output { id: "project".into(), input: edge.id });
    Ok(Scope {
        ir: Relational {
            sources: builder.sources,
            nodes: builder.nodes,
            output: crate::compiler::Projected { columns },
        },
        columns: edge.columns,
    })
}
impl Builder<'_> {
    fn tree(&mut self, tree: Tree) -> Result<Edge> {
        match tree {
            Tree::Source(table) => self.source(table),
            Tree::Exists { left, right, keys, anti } => {
                let left = self.tree(*left)?;
                let right = self.source(right)?;
                self.existence(left, right, keys, anti)
            }
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
        let scope = super::derived::scope((source, alias), &prefix, offset, self.contract)?;
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
        self.sources.extend(scope.ir.sources);
        ensure!(self.sources.len() <= 16, "join source occurrence limit exceeded");
        self.nodes.extend(scope.ir.nodes);
        Ok(Edge { id: format!("{prefix}visible"), columns })
    }
    fn source(&mut self, table: crate::compiler::parser::Table) -> Result<Edge> {
        let native = native_source(table, self.contract)?;
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
        mut left: Edge,
        mut right: Edge,
        names: Vec<(crate::compiler::parser::scalar::Key, crate::compiler::parser::scalar::Key)>,
        outer: bool,
    ) -> Result<Edge> {
        ensure!(
            !left.columns.iter().any(|left| right
                .columns
                .iter()
                .any(|right| left.qualifiers.last() == right.qualifiers.last())),
            "duplicate exposed table qualifier"
        );
        let id = format!("tree_join_{}", self.joins);
        let keys::Bound { keys, computed } =
            keys::bind(names, &left.columns, &right.columns, &id, self.validated)?;
        let [left_computed, right_computed] = computed;
        self.map(&mut left, left_computed, &format!("{id}_left_map"));
        self.map(&mut right, right_computed, &format!("{id}_right_map"));
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
    fn existence(
        &mut self,
        left: Edge,
        right: Edge,
        names: Vec<(crate::compiler::parser::scalar::Key, crate::compiler::parser::scalar::Key)>,
        anti: bool,
    ) -> Result<Edge> {
        let columns = left.columns.clone();
        let edge = self.join(left, right, names, true)?;
        let Some(Node::LeftJoin { id, input, .. }) = self.nodes.pop() else {
            anyhow::bail!("missing existence join")
        };
        let Some(Node::Union { id: union, left, right }) = self.nodes.pop() else {
            anyhow::bail!("missing existence tagged union")
        };
        ensure!(union == input, "existence input differs from tagged union");
        let counts = format!("{id}_right_counts");
        let presence = format!("{id}_right_presence");
        self.nodes.push(Node::RightCounts { id: counts.clone(), input: right });
        self.nodes.push(Node::RightPresence { id: presence.clone(), input: counts });
        self.nodes.push(Node::Union { id: union, left, right: presence });
        self.nodes.push(Node::Existence { id: id.clone(), input, anti });
        ensure!(edge.columns.len() > columns.len(), "missing existence source columns");
        Ok(Edge { id, columns })
    }
    fn map(&mut self, edge: &mut Edge, computed: Vec<crate::compiler::scalar::Computed>, id: &str) {
        if !computed.is_empty() {
            self.nodes.push(Node::Map { id: id.into(), input: edge.id.clone(), computed });
            edge.id = id.into();
        }
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
