//! Resolve native source occurrences into an executable join tree.
mod keys;
mod subquery;
mod using;
use super::scope as native_source;
use crate::{
    compiler::{
        ColumnRef,
        left_join::Kind,
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
    collation: u32,
    modifier: i32,
    unqualified: bool,
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
pub(super) fn scope(tree: Tree, contract: &Contract) -> Result<Scope> {
    let mut builder = Builder { contract, sources: Vec::new(), nodes: Vec::new(), joins: 0 };
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
            projection: Vec::new(),
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
            Tree::Scalar { left, right, alias } => {
                let left = self.tree(*left)?;
                let right = self.derived_checked(right, alias, true)?;
                let id = format!("tree_join_{}", self.joins);
                let bound = keys::condition(None, &left.columns, &right.columns, &id)?;
                self.join(left, right, bound, Kind::Left)
            }
            Tree::Mark { left, right, probe, alias, condition, semijoin } => {
                let left = self.tree(*left)?;
                self.mark(left, right, (&probe, alias, condition.as_deref(), semijoin))
            }
            Tree::Join { left, right, kind, clause } => {
                let left = self.tree(*left)?;
                let right = self.tree(*right)?;
                self.native_join(left, right, kind, clause)
            }
        }
    }
    fn derived(
        &mut self,
        source: Box<crate::compiler::parser::Parsed>,
        alias: String,
    ) -> Result<Edge> {
        self.derived_checked(source, alias, false)
    }
    fn derived_checked(
        &mut self,
        source: Box<crate::compiler::parser::Parsed>,
        alias: String,
        scalar: bool,
    ) -> Result<Edge> {
        let offset = self.sources.len();
        let prefix = format!("tree_scope_{offset}_");
        let scope = super::derived::scope((source, alias), &prefix, offset, self.contract)?;
        let mut context = scope.ir.clone();
        context.sources.splice(..0, self.sources.iter().cloned());
        context.nodes.splice(..0, self.nodes.iter().cloned());
        let facts = crate::compiler::properties::occurrence(&context, self.contract)?;
        ensure!(
            !scalar || facts.maximum_rows().is_some_and(|maximum| maximum <= 1),
            "scalar subquery cardinality is not proven at most one row"
        );
        let columns = scope
            .ir
            .output
            .columns
            .iter()
            .map(|output| Column {
                label: output.label.clone(),
                qualifiers: vec![scope.alias.clone()],
                value: ColumnRef {
                    collation: facts.collation(&output.column).unwrap_or(0),
                    ..output.column.clone()
                },
                collation: facts.collation(&output.column).unwrap_or(0),
                modifier: facts.modifier(&output.column).unwrap_or(-1),
                unqualified: true,
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
                    collation: column.collation,
                    modifier: Some(column.modifier).filter(|modifier| *modifier >= 0),
                },
                collation: column.collation,
                modifier: column.modifier,
                unqualified: true,
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
        bound: keys::Bound,
        kind: Kind,
    ) -> Result<Edge> {
        if kind == Kind::Right {
            return self.right_join(left, right, bound);
        }
        let outer = kind != Kind::Inner;
        ensure!(
            !left.columns.iter().any(|left| right.columns.iter().any(|right| left
                .qualifiers
                .last()
                .is_some_and(|qualifier| right.qualifiers.last() == Some(qualifier)))),
            "duplicate exposed table qualifier"
        );
        let id = format!("tree_join_{}", self.joins);
        let keys::Bound { keys, computed, predicate } = bound;
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
                    left: left.columns.iter().map(|value| value.value.clone()).collect(),
                    full: kind == Kind::Full,
                    right: right.columns.iter().map(|value| value.value.clone()).collect(),
                    predicate: predicate.clone(),
                },
            });
            if kind == Kind::Full {
                for column in &mut left.columns {
                    column.value.nullable = true;
                }
            }
            for column in &mut right.columns {
                column.value.nullable = true;
            }
        } else {
            self.key(left_key.clone(), left.id, keys.iter().map(|pair| pair.0.clone()).collect());
            self.key(right_key.clone(), right.id, keys.into_iter().map(|pair| pair.1).collect());
            self.nodes.push(Node::Join { id: id.clone(), left: left_key, right: right_key });
        }
        let result = if !outer && let Some(predicate) = predicate {
            let filtered = format!("{id}_filtered");
            self.nodes.push(Node::Filter { id: filtered.clone(), input: id, predicate });
            filtered
        } else {
            id
        };
        let mut columns = left.columns;
        columns.extend(right.columns);
        Ok(Edge { id: result, columns })
    }
    fn right_join(&mut self, left: Edge, right: Edge, mut bound: keys::Bound) -> Result<Edge> {
        let width = right.columns.len();
        bound.keys = bound.keys.into_iter().map(|(left, right)| (right, left)).collect();
        bound.computed.swap(0, 1);
        let mut edge = self.join(right, left, bound, Kind::Left)?;
        edge.columns.rotate_left(width);
        Ok(edge)
    }
    fn existence(
        &mut self,
        left: Edge,
        right: Edge,
        names: Vec<(crate::compiler::parser::scalar::Key, crate::compiler::parser::scalar::Key)>,
        anti: bool,
    ) -> Result<Edge> {
        let columns = left.columns.clone();
        let id = format!("tree_join_{}", self.joins);
        let bound = keys::bind(names, &left.columns, &right.columns, &id)?;
        let edge = self.join(left, right, bound, Kind::Left)?;
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
                [_] => column.unqualified,
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
