//! Resolved relational nodes lowered into executable durable circuits.
mod constants;
mod projection;
use super::{ColumnRef, Projected};
pub(super) use projection::column_names;
use serde::Serialize;
#[derive(Clone, Serialize)]
pub struct Source {
    pub(crate) schema: String,
    pub(crate) table: String,
}
#[derive(Clone, Serialize)]
pub struct Binding {
    pub(crate) input: ColumnRef,
    pub(crate) output: ColumnRef,
}
#[derive(Clone, Serialize)]
pub struct ConstantColumn {
    pub(crate) reference: ColumnRef,
    pub(crate) modifier: i32,
    pub(crate) collation: u32,
}
// Keep serialized variants, edge rewriting and runtime declarations in one schema.
macro_rules! relational_nodes {
    ($($variant:ident [$($input:ident),*] => $kind:ident { $($(#[$attr:meta])* $field:ident: $ty:ty),* }),* $(,)?) => {
        #[derive(Clone, Serialize)]
        pub enum Node {
            Source { id: String, source: usize },
            $($variant { id: String, $($input: String,)* $($(#[$attr])* $field: $ty,)* }),*
        }
        impl Node {
            pub(crate) fn id(&self) -> &str {
                match self {
                    Self::Source { id, .. } $(| Self::$variant { id, .. })* => id,
                }
            }
            pub(crate) fn inputs(&self) -> Vec<&str> {
                match self {
                    Self::Source { .. } => Vec::new(),
                    $(Self::$variant { $($input,)* .. } => vec![$($input.as_str()),*]),*
                }
            }
            pub(crate) fn rewrite(
                &mut self,
                rename: &impl Fn(&str) -> String,
                edge: &impl Fn(&str) -> String,
            ) {
                match self {
                    Self::Source { id, .. } => *id = rename(id),
                    $(Self::$variant { id, $($input,)* .. } => {
                        *id = rename(id);
                        $(*$input = edge($input);)*
                    }),*
                }
            }
            pub(crate) fn declaration(
                &self,
                schema: &impl Fn(&str) -> String,
            ) -> crate::engine::plan::Node {
                use crate::engine::plan::{Kind, Node};
                let (id, kind, inputs) = match self {
                    Self::Source { id, source } => (id, Kind::Source, vec![format!("source_{source}")]),
                    $(Self::$variant { id, $($input,)* .. } => (id, Kind::$kind, vec![$($input.clone()),*])),*
                };
                Node { id: id.clone(), kind, inputs, schema: schema(id) }
            }
        }
    };
}
relational_nodes! {
    Empty [input] => Project {},
    Sorted [input] => Project { columns: Vec<ColumnRef> },
    Constant [input] => Project {
        rows: Vec<crate::transaction::Row>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        columns: Vec<ConstantColumn>
    },
    Values [input] => Project {
        rows: Vec<Vec<super::typed::Typed>>,
        columns: Vec<ConstantColumn>
    },
    Filter [input] => Project { predicate: super::Predicate },
    Expand [input] => Expand { series: super::expansion::Series },
    Map [input] => Project { computed: Vec<super::scalar::Computed> },
    Rebind [input] => Project { fields: Vec<Binding> },
    CrossKey [input] => Project {},
    EquijoinKey [input] => Project { keys: Vec<ColumnRef> },
    Join [left, right] => Join {},
    Union [left, right] => Union {},
    LeftJoinInput [input] => Project { spec: super::left_join::Input },
    SetDistinct [input] => Aggregate { columns: Vec<ColumnRef> },
    SetTag [input] => Project { left: bool },
    SetOperation [input] => Aggregate { spec: super::setop::Spec },
    Existence [input] => Aggregate { anti: bool },
    Mark [input] => Aggregate { spec: Box<super::subquery::Mark> },
    PrefixInput [input] => Project { spec: super::partition::Partition },
    PrefixStatistics [input] => Statistics { spec: super::partition::Partition },
    PrefixWindow [left, right] => Window { spec: super::partition::Partition },
    RightCounts [input] => Statistics {},
    RightPresence [input] => Project {},
    LeftJoin [input] => Aggregate { spec: super::left_join::Spec },
    LookupInput [input] => Project { spec: super::lookup::Input },
    Lookup [input] => Aggregate { spec: super::lookup::Lookup },
    Output [input] => Project {},
    PartitionBy [input] => Project { keys: Vec<ColumnRef> },
    Partition [input] => Aggregate { spec: super::partition::Partition },
    Statistics [input] => Statistics { spec: super::partition::Partition },
    DistinctInput [input] => Expand { spec: super::partition::Partition },
    DistinctCounts [input] => Statistics { spec: super::partition::Partition },
    DistinctMembership [input] => Project { spec: super::partition::Partition },
    OrderedInput [input] => Expand { spec: super::partition::Partition },
    OrderedFirst [input] => Ordered { spec: super::partition::Partition },
    TopKInput [input] => Project { spec: super::topk::Spec },
    TopK [input] => Ordered { spec: super::topk::Spec },
    OrderedGroup [input] => Project {},
    OrderedFinalize [input] => Aggregate { spec: super::partition::Partition },
    Finalize [input] => Project { spec: super::partition::Partition },
}
#[derive(Clone, Serialize)]
pub struct Relational {
    pub(crate) sources: Vec<Source>,
    pub(crate) projection: Vec<std::collections::BTreeSet<String>>,
    pub(crate) nodes: Vec<Node>,
    pub(crate) output: Projected,
}
/// A materialized CTE: evaluated as written, so no rewrite crosses it.
pub(super) const MATERIALIZED: &str = "materialized";
pub(super) fn barrier(id: &str) -> bool {
    id.ends_with(MATERIALIZED)
}
/// A filter standing for LIMIT 0: what it reads is planned and then discarded.
pub(super) const ZERO_LIMIT: &str = "limit_zero";
pub fn field(source: usize, name: &str) -> String {
    format!("{source}:{name}")
}
