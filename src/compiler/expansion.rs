//! Resolved constant series and expansion namespace.
use super::{ColumnRef, relational::Node};
use anyhow::{Result, ensure};
use serde::Serialize;
pub const FIELD: &str = "@series_0";
#[derive(Clone, Serialize)]
pub struct Series {
    pub(crate) start: i32,
    pub(crate) end: i32,
}
pub(in crate::compiler) fn resolve(
    name: &super::parser::Name,
    series: Option<&super::parser::expansion::Series>,
    native: Result<ColumnRef>,
) -> Result<ColumnRef> {
    let matches=series.is_some_and(|series|matches!(name.0.as_slice(),[column] if column==&series.column)||matches!(name.0.as_slice(),[alias,column] if alias==&series.alias&&column==&series.column));
    if !matches {
        return native;
    }
    ensure!(native.is_err(), "SQL resolve: ambiguous series column");
    Ok(ColumnRef { name: FIELD.into(), right: false, oid: 23, nullable: false })
}
pub(in crate::compiler) fn attach(
    nodes: &mut Vec<Node>,
    series: &super::parser::expansion::Series,
) {
    let spec = Series { start: series.start, end: series.end };
    for node in nodes.iter_mut() {
        match node {
            Node::Map { input, .. } | Node::PartitionBy { input, .. } if input == "source" => {
                *input = "expanded".into();
            }
            _ => {}
        }
    }
    nodes.insert(1, Node::Expand { id: "expanded".into(), input: "source".into(), series: spec });
}
