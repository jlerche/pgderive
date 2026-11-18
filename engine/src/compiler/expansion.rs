//! Resolved constant series and expansion namespace.
use super::{ColumnRef, relational::Node};
use anyhow::{Result, ensure};
use serde::Serialize;
pub const FIELD: &str = "@series_0";
pub const ORDINAL: &str = "@series_1";

#[derive(Clone, Serialize)]
pub struct Series {
    start: i64,
    step: i64,
    pub(crate) count: u16,
    pub(crate) oid: u32,
    pub(crate) ordinality: bool,
}

impl Series {
    pub(in crate::compiler) fn new(
        start: (Option<i64>, u32),
        end: (Option<i64>, u32),
        step: (Option<i64>, u32),
        ordinality: bool,
    ) -> Result<Self> {
        let types = [start.1, end.1, step.1];
        ensure!(types.contains(&20) || types.contains(&23), "ambiguous integral series overload");
        let oid = if types.contains(&20) { 20 } else { 23 };
        let (Some(start), Some(end), Some(step)) = (start.0, end.0, step.0) else {
            return Ok(Self { start: 0, step: 1, count: 0, oid, ordinality });
        };
        ensure!(step != 0, "step size cannot equal zero");
        let distance = (i128::from(end) - i128::from(start)) * i128::from(step.signum());
        let count = if distance < 0 { 0 } else { distance / i128::from(step).abs() + 1 };
        ensure!(count <= 1024, "series expansion exceeds 1024 rows per source occurrence");
        Ok(Self { start, step, count: u16::try_from(count)?, oid, ordinality })
    }

    pub(in crate::compiler) fn exact_interval_offsets(&self, duration: i64) -> bool {
        self.values().next_back().is_none_or(|(last, _)| {
            last.abs().max(i128::from(self.start).abs()) * i128::from(duration).abs()
                <= 9_007_199_254_740_992
        })
    }

    pub(crate) fn values(&self) -> impl DoubleEndedIterator<Item = (i128, u16)> + '_ {
        (0..self.count).map(|index| {
            (i128::from(self.start) + i128::from(index) * i128::from(self.step), index + 1)
        })
    }
}

fn named_column<'a>(name: &'a super::parser::Name, alias: &str) -> Option<&'a str> {
    match name.0.as_slice() {
        [column] => Some(column),
        [qualifier, column] if qualifier == alias => Some(column),
        _ => None,
    }
}

pub(in crate::compiler) fn resolve(
    name: &super::parser::Name,
    series: Option<&super::parser::expansion::Series>,
    native: Result<ColumnRef>,
) -> Result<ColumnRef> {
    let Some(series) = series else { return native };
    let Some(column) = named_column(name, &series.alias) else { return native };
    let (field, oid) = if column == series.column {
        (FIELD, series.spec.oid)
    } else if Some(column) == series.ordinal.as_deref() {
        (ORDINAL, 20)
    } else {
        return native;
    };
    ensure!(native.is_err(), "SQL resolve: ambiguous series column");
    Ok(ColumnRef {
        name: field.into(),
        right: false,
        oid,
        nullable: false,
        collation: 0,
        modifier: None,
    })
}
pub(in crate::compiler) fn attach(
    nodes: &mut Vec<Node>,
    series: &super::parser::expansion::Series,
) {
    let spec = series.spec.clone();
    for node in nodes.iter_mut() {
        match node {
            Node::Map { input, .. }
            | Node::PartitionBy { input, .. }
            | Node::Output { input, .. }
                if input == "source" =>
            {
                *input = "expanded".into();
            }
            _ => {}
        }
    }
    nodes.insert(1, Node::Expand { id: "expanded".into(), input: "source".into(), series: spec });
}
