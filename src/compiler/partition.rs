//! Typed partition aggregates and row-position window semantics.
pub mod statistics;
use super::{ColumnRef, Predicate};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
#[derive(Clone, Serialize)]
pub enum Function {
    Count,
    CountDistinct,
    Sum,
    Average,
    Min,
    Max,
    Rank,
    DenseRank,
    RowNumber,
    Lag,
    Lead,
}
impl Function {
    pub(crate) const fn occurrence_order(&self) -> bool {
        matches!(self, Self::RowNumber | Self::Lag | Self::Lead)
    }
    pub(crate) const fn ranking(&self) -> bool {
        matches!(self, Self::Rank | Self::DenseRank | Self::RowNumber)
    }
}
#[derive(Clone, Serialize)]
pub struct Aggregate {
    pub(crate) function: Function,
    pub(crate) argument: Option<ColumnRef>,
    pub(crate) filter: Option<Predicate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expression: Option<super::typed::Typed>,
    pub(crate) field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) navigation: Option<super::navigation::Navigation>,
}
#[derive(Clone, Serialize)]
pub struct Order {
    pub(crate) column: ColumnRef,
    pub(crate) descending: bool,
    pub(crate) nulls_first: bool,
}
#[derive(Clone, Serialize)]
pub struct Frame {
    // None means unbounded; signed offset relative to current row.
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
}
#[derive(Clone, Serialize)]
pub enum Mode {
    Grouped { keys: Vec<ColumnRef> },
    Rows { order: Vec<Order>, frame: Frame },
    Ranking { order: Vec<Order> },
    Peers { order: Vec<Order>, frame: Frame, groups: bool },
}
#[derive(Clone, Serialize)]
pub struct Partition {
    pub(crate) mode: Mode,
    pub(crate) aggregates: Vec<Aggregate>,
}

pub(in crate::compiler) fn result_type(
    function: &Function,
    argument: Option<&ColumnRef>,
) -> Result<u32> {
    match function {
        Function::Count | Function::Rank | Function::DenseRank | Function::RowNumber => Ok(20),
        Function::CountDistinct => {
            ensure!(
                argument.is_some_and(|arg| matches!(arg.oid, 20 | 21 | 23)),
                "COUNT DISTINCT requires native integral column"
            );
            Ok(20)
        }
        Function::Sum => {
            ensure!(
                argument.is_some_and(|arg| matches!(arg.oid, 20 | 21 | 23 | 1700)),
                "SUM requires an integral argument"
            );
            Ok(if argument.is_some_and(|arg| matches!(arg.oid, 20 | 1700)) { 1700 } else { 20 })
        }
        Function::Average => {
            ensure!(
                argument.is_some_and(|arg| matches!(arg.oid, 20 | 21 | 23)),
                "AVG requires integral input"
            );
            Ok(1700)
        }
        Function::Lag | Function::Lead => {
            anyhow::bail!("navigation requires typed operand binding")
        }
        Function::Min | Function::Max => {
            let column = argument.context("missing aggregate argument")?;
            ensure!(
                matches!(column.oid, 20 | 21 | 23 | 1700 | 1114 | 1184),
                "MIN/MAX require integral or timestamp columns"
            );
            Ok(column.oid)
        }
    }
}
