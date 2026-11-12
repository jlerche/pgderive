//! Typed partition aggregates and row-position window semantics.
pub mod statistics;
use super::{ColumnRef, Predicate};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
#[derive(Clone, PartialEq, Eq, Serialize)]
pub enum Function {
    Count,
    CountDistinct,
    Sum,
    SumDistinct,
    Average,
    AverageDistinct,
    MinDistinct,
    MaxDistinct,
    BoolAnd,
    BoolOr,
    Min,
    Max,
    Rank,
    DenseRank,
    RowNumber,
    Ntile,
    PercentRank,
    CumeDist,
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue,
}
impl Function {
    pub(crate) const fn distinct_measure(&self) -> bool {
        matches!(self, Self::CountDistinct | Self::SumDistinct | Self::AverageDistinct)
    }

    pub(crate) fn ordinary(&self) -> Self {
        match self {
            Self::CountDistinct => Self::Count,
            Self::SumDistinct => Self::Sum,
            Self::AverageDistinct => Self::Average,
            Self::MinDistinct => Self::Min,
            Self::MaxDistinct => Self::Max,
            other => other.clone(),
        }
    }

    pub(crate) const fn boolean(&self) -> bool {
        matches!(self, Self::BoolAnd | Self::BoolOr)
    }
    pub(crate) const fn occurrence_order(&self) -> bool {
        matches!(self, Self::RowNumber | Self::Ntile | Self::Lag | Self::Lead)
    }
    pub(crate) const fn frame_value(&self) -> bool {
        matches!(self, Self::FirstValue | Self::LastValue | Self::NthValue)
    }
    pub(crate) const fn ranking(&self) -> bool {
        matches!(
            self,
            Self::Rank
                | Self::DenseRank
                | Self::RowNumber
                | Self::Ntile
                | Self::PercentRank
                | Self::CumeDist
        )
    }
}
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct Aggregate {
    pub(crate) function: Function,
    pub(crate) argument: Option<ColumnRef>,
    pub(crate) filter: Option<Predicate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expression: Option<super::typed::Typed>,
    pub(crate) field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) navigation: Option<super::navigation::Navigation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) position: Option<super::typed::Typed>,
}
impl Aggregate {
    pub(super) fn output_column(&self) -> Result<ColumnRef> {
        let oid = self.navigation.as_ref().map_or_else(
            || result_type(&self.function, self.argument.as_ref()),
            |navigation| Ok(navigation.oid),
        )?;
        let nullable = !matches!(
            self.function,
            Function::Count
                | Function::CountDistinct
                | Function::Rank
                | Function::DenseRank
                | Function::RowNumber
                | Function::PercentRank
                | Function::CumeDist
        );
        let collation = if self.function.frame_value() {
            self.argument.as_ref().context("missing window value")?.collation
        } else {
            0
        };
        Ok(ColumnRef {
            name: self.field.clone(),
            right: false,
            oid,
            nullable,
            collation,
            modifier: None,
        })
    }
}
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct Order {
    pub(crate) column: ColumnRef,
    pub(crate) descending: bool,
    pub(crate) nulls_first: bool,
}
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct Frame {
    // None means unbounded; signed offset relative to current row.
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(crate) exclude_current: bool,
}
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct TemporalRangeFrame {
    pub(crate) start: Option<TemporalRangeOffset>,
    pub(crate) end: Option<TemporalRangeOffset>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(crate) exclude_current: bool,
}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub enum TemporalRangeOffset {
    Current,
    Interval { value: String, preceding: bool },
}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub enum Mode {
    Grouped { keys: Vec<ColumnRef> },
    Rows { order: Vec<Order>, frame: Frame },
    Ranking { order: Vec<Order> },
    Peers { order: Vec<Order>, frame: Frame, groups: bool },
    Range { order: Vec<Order>, frame: Frame },
    TemporalRange { order: Vec<Order>, frame: TemporalRangeFrame },
}
impl Mode {
    pub(crate) const fn excludes_current(&self) -> bool {
        match self {
            Self::Rows { frame, .. } | Self::Peers { frame, .. } | Self::Range { frame, .. } => {
                frame.exclude_current
            }
            Self::TemporalRange { frame, .. } => frame.exclude_current,
            _ => false,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct Partition {
    pub(crate) mode: Mode,
    pub(crate) aggregates: Vec<Aggregate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) previous_windows: Option<PreviousWindows>,
}

#[derive(Clone, Serialize)]
pub struct PreviousWindows {
    // Same-SELECT window outputs carried forward but excluded from peer tie breaks.
    pub(crate) fields: std::collections::BTreeSet<String>,
    // Outer filters can cross all preceding stages only on these common keys.
    pub(crate) common_keys: std::collections::BTreeSet<String>,
}

pub(in crate::compiler) fn result_type(
    function: &Function,
    argument: Option<&ColumnRef>,
) -> Result<u32> {
    if function.distinct_measure()
        || matches!(function, Function::MinDistinct | Function::MaxDistinct)
    {
        ensure!(
            argument.is_some_and(|arg| matches!(arg.oid, 20 | 21 | 23)),
            "DISTINCT aggregate requires an integral argument"
        );
    }
    match function {
        Function::Count
        | Function::CountDistinct
        | Function::Rank
        | Function::DenseRank
        | Function::RowNumber => Ok(20),
        Function::Ntile => Ok(23),
        Function::PercentRank | Function::CumeDist => Ok(701),
        Function::BoolAnd | Function::BoolOr => {
            ensure!(
                argument.is_some_and(|arg| arg.oid == 16),
                "Boolean aggregate requires a Boolean argument"
            );
            Ok(16)
        }
        Function::Sum | Function::SumDistinct => {
            ensure!(
                argument.is_some_and(|arg| matches!(arg.oid, 20 | 21 | 23 | 1700)),
                "SUM requires an integral argument"
            );
            Ok(if argument.is_some_and(|arg| matches!(arg.oid, 20 | 1700)) { 1700 } else { 20 })
        }
        Function::Average | Function::AverageDistinct => {
            ensure!(
                argument.is_some_and(|arg| matches!(arg.oid, 20 | 21 | 23 | 1700)),
                "AVG requires integral or numeric input"
            );
            Ok(1700)
        }
        Function::FirstValue | Function::LastValue | Function::NthValue => {
            Ok(argument.context("missing window value")?.oid)
        }
        Function::Lag | Function::Lead => {
            anyhow::bail!("navigation requires typed operand binding")
        }
        Function::Min | Function::Max | Function::MinDistinct | Function::MaxDistinct => {
            let column = argument.context("missing aggregate argument")?;
            ensure!(
                matches!(column.oid, 20 | 21 | 23 | 1700 | 1114 | 1184),
                "MIN/MAX require integral or timestamp columns"
            );
            Ok(column.oid)
        }
    }
}

/// Private neutral occurrence that makes the SQL global group exist on empty input.
pub const SEED: &str = "@sql_global_seed";
