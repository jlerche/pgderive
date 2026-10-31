//! Component-preserving intervals with `PostgreSQL`'s duration comparison.
mod parse;
use super::DAY;
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::cmp::Ordering;
#[derive(Clone, Copy, Debug, Serialize)]
pub enum Interval {
    NegativeInfinity,
    Finite { months: i32, days: i32, micros: i64 },
    PositiveInfinity,
}
impl Interval {
    pub(crate) fn finite(months: i32, days: i32, micros: i64) -> Result<Self> {
        ensure!(
            !((months == i32::MAX && days == i32::MAX && micros == i64::MAX)
                || (months == i32::MIN && days == i32::MIN && micros == i64::MIN)),
            "interval out of range"
        );
        Ok(Self::Finite { months, days, micros })
    }
    pub(crate) const fn duration(micros: i64) -> Self {
        Self::Finite { months: 0, days: 0, micros }
    }
    pub(crate) fn difference(micros: i64) -> Result<Self> {
        Ok(Self::Finite { months: 0, days: i32::try_from(micros / DAY)?, micros: micros % DAY })
    }
    pub(crate) fn parse(text: &str) -> Result<Self> {
        parse::parse(text)
    }
    pub(crate) fn text(self) -> String {
        match self {
            Self::NegativeInfinity => "-infinity".into(),
            Self::PositiveInfinity => "infinity".into(),
            Self::Finite { months, days, micros } => {
                let second = decimal(i128::from(micros), 6);
                format!("P{months}M{days}DT{second}S")
            }
        }
    }
    pub(crate) fn negate(self) -> Result<Self> {
        Ok(match self {
            Self::NegativeInfinity => Self::PositiveInfinity,
            Self::PositiveInfinity => Self::NegativeInfinity,
            Self::Finite { months, days, micros } => Self::finite(
                months.checked_neg().context("interval month overflow")?,
                days.checked_neg().context("interval day overflow")?,
                micros.checked_neg().context("interval time overflow")?,
            )?,
        })
    }
    pub(crate) fn binary(self, other: Self, subtract: bool) -> Result<Self> {
        if let (
            Self::Finite { months, days, micros },
            Self::Finite { months: right_months, days: right_days, micros: right_micros },
        ) = (self, other)
        {
            return Self::finite(
                if subtract {
                    months.checked_sub(right_months)
                } else {
                    months.checked_add(right_months)
                }
                .context("interval month overflow")?,
                if subtract { days.checked_sub(right_days) } else { days.checked_add(right_days) }
                    .context("interval day overflow")?,
                if subtract {
                    micros.checked_sub(right_micros)
                } else {
                    micros.checked_add(right_micros)
                }
                .context("interval time overflow")?,
            );
        }
        let other = if subtract {
            match other {
                Self::NegativeInfinity => Self::PositiveInfinity,
                Self::PositiveInfinity => Self::NegativeInfinity,
                finite @ Self::Finite { .. } => finite,
            }
        } else {
            other
        };
        ensure!(
            !matches!(
                (self, other),
                (Self::NegativeInfinity, Self::PositiveInfinity)
                    | (Self::PositiveInfinity, Self::NegativeInfinity)
            ),
            "interval infinity arithmetic is indeterminate"
        );
        Ok(if matches!(self, Self::NegativeInfinity) || matches!(other, Self::NegativeInfinity) {
            Self::NegativeInfinity
        } else {
            Self::PositiveInfinity
        })
    }
    fn duration_key(self) -> (i8, i128) {
        match self {
            Self::NegativeInfinity => (-1, 0),
            Self::PositiveInfinity => (1, 0),
            Self::Finite { months, days, micros } => (
                0,
                (i128::from(months) * 30 + i128::from(days)) * i128::from(DAY) + i128::from(micros),
            ),
        }
    }
}
impl PartialEq for Interval {
    fn eq(&self, other: &Self) -> bool {
        self.duration_key() == other.duration_key()
    }
}
impl Eq for Interval {}
impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Interval {
    fn cmp(&self, other: &Self) -> Ordering {
        self.duration_key().cmp(&other.duration_key())
    }
}
pub(super) fn decimal(value: i128, scale: usize) -> String {
    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    if scale == 0 {
        return value.to_string();
    }
    let digits = format!("{digits:0>width$}", width = scale + 1);
    let (whole, fraction) = digits.split_at(digits.len() - scale);
    format!("{}{whole}.{fraction}", if negative { "-" } else { "" })
}
