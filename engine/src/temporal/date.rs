//! Native signed-day dates retain the full `PostgreSQL` date range.
use super::{DAY, Timestamp, calendar};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Date(i32);
impl Date {
    pub(crate) fn parse(text: &str) -> Result<Self> {
        match text {
            "infinity" => return Ok(Self(i32::MAX)),
            "-infinity" => return Ok(Self(i32::MIN)),
            _ => {}
        }
        let (text, bc) = text.strip_suffix(" BC").map_or((text, false), |value| (value, true));
        Self::from_days(calendar::parse(text, bc)?)
    }
    pub(crate) fn from_days(value: i64) -> Result<Self> {
        ensure!((-2_451_545..2_145_031_949).contains(&value), "date out of range");
        Ok(Self(i32::try_from(value)?))
    }
    pub(crate) fn sort_value(self) -> i64 {
        i64::from(self.0)
    }
    pub(crate) const fn infinity() -> Self {
        Self(i32::MAX)
    }
    pub(crate) const fn negative_infinity() -> Self {
        Self(i32::MIN)
    }
    pub(crate) fn text(self) -> String {
        match self.0 {
            i32::MIN => "-infinity".into(),
            i32::MAX => "infinity".into(),
            value => {
                let (text, bc) = calendar::format(i64::from(value));
                if bc { format!("{text} BC") } else { text }
            }
        }
    }
    pub(crate) fn timestamp(self) -> Result<Timestamp> {
        match self.0 {
            i32::MIN => Ok(Timestamp(i64::MIN)),
            i32::MAX => Ok(Timestamp(i64::MAX)),
            value => Timestamp::from_micros(
                i64::from(value).checked_mul(DAY).context("date out of timestamp range")?,
            ),
        }
    }
    pub(crate) fn add_days(self, days: i64) -> Result<Self> {
        if matches!(self.0, i32::MIN | i32::MAX) {
            return Ok(self);
        }
        Self::from_days(
            i64::from(self.0).checked_add(days).context("date arithmetic out of range")?,
        )
    }
    pub(crate) fn compare_timestamp(self, right: Timestamp) -> std::cmp::Ordering {
        let date = match self.0 {
            i32::MIN => i128::MIN,
            i32::MAX => i128::MAX,
            days => i128::from(days) * i128::from(DAY),
        };
        let timestamp = match right.sort_value() {
            i64::MIN => i128::MIN,
            i64::MAX => i128::MAX,
            micros => i128::from(micros),
        };
        // A finite date at this boundary overflows conversion to timestamp.
        date.cmp(&timestamp).then_with(|| {
            if right.at_rounded_end() {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        })
    }
    pub(crate) fn difference(self, right: Self) -> Result<i32> {
        ensure!(
            !matches!(self.0, i32::MIN | i32::MAX) && !matches!(right.0, i32::MIN | i32::MAX),
            "cannot subtract infinite dates"
        );
        self.0.checked_sub(right.0).context("date difference out of range")
    }
}
