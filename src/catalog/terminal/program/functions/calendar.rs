//! Calendar fields over timezone-independent timestamp inputs.
use anyhow::Result;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Calendar {
    Year,
    Month,
    Day,
    Minute,
    Second,
    Epoch,
    Week,
    IsoYear,
    IsoDow,
    Dow,
    Doy,
    Quarter,
}
impl Calendar {
    pub fn parse(field: &str) -> Result<Self> {
        Ok(match field {
            "year" => Self::Year,
            "month" => Self::Month,
            "day" => Self::Day,
            "minute" => Self::Minute,
            "second" => Self::Second,
            "epoch" => Self::Epoch,
            "week" => Self::Week,
            "isoyear" => Self::IsoYear,
            "isodow" => Self::IsoDow,
            "dow" => Self::Dow,
            "doy" => Self::Doy,
            "quarter" => Self::Quarter,
            _ => anyhow::bail!("unsupported timestamp extraction field"),
        })
    }
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Year => "year",
            Self::Month => "month",
            Self::Day => "day",
            Self::Minute => "minute",
            Self::Second => "second",
            Self::Epoch => "epoch",
            Self::Week => "week",
            Self::IsoYear => "isoyear",
            Self::IsoDow => "isodow",
            Self::Dow => "dow",
            Self::Doy => "doy",
            Self::Quarter => "quarter",
        }
    }
}
