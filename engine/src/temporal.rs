//! `PostgreSQL` timestamp values in microseconds relative to 2000-01-01.
mod calendar;
mod date;
mod fields;
mod interval;
use anyhow::{Context, Result, ensure};
pub use date::Date;
pub use fields::{extract, truncate, validate as validate_field};
pub use interval::Interval;
use serde::Serialize;
const DAY: i64 = 86_400_000_000;
const MIN: i64 = -211_813_488_000_000_000;
const END: i64 = 9_223_371_331_200_000_000;
/// Fractional-second precision for native timestamp and full-range interval casts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Precision(u8);
impl Precision {
    pub(crate) const INTERVAL_FULL_RANGE: i32 = 0x7fff;
    pub(crate) const INTERVAL_FOUR_DIGITS: i32 = (Self::INTERVAL_FULL_RANGE << 16) | 4;
    const MAX_DIGITS: u8 = 6;

    pub(crate) fn interval_modifier(self) -> i32 {
        (Self::INTERVAL_FULL_RANGE << 16) | self.encoded()
    }

    pub(crate) fn from_interval_modifier(modifier: i32) -> Result<Self> {
        let digits = modifier & 0xffff;
        ensure!(
            modifier >> 16 == Self::INTERVAL_FULL_RANGE && digits <= i32::from(Self::MAX_DIGITS),
            "unsupported interval result modifier"
        );
        Ok(Self(u8::try_from(digits)?))
    }

    pub(crate) fn new(digits: i64) -> Result<Self> {
        ensure!((0..=i64::from(i32::MAX)).contains(&digits), "temporal precision out of range");
        Ok(Self(u8::try_from(digits.min(i64::from(Self::MAX_DIGITS)))?))
    }

    pub(crate) fn needs_rounding(self, prior: i32) -> bool {
        self.0 < Self::MAX_DIGITS && (prior < 0 || self.encoded() < prior)
    }

    pub(crate) fn needs_interval_rounding(self, prior: i32) -> bool {
        const SECOND_FIELD: i32 = 1 << 12;
        prior < 0 && self.0 < Self::MAX_DIGITS
            || prior >= 0
                && (prior >> 16) & SECOND_FIELD != 0
                && self.needs_rounding(prior & 0xffff)
    }

    pub(crate) fn encoded(self) -> i32 {
        i32::from(self.0)
    }

    fn round(self, micros: i64) -> Option<i64> {
        let step = 10_i64.pow(u32::from(Self::MAX_DIGITS - self.0));
        let offset = step / 2;
        let adjusted = micros.checked_add(if micros < 0 { -offset } else { offset })?;
        Some(adjusted - adjusted % step)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Timestamp(i64);
impl Timestamp {
    pub(crate) const ROUNDED_END: &str = "294277-01-01T00:00:00";
    pub(crate) const LAST_FINITE: &str = "294276-12-31 23:59:59.999999";

    pub(crate) fn parse_literal(value: &str, oid: u32) -> Result<Self> {
        let result = Self::parse_input(value.trim_ascii(), oid, true)?;
        ensure!(result.0 != END, "timestamp out of PostgreSQL range");
        Ok(result)
    }

    pub(crate) fn difference(self, other: Self) -> Result<Interval> {
        match (self.0, other.0) {
            (i64::MIN, value) => {
                ensure!(value != i64::MIN, "interval out of range");
                Ok(Interval::NegativeInfinity)
            }
            (i64::MAX, value) => {
                ensure!(value != i64::MAX, "interval out of range");
                Ok(Interval::PositiveInfinity)
            }
            (_, i64::MIN) => Ok(Interval::PositiveInfinity),
            (_, i64::MAX) => Ok(Interval::NegativeInfinity),
            (left, right) => {
                Interval::difference(left.checked_sub(right).context("interval out of range")?)
            }
        }
    }

    pub(crate) fn parse(value: &str, oid: u32) -> Result<Self> {
        Self::parse_input(value, oid, false)
    }

    fn parse_input(value: &str, oid: u32, literal: bool) -> Result<Self> {
        // PostgreSQL 17 precision casts can produce exactly END, which is also
        // visible in source rows and internal expression results.
        ensure!(
            matches!(oid, 1114 | 1184),
            crate::engine::execution::PlanFault("invalid timestamp type")
        );
        match value {
            "-infinity" => return Ok(Self(i64::MIN)),
            "infinity" => return Ok(Self(i64::MAX)),
            _ => {}
        }
        let (value, bc) = value.strip_suffix(" BC").map_or((value, false), |value| (value, true));
        let (date, time) = if literal {
            value.split_once([' ', 'T']).unwrap_or((value, "00:00:00"))
        } else {
            value.split_once([' ', 'T']).context("timestamp requires ISO date/time")?
        };
        let (time, offset) =
            if literal { literal_timezone(time, oid)? } else { timezone(time, oid)? };
        let days = calendar::parse(date, bc)?;
        let clock = clock(time)?;
        let micros = days
            .checked_mul(DAY)
            .and_then(|value| value.checked_add(clock))
            .and_then(|value| value.checked_sub(offset * 1_000_000))
            .context("timestamp arithmetic out of range")?;
        ensure!((MIN..=END).contains(&micros), "timestamp out of PostgreSQL range");
        Ok(Self(micros))
    }
    pub(crate) fn from_micros(value: i64) -> Result<Self> {
        ensure!((MIN..END).contains(&value), "timestamp out of range");
        Ok(Self(value))
    }
    pub(crate) fn rounded(self, precision: Precision) -> Result<Self> {
        if matches!(self.0, i64::MIN | i64::MAX) {
            return Ok(self);
        }
        let micros = precision.round(self.0).context("timestamp out of range")?;
        if micros == END { Ok(Self(END)) } else { Self::from_micros(micros) }
    }
    pub(crate) const fn at_rounded_end(self) -> bool {
        self.0 == END
    }
    pub(crate) fn cast_zone(self) -> Result<Self> {
        // The timezone conversion checks the ordinary timestamp range even in UTC.
        ensure!(self.0 != END, "timestamp out of range");
        Ok(self)
    }
    pub(crate) fn date(self) -> Result<Date> {
        Ok(match self.0 {
            i64::MIN => Date::negative_infinity(),
            i64::MAX => Date::infinity(),
            value => Date::from_days(value.div_euclid(DAY))?,
        })
    }
    pub(crate) fn add_interval(self, interval: Interval) -> Result<Self> {
        match (self.0, interval) {
            (i64::MIN, Interval::PositiveInfinity) | (i64::MAX, Interval::NegativeInfinity) => {
                anyhow::bail!("timestamp infinity arithmetic is indeterminate")
            }
            (_, Interval::PositiveInfinity) => Ok(Self(i64::MAX)),
            (_, Interval::NegativeInfinity) => Ok(Self(i64::MIN)),
            (i64::MIN | i64::MAX, _) => Ok(self),
            (value, Interval::Finite { months, days, micros }) => {
                let mut result = value;
                if months != 0 {
                    let (year, month, day) = calendar::fields(result.div_euclid(DAY));
                    let month = year * 12 + month - 1 + i64::from(months);
                    let year = month.div_euclid(12);
                    let month = month.rem_euclid(12) + 1;
                    let day = day.min(calendar::month_days(year, month));
                    result = calendar::days(year, month, day)
                        .checked_mul(DAY)
                        .and_then(|date| date.checked_add(result.rem_euclid(DAY)))
                        .context("timestamp month arithmetic out of range")?;
                    Self::from_micros(result)?;
                }
                if days != 0 {
                    result = i64::try_from(i128::from(result) + i128::from(days) * i128::from(DAY))
                        .context("timestamp day arithmetic out of range")?;
                    Self::from_micros(result)?;
                }
                Self::from_micros(
                    result.checked_add(micros).context("timestamp time arithmetic out of range")?,
                )
            }
        }
    }
    pub(crate) fn bin(self, stride: i64, origin: Self) -> Result<Self> {
        if matches!(self.0, i64::MIN | i64::MAX) {
            return Ok(self);
        }
        ensure!(
            stride > 0 && !matches!(origin.0, i64::MIN | i64::MAX),
            "invalid date_bin stride/origin"
        );
        let difference = self.0.checked_sub(origin.0).context("date_bin interval out of range")?;
        let remainder = difference % stride;
        let delta = difference - remainder;
        let mut result = origin.0.checked_add(delta).context("date_bin timestamp out of range")?;
        if remainder < 0 {
            result = result.checked_sub(stride).context("date_bin timestamp out of range")?;
        }
        ensure!((MIN..END).contains(&result), "date_bin timestamp out of range");
        Ok(Self(result))
    }
    pub(crate) fn subtract_duration(self, duration: i64) -> Result<Self> {
        if matches!(self.0, i64::MIN | i64::MAX) {
            return Ok(self);
        }
        let result =
            self.0.checked_sub(duration).context("timestamp interval arithmetic out of range")?;
        ensure!((MIN..END).contains(&result), "timestamp interval arithmetic out of range");
        Ok(Self(result))
    }
    pub(crate) fn text(self, oid: u32) -> Result<String> {
        Ok(self.json(oid)?.replacen('T', " ", 1).replace("+00:00", "+00"))
    }
    // Native ordering tokens are the original timestamp representation, including infinities.
    pub(crate) const fn from_sort_value(value: i64) -> Self {
        Self(value)
    }

    pub(crate) const fn sort_value(self) -> i64 {
        self.0
    }
    pub(crate) fn json(self, oid: u32) -> Result<String> {
        ensure!(
            matches!(oid, 1114 | 1184),
            crate::engine::execution::PlanFault("invalid timestamp type")
        );
        match self.0 {
            i64::MIN => return Ok("-infinity".into()),
            i64::MAX => return Ok("infinity".into()),
            _ => {}
        }
        let (date, bc) = calendar::format(self.0.div_euclid(DAY));
        let micros = self.0.rem_euclid(DAY);
        let hour = micros / 3_600_000_000;
        let minute = (micros / 60_000_000) % 60;
        let second = (micros / 1_000_000) % 60;
        let fraction = micros % 1_000_000;
        let fraction = if fraction == 0 {
            String::new()
        } else {
            format!(".{fraction:06}").trim_end_matches('0').into()
        };
        let zone = if oid == 1184 { "+00:00" } else { "" };
        let era = if bc { " BC" } else { "" };
        Ok(format!("{date}T{hour:02}:{minute:02}:{second:02}{fraction}{zone}{era}"))
    }
}
fn clock(value: &str) -> Result<i64> {
    let parts = value.split(':').collect::<Vec<_>>();
    let [hour, minute, seconds] = parts.as_slice() else {
        anyhow::bail!("invalid timestamp clock");
    };
    let hour: i64 = hour.parse()?;
    let minute: i64 = minute.parse()?;
    let (seconds, fraction) = seconds.split_once('.').unwrap_or((seconds, ""));
    let seconds: i64 = seconds.parse()?;
    ensure!(
        (0..24).contains(&hour) && (0..60).contains(&minute) && (0..60).contains(&seconds),
        "invalid timestamp clock"
    );
    ensure!(
        fraction.len() <= 6 && fraction.bytes().all(|byte| byte.is_ascii_digit()),
        "invalid timestamp precision"
    );
    let fraction = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<i64>()? * 10_i64.pow(u32::try_from(6 - fraction.len())?)
    };
    Ok((hour * 3600 + minute * 60 + seconds) * 1_000_000 + fraction)
}
fn literal_timezone(time: &str, oid: u32) -> Result<(&str, i64)> {
    if !time.contains(['+', '-', 'Z']) {
        // The frozen session timezone is UTC.
        return Ok((time, 0));
    }
    let (clock, offset) = timezone(time, 1184)?;
    Ok((clock.trim_ascii_end(), if oid == 1114 { 0 } else { offset }))
}
fn timezone(value: &str, oid: u32) -> Result<(&str, i64)> {
    let zone = value.find(['+', '-', 'Z']);
    if oid == 1114 {
        ensure!(zone.is_none(), "timestamp without timezone requires local ISO time");
        return Ok((value, 0));
    }
    let zone = zone.context("timestamptz requires explicit ISO offset")?;
    let (clock, suffix) = value.split_at(zone);
    if suffix == "Z" {
        return Ok((clock, 0));
    }
    let sign = match suffix.as_bytes().first() {
        Some(b'-') => -1,
        Some(b'+') => 1,
        _ => anyhow::bail!("invalid timezone offset"),
    };
    let parts = suffix[1..]
        .split(':')
        .map(|part| {
            ensure!(
                !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()),
                "invalid timezone offset"
            );
            Ok(part.parse::<i64>()?)
        })
        .collect::<Result<Vec<_>>>()?;
    let offset = match parts.as_slice() {
        [hour] => (*hour, 0, 0),
        [hour, minute] => (*hour, *minute, 0),
        [hour, minute, second] => (*hour, *minute, *second),
        _ => anyhow::bail!("invalid timezone offset"),
    };
    ensure!(
        (0..16).contains(&offset.0) && (0..60).contains(&offset.1) && (0..60).contains(&offset.2),
        "invalid timezone offset"
    );
    Ok((clock, sign * (offset.0 * 3600 + offset.1 * 60 + offset.2)))
}
#[cfg(test)]
mod tests;
