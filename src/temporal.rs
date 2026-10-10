//! `PostgreSQL` timestamp values in microseconds relative to 2000-01-01.
mod calendar;
use anyhow::{Context, Result, ensure};
use serde::Serialize;
const DAY: i64 = 86_400_000_000;
const MIN: i64 = -211_813_488_000_000_000;
const END: i64 = 9_223_371_331_200_000_000;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Timestamp(i64);
impl Timestamp {
    pub(crate) fn parse(value: &str, oid: u32) -> Result<Self> {
        ensure!(matches!(oid, 1114 | 1184), "invalid timestamp type");
        match value {
            "-infinity" => return Ok(Self(i64::MIN)),
            "infinity" => return Ok(Self(i64::MAX)),
            _ => {}
        }
        let (value, bc) = value.strip_suffix(" BC").map_or((value, false), |value| (value, true));
        let (date, time) =
            value.split_once([' ', 'T']).context("timestamp requires ISO date/time")?;
        let (time, offset) = timezone(time, oid)?;
        let days = calendar::parse(date, bc)?;
        let clock = clock(time)?;
        let micros = days
            .checked_mul(DAY)
            .and_then(|value| value.checked_add(clock))
            .and_then(|value| value.checked_sub(offset * 1_000_000))
            .context("timestamp arithmetic out of range")?;
        ensure!((MIN..END).contains(&micros), "timestamp out of PostgreSQL range");
        Ok(Self(micros))
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
    pub(crate) const fn sort_value(self) -> i64 {
        self.0
    }
    pub(crate) fn json(self, oid: u32) -> Result<String> {
        ensure!(matches!(oid, 1114 | 1184), "invalid timestamp type");
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
    let sign = if suffix.starts_with('-') { -1 } else { 1 };
    let parts = suffix[1..]
        .split(':')
        .map(str::parse::<i64>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
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
