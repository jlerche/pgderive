use super::{DAY, Date, Interval, Result, Timestamp, calendar, normalize, validate};
use anyhow::Context;
pub fn truncate(field: &str, text: &str, oid: u32) -> Result<String> {
    let lower = field.to_ascii_lowercase();
    let field = normalize(&lower);
    validate(field, oid, true)?;
    if oid == 1186 {
        return Ok(interval(field, Interval::parse(text)?)?.text());
    }
    let timestamp =
        if oid == 1082 { Date::parse(text)?.timestamp()? } else { Timestamp::parse(text, oid)? };
    let value = timestamp.sort_value();
    let result_oid = if oid == 1082 { 1184 } else { oid };
    if matches!(value, i64::MIN | i64::MAX) {
        return timestamp.text(result_oid);
    }
    let days = value.div_euclid(DAY);
    let clock = value.rem_euclid(DAY);
    let (mut year, mut month, mut day) = calendar::fields(days);
    let unit = match field {
        "microseconds" => 1,
        "milliseconds" => 1000,
        "second" => 1_000_000,
        "minute" => 60_000_000,
        "hour" => 3_600_000_000,
        _ => 0,
    };
    if unit > 0 {
        return Timestamp::from_micros(days * DAY + clock / unit * unit)?.text(result_oid);
    }
    if field == "week" {
        return Timestamp::from_micros((days - (days + 5).rem_euclid(7)) * DAY)?.text(result_oid);
    }
    match field {
        "millennium" => {
            year = first_year(year, 1000);
            month = 1;
            day = 1;
        }
        "century" => {
            year = first_year(year, 100);
            month = 1;
            day = 1;
        }
        "decade" => {
            year = year.div_euclid(10) * 10;
            month = 1;
            day = 1;
        }
        "year" => {
            month = 1;
            day = 1;
        }
        "quarter" => {
            month = (month - 1) / 3 * 3 + 1;
            day = 1;
        }
        "month" => day = 1,
        _ => {}
    }
    Timestamp::from_micros(
        calendar::days(year, month, day)
            .checked_mul(DAY)
            .context("date_trunc timestamp out of range")?,
    )?
    .text(result_oid)
}
const fn first_year(year: i64, width: i64) -> i64 {
    if year > 0 { (year - 1) / width * width + 1 } else { -((-year) / width) * width - (width - 1) }
}
fn interval(field: &str, value: Interval) -> Result<Interval> {
    let Interval::Finite { mut months, mut days, mut micros } = value else {
        return Ok(value);
    };
    let unit = match field {
        "microseconds" => 1,
        "milliseconds" => 1000,
        "second" => 1_000_000,
        "minute" => 60_000_000,
        "hour" => 3_600_000_000,
        _ => 0,
    };
    if unit > 0 {
        micros = micros / unit * unit;
    } else {
        micros = 0;
        if field != "day" {
            days = 0;
            months = match field {
                "millennium" => months / 12000 * 12000,
                "century" => months / 1200 * 1200,
                "decade" => months / 120 * 120,
                "year" => months / 12 * 12,
                "quarter" => months / 3 * 3,
                _ => months,
            };
        }
    }
    Interval::finite(months, days, micros)
}
