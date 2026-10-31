use super::{
    DAY, Date, Interval, Result, Timestamp, calendar, decimal, iso, normalize, period, validate,
};
use crate::numeric::{Numeric, Rounding};
pub fn extract(field: &str, text: &str, oid: u32) -> Result<Option<String>> {
    let lower = field.to_ascii_lowercase();
    let field = normalize(&lower);
    validate(field, oid, false)?;
    if oid == 1186 {
        return Ok(interval(field, Interval::parse(text)?));
    }
    let value = if oid == 1082 {
        Date::parse(text)?.sort_value()
    } else {
        Timestamp::parse(text, oid)?.sort_value()
    };
    if value == i64::MIN
        || value == i64::MAX
        || (oid == 1082 && matches!(value, -2_147_483_648 | 2_147_483_647))
    {
        return Ok(infinite(field, value < 0, false));
    }
    let (days, clock) =
        if oid == 1082 { (value, 0) } else { (value.div_euclid(DAY), value.rem_euclid(DAY)) };
    if field == "epoch" {
        if oid == 1082 {
            return Ok(Some((i128::from(days) * 86400 + 946_684_800).to_string()));
        }
        let epoch = i128::from(value) + 946_684_800_000_000;
        let result = if epoch < i128::from(i64::MAX) {
            decimal(epoch, 6)
        } else {
            Numeric::parse(&epoch.to_string())?
                .binary(&Numeric::parse("1000000")?, "/")?
                .rounded(6, Rounding::Nearest)?
                .text()
        };
        return Ok(Some(result));
    }
    if field == "julian" && oid != 1082 {
        let fraction =
            Numeric::parse(&clock.to_string())?.binary(&Numeric::parse(&DAY.to_string())?, "/")?;
        return Ok(Some(
            Numeric::parse(&(days + 2_451_545).to_string())?.binary(&fraction, "+")?.text(),
        ));
    }
    let (year, month, day) = calendar::fields(days);
    let result = match field {
        "year" => {
            if year <= 0 {
                year - 1
            } else {
                year
            }
        }
        "month" => month,
        "day" => day,
        "quarter" => (month - 1) / 3 + 1,
        "decade" => year.div_euclid(10),
        "century" => period(year, 100),
        "millennium" => period(year, 1000),
        "dow" => (days + 6).rem_euclid(7),
        "isodow" => (days + 5).rem_euclid(7) + 1,
        "doy" => days - calendar::days(year, 1, 1) + 1,
        "week" => iso(days).1,
        "isoyear" => {
            let year = iso(days).0;
            if year <= 0 { year - 1 } else { year }
        }
        "julian" => days + 2_451_545,
        "timezone" | "timezone_hour" | "timezone_minute" => 0,
        _ => return Ok(Some(clock_field(field, clock))),
    };
    Ok(Some(result.to_string()))
}
fn clock_field(field: &str, micros: i64) -> String {
    match field {
        "hour" => (micros / 3_600_000_000).to_string(),
        "minute" => ((micros / 60_000_000) % 60).to_string(),
        "second" => decimal(i128::from(micros % 60_000_000), 6),
        "milliseconds" => decimal(i128::from(micros % 60_000_000), 3),
        _ => (micros % 60_000_000).to_string(),
    }
}
fn interval(field: &str, value: Interval) -> Option<String> {
    let Interval::Finite { months, days, micros } = value else {
        return infinite(field, matches!(value, Interval::NegativeInfinity), true);
    };
    let result = match field {
        "year" => (months / 12).to_string(),
        "month" => (months % 12).to_string(),
        "day" => days.to_string(),
        "quarter" => (months % 12 / 3 + 1).to_string(),
        "decade" => (months / 120).to_string(),
        "century" => (months / 1200).to_string(),
        "millennium" => (months / 12000).to_string(),
        "epoch" => decimal(
            (i128::from(months / 12) * 31_557_600
                + i128::from(months % 12) * 2_592_000
                + i128::from(days) * 86400)
                * 1_000_000
                + i128::from(micros),
            6,
        ),
        _ => clock_field(field, micros),
    };
    Some(result)
}
fn infinite(field: &str, negative: bool, interval: bool) -> Option<String> {
    let monotonic = matches!(field, "year" | "decade" | "century" | "millennium" | "epoch")
        || if interval {
            matches!(field, "day" | "hour")
        } else {
            matches!(field, "isoyear" | "julian")
        };
    monotonic.then(|| if negative { "-Infinity" } else { "Infinity" }.into())
}
