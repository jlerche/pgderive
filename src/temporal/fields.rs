//! `PostgreSQL` calendar fields and truncation in the attested UTC context.
use super::{DAY, Date, Interval, Timestamp, calendar, interval::decimal};
use anyhow::{Result, ensure};
mod extract;
mod truncate;
pub use extract::extract;
pub use truncate::truncate;
pub fn validate(field: &str, oid: u32, truncation: bool) -> Result<()> {
    let field = normalize(field);
    let valid = if truncation {
        matches!(
            field,
            "microseconds"
                | "milliseconds"
                | "second"
                | "minute"
                | "hour"
                | "day"
                | "month"
                | "quarter"
                | "year"
                | "decade"
                | "century"
                | "millennium"
        ) || (field == "week" && oid != 1186)
    } else {
        matches!(
            field,
            "year" | "month" | "day" | "quarter" | "decade" | "century" | "millennium" | "epoch"
        ) || (oid != 1082
            && matches!(field, "hour" | "minute" | "second" | "milliseconds" | "microseconds"))
            || (oid != 1186
                && matches!(field, "week" | "isoyear" | "dow" | "isodow" | "doy" | "julian"))
            || (oid == 1184 && matches!(field, "timezone" | "timezone_hour" | "timezone_minute"))
    };
    ensure!(valid, "unsupported calendar field {field} for OID {oid}");
    Ok(())
}
fn normalize(field: &str) -> &str {
    match field {
        "years" => "year",
        "months" => "month",
        "days" => "day",
        "hours" => "hour",
        "minutes" => "minute",
        "seconds" => "second",
        "millisecond" => "milliseconds",
        "microsecond" => "microseconds",
        "weeks" => "week",
        "quarters" => "quarter",
        "decades" => "decade",
        "centuries" => "century",
        "millennia" => "millennium",
        value => value,
    }
}
fn iso(days: i64) -> (i64, i64) {
    let thursday = days + 3 - (days + 5).rem_euclid(7);
    let (year, _, _) = calendar::fields(thursday);
    let january_fourth = calendar::days(year, 1, 4);
    let monday = january_fourth - (january_fourth + 5).rem_euclid(7);
    (year, (days - monday).div_euclid(7) + 1)
}
const fn period(year: i64, width: i64) -> i64 {
    if year > 0 { (year - 1) / width + 1 } else { -((-year) / width + 1) }
}
