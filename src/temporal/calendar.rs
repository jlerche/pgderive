use anyhow::{Result, ensure};
// Proleptic Gregorian civil-day conversion, with astronomical year numbering.
// Epoch adjustment is 719468 days to 1970 and 10957 more days to PostgreSQL 2000.
pub(super) fn parse(value: &str, bc: bool) -> Result<i64> {
    let parts =
        value.split('-').map(str::parse::<i64>).collect::<std::result::Result<Vec<_>, _>>()?;
    let [year, month, day] = parts.as_slice() else {
        anyhow::bail!("invalid ISO date");
    };
    ensure!(
        (1..=294_277).contains(year) && (1..=12).contains(month),
        "invalid timestamp year/month"
    );
    let year = if bc { 1 - year } else { *year };
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    ensure!((1..=days).contains(day), "invalid timestamp day");
    let adjusted_year = year - i64::from(*month <= 2);
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let shifted_month = month + if *month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Ok(era * 146_097 + day_of_era - 719_468 - 10_957)
}
pub(super) fn format(days: i64) -> (String, bool) {
    let days = days + 719_468 + 10_957;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = shifted_month + if shifted_month < 10 { 3 } else { -9 };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    let display_year = if year <= 0 { 1 - year } else { year };
    (format!("{display_year:04}-{month:02}-{day:02}"), year <= 0)
}
