//! Bounded ISO-8601 source text and explicit integral calendar literal units.
use super::Interval;
use anyhow::{Context, Result, ensure};
pub(super) fn parse(text: &str) -> Result<Interval> {
    let text = text.trim();
    ensure!(text.len() <= 4096, "interval text exceeds engine budget");
    match text.trim() {
        "infinity" => return Ok(Interval::PositiveInfinity),
        "-infinity" => return Ok(Interval::NegativeInfinity),
        _ => {}
    }
    let mut values = [0_i128; 3];
    if let Some(text) = text.strip_prefix('P') {
        iso(text, &mut values)?;
    } else {
        words(text, &mut values)?;
    }
    Interval::finite(
        i32::try_from(values[0]).context("interval months out of range")?,
        i32::try_from(values[1]).context("interval days out of range")?,
        i64::try_from(values[2]).context("interval time out of range")?,
    )
}
fn iso(text: &str, values: &mut [i128; 3]) -> Result<()> {
    let mut time = false;
    let mut start = 0;
    let mut previous = 0;
    let mut units = 0;
    for (index, character) in text.char_indices() {
        if character == 'T' {
            ensure!(index == start && !time, "invalid ISO interval time marker");
            time = true;
            start = index + 1;
            continue;
        }
        if character.is_ascii_alphabetic() {
            let number = &text[start..index];
            let rank = match (character, time) {
                ('Y', false) => 1,
                ('M', false) => 2,
                ('D', false) => 3,
                ('H', true) => 4,
                ('M', true) => 5,
                ('S', true) => 6,
                _ => anyhow::bail!("unsupported ISO interval unit"),
            };
            ensure!(rank > previous, "duplicate or unordered ISO interval unit");
            previous = rank;
            units += 1;
            let (slot, multiplier) = match (character, time) {
                ('Y', false) => (0, 12),
                ('M', false) => (0, 1),
                ('D', false) => (1, 1),
                ('H', true) => (2, 3_600_000_000_i128),
                ('M', true) => (2, 60_000_000),
                ('S', true) => (2, 1_000_000),
                _ => anyhow::bail!("unsupported ISO interval unit"),
            };
            values[slot] = values[slot]
                .checked_add(scaled(number, multiplier)?)
                .context("interval accumulation overflow")?;
            start = index + 1;
        }
    }
    ensure!(start == text.len() && units > 0 && !text.ends_with('T'), "invalid ISO interval");
    Ok(())
}
fn words(text: &str, values: &mut [i128; 3]) -> Result<()> {
    let parts = text.split_whitespace().collect::<Vec<_>>();
    ensure!(!parts.is_empty(), "empty interval");
    let mut index = 0;
    while index < parts.len() {
        if parts[index].contains(':') {
            values[2] = values[2]
                .checked_add(clock(parts[index])?)
                .context("interval accumulation overflow")?;
            index += 1;
            continue;
        }
        let number = parts[index];
        let unit = parts
            .get(index + 1)
            .context("interval literal requires explicit units")?
            .to_ascii_lowercase();
        let (slot, multiplier) = match unit.as_str() {
            "year" | "years" => (0, 12),
            "mon" | "mons" | "month" | "months" => (0, 1),
            "day" | "days" => (1, 1),
            "week" | "weeks" => (1, 7),
            "hour" | "hours" => (2, 3_600_000_000_i128),
            "minute" | "minutes" | "min" | "mins" => (2, 60_000_000),
            "second" | "seconds" | "sec" | "secs" => (2, 1_000_000),
            "millisecond" | "milliseconds" => (2, 1000),
            "microsecond" | "microseconds" => (2, 1),
            _ => anyhow::bail!("unsupported interval literal unit"),
        };
        values[slot] = values[slot]
            .checked_add(scaled(number, multiplier)?)
            .context("interval accumulation overflow")?;
        index += 2;
    }
    Ok(())
}
fn scaled(text: &str, multiplier: i128) -> Result<i128> {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    ensure!(
        fraction.len() <= 6 && fraction.bytes().all(|byte| byte.is_ascii_digit()),
        "interval precision exceeds microseconds"
    );
    let sign = if text.starts_with('-') { -1 } else { 1 };
    let whole = whole.parse::<i128>()?;
    let fraction = if fraction.is_empty() { 0 } else { fraction.parse::<i128>()? };
    let divisor =
        10_i128.pow(u32::try_from(text.split_once('.').map_or(0, |(_, value)| value.len()))?);
    let numerator = whole
        .checked_mul(divisor)
        .and_then(|value| value.checked_add(sign * fraction))
        .and_then(|value| value.checked_mul(multiplier))
        .context("interval literal overflow")?;
    ensure!(
        numerator % divisor == 0,
        "fractional calendar units or submicroseconds are unsupported"
    );
    Ok(numerator / divisor)
}
fn clock(text: &str) -> Result<i128> {
    let negative = text.starts_with('-');
    let text = text.strip_prefix('-').or_else(|| text.strip_prefix('+')).unwrap_or(text);
    let parts = text.split(':').collect::<Vec<_>>();
    let [hours, minutes, seconds] = parts.as_slice() else {
        anyhow::bail!("interval clock requires hours:minutes:seconds");
    };
    ensure!(
        !hours.is_empty() && hours.bytes().all(|byte| byte.is_ascii_digit()),
        "invalid interval hours"
    );
    let hours: i128 = hours.parse()?;
    let minutes: i128 = minutes.parse()?;
    ensure!((0..60).contains(&minutes), "invalid interval minutes");
    let seconds = scaled(seconds, 1_000_000)?;
    ensure!((0..60_000_000).contains(&seconds), "invalid interval seconds");
    let result = hours
        .checked_mul(3_600_000_000)
        .and_then(|value| value.checked_add(minutes * 60_000_000 + seconds))
        .context("interval clock overflow")?;
    Ok(if negative { -result } else { result })
}
