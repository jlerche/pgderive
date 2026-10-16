//! Character operations use Unicode scalar values, never bytes or grapheme clusters.
use crate::compiler::capabilities::Builtin;
use anyhow::{Context, Result, ensure};
use std::collections::BTreeSet;
const OUTPUT_LIMIT: usize = 16 * 1024 * 1024;

pub(super) fn evaluate(operation: Builtin, arguments: &[String], input_oid: u32) -> Result<String> {
    let first = arguments.first().context("function input absent")?;
    let second = || arguments.get(1).context("second function input absent");
    let third = || arguments.get(2).context("third function input absent");
    match operation {
        Builtin::Abs => {
            let value = first.parse::<i64>()?.checked_abs().context("integral abs overflow")?;
            super::super::check_width(value, input_oid)?;
            Ok(value.to_string())
        }
        Builtin::Hex => hexadecimal(first, input_oid),
        Builtin::Length => count(first.chars().count()),
        Builtin::OctetLength => count(first.len()),
        Builtin::BitLength => count(first.len().checked_mul(8).context("bit length overflow")?),
        Builtin::Ascii => Ok(first.chars().next().map_or(0, u32::from).to_string()),
        Builtin::Chr => character(first),
        Builtin::Reverse => Ok(first.chars().rev().collect()),
        Builtin::Left | Builtin::Right => {
            edge(first, second()?.parse()?, operation == Builtin::Right)
        }
        Builtin::Ltrim | Builtin::Rtrim | Builtin::Btrim => {
            Ok(trim(first, arguments.get(1).map_or(" ", String::as_str), operation))
        }
        Builtin::Replace => replace(first, second()?, third()?),
        Builtin::Translate => Ok(translate(first, second()?, third()?)),
        Builtin::Strpos => first
            .find(second()?)
            .map_or_else(|| Ok("0".into()), |index| count(first[..index].chars().count() + 1)),
        Builtin::StartsWith => Ok(if first.starts_with(second()?) { "t" } else { "f" }.into()),
        Builtin::Repeat => repeat(first, second()?.parse()?),
        Builtin::SplitPart => split(first, second()?, third()?.parse()?),
        Builtin::Substring => substring(
            first,
            second()?.parse()?,
            arguments.get(2).map(|value| value.parse()).transpose()?,
        ),
    }
}
fn count(value: usize) -> Result<String> {
    Ok(i32::try_from(value).context("function integer result overflow")?.to_string())
}
fn character(text: &str) -> Result<String> {
    let value = text.parse::<u32>()?;
    ensure!(value != 0, "chr(0) is not permitted in PostgreSQL text");
    Ok(char::from_u32(value).context("invalid Unicode code point for chr")?.to_string())
}
fn hexadecimal(text: &str, oid: u32) -> Result<String> {
    if oid == 20 {
        Ok(format!("{:x}", text.parse::<i64>()?))
    } else {
        Ok(format!("{:x}", text.parse::<i32>()?))
    }
}
fn edge(text: &str, number: i32, right: bool) -> Result<String> {
    ensure!(
        !(right && number == i32::MIN),
        "right(text, -2147483648) is not available in-engine: PostgreSQL minor versions differ at this boundary"
    );
    let length = text.chars().count();
    let magnitude = usize::try_from(number.unsigned_abs())?;
    let take = if number < 0 { length.saturating_sub(magnitude) } else { length.min(magnitude) };
    Ok(text.chars().skip(if right { length - take } else { 0 }).take(take).collect())
}
fn trim(text: &str, characters: &str, operation: Builtin) -> String {
    let set = characters.chars().collect::<BTreeSet<_>>();
    let matches = |character| set.contains(&character);
    match operation {
        Builtin::Ltrim => text.trim_start_matches(matches),
        Builtin::Rtrim => text.trim_end_matches(matches),
        _ => text.trim_matches(matches),
    }
    .into()
}
fn replace(text: &str, from: &str, to: &str) -> Result<String> {
    if from.is_empty() {
        return Ok(text.into());
    }
    let occurrences = text.matches(from).count();
    let size = text
        .len()
        .checked_sub(occurrences * from.len())
        .and_then(|base| {
            occurrences.checked_mul(to.len()).and_then(|extra| base.checked_add(extra))
        })
        .context("replace size overflow")?;
    ensure!(size <= OUTPUT_LIMIT, "replace output exceeds 16 MiB engine budget");
    Ok(text.replace(from, to))
}
fn translate(text: &str, from: &str, to: &str) -> String {
    let from = from.chars().collect::<Vec<_>>();
    let to = to.chars().collect::<Vec<_>>();
    text.chars()
        .filter_map(|character| {
            from.iter()
                .position(|value| *value == character)
                .map_or(Some(character), |index| to.get(index).copied())
        })
        .collect()
}
fn repeat(text: &str, number: i32) -> Result<String> {
    let number = usize::try_from(number.max(0))?;
    ensure!(
        text.len().checked_mul(number).is_some_and(|size| size <= OUTPUT_LIMIT),
        "repeat output exceeds 16 MiB engine budget"
    );
    Ok(text.repeat(number))
}
fn split(text: &str, delimiter: &str, field: i32) -> Result<String> {
    ensure!(field != 0, "split_part field position must not be zero");
    if delimiter.is_empty() {
        return Ok(if field == 1 || field == -1 { text } else { "" }.into());
    }
    let index = usize::try_from(field.unsigned_abs() - 1)?;
    Ok(if field < 0 {
        text.split(delimiter).collect::<Vec<_>>().into_iter().rev().nth(index)
    } else {
        text.split(delimiter).nth(index)
    }
    .unwrap_or("")
    .into())
}
fn substring(text: &str, start: i32, length: Option<i32>) -> Result<String> {
    if let Some(length) = length {
        ensure!(length >= 0, "negative substring length not allowed");
    }
    let skip = usize::try_from(i64::from(start).max(1) - 1)?;
    let take = length
        .map(|length| {
            usize::try_from((i64::from(start) + i64::from(length) - i64::from(start).max(1)).max(0))
        })
        .transpose()?
        .unwrap_or(usize::MAX);
    Ok(text.chars().skip(skip).take(take).collect())
}
