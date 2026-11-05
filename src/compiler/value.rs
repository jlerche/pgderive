//! Native scalar decoding shared by predicates, keys and typed computation.
use anyhow::{Result, ensure};
use serde::Serialize;
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum Value {
    Boolean(bool),
    Text(String),
    Integer(i64),
    Numeric(crate::numeric::Numeric),
    Uuid([u8; 16]),
    Timestamp(crate::temporal::Timestamp),
    Date(crate::temporal::Date),
    Interval(crate::temporal::Interval),
}
pub fn decode(oid: u32, text: &str) -> Result<Value> {
    match oid {
        16 => match text {
            "t" => Ok(Value::Boolean(true)),
            "f" => Ok(Value::Boolean(false)),
            _ => anyhow::bail!("invalid native boolean"),
        },
        20 | 21 | 23 => Ok(Value::Integer(text.parse()?)),
        1700 => Ok(Value::Numeric(crate::numeric::Numeric::parse(text)?)),
        25 | 1043 | 705 => Ok(Value::Text(text.into())),
        1114 | 1184 => Ok(Value::Timestamp(crate::temporal::Timestamp::parse(text, oid)?)),
        1082 => Ok(Value::Date(crate::temporal::Date::parse(text)?)),
        1186 => Ok(Value::Interval(crate::temporal::Interval::parse(text)?)),
        2950 => Ok(Value::Uuid(uuid(text)?)),
        _ => Err(crate::engine::execution::PlanFault("unsupported scalar codec").into()),
    }
}
fn uuid(text: &str) -> Result<[u8; 16]> {
    let text = text.strip_prefix('{').and_then(|text| text.strip_suffix('}')).unwrap_or(text);
    let mut digits = [0_u8; 32];
    let mut length: usize = 0;
    for byte in text.bytes() {
        if byte == b'-' {
            ensure!(length != 0 && length.is_multiple_of(4), "invalid UUID separator");
        } else {
            ensure!(byte.is_ascii_hexdigit(), "invalid UUID digit");
            if let Some(digit) = digits.get_mut(length) {
                *digit = byte;
            }
            length += 1;
        }
    }
    ensure!(length == 32 && !text.ends_with('-') && !text.contains("--"), "invalid UUID length");
    let mut value = [0_u8; 16];
    for (index, byte) in value.iter_mut().enumerate() {
        *byte = u8::from_str_radix(std::str::from_utf8(&digits[index * 2..index * 2 + 2])?, 16)?;
    }
    Ok(value)
}

fn uuid_text(bytes: [u8; 16]) -> Result<String> {
    use std::fmt::Write;
    let mut text = String::with_capacity(36);
    for (index, byte) in bytes.into_iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            text.push('-');
        }
        write!(text, "{byte:02x}")?;
    }
    Ok(text)
}

pub(super) fn literal_text(oid: u32, text: &str) -> Result<String> {
    Ok(match decode(oid, text)? {
        Value::Date(value) => value.text(),
        Value::Interval(value) => value.text(),
        Value::Timestamp(value) => value.text(oid)?,
        Value::Uuid(bytes) => uuid_text(bytes)?,
        Value::Numeric(value) => value.text(),
        _ => text.into(),
    })
}

impl Value {
    pub(super) fn text(self, oid: u32) -> Result<String> {
        Ok(match self {
            Self::Boolean(value) => if value { "t" } else { "f" }.into(),
            Self::Integer(value) => value.to_string(),
            Self::Numeric(value) => value.text(),
            Self::Text(value) => value,
            Self::Timestamp(value) => value.text(oid)?,
            Self::Uuid(bytes) => uuid_text(bytes)?,
            Self::Interval(value) => value.text(),
            Self::Date(value) => value.text(),
        })
    }
}
