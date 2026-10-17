//! Native scalar decoding shared by predicates, keys and typed computation.
use anyhow::{Result, ensure};
use serde::Serialize;
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(super) enum Value {
    Boolean(bool),
    Text(String),
    Integer(i64),
    Numeric(crate::numeric::Numeric),
    Uuid([u8; 16]),
    Timestamp(crate::temporal::Timestamp),
    Interval(crate::temporal::Gap),
}
pub(super) fn decode(oid: u32, text: &str) -> Result<Value> {
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
        2950 => Ok(Value::Uuid(uuid(text)?)),
        _ => anyhow::bail!("unsupported scalar codec for OID {oid}"),
    }
}
fn uuid(text: &str) -> Result<[u8; 16]> {
    let text = text.strip_prefix('{').and_then(|text| text.strip_suffix('}')).unwrap_or(text);
    let mut digits = String::new();
    for byte in text.bytes() {
        if byte == b'-' {
            ensure!(!digits.is_empty() && digits.len().is_multiple_of(4), "invalid UUID separator");
        } else {
            ensure!(byte.is_ascii_hexdigit(), "invalid UUID digit");
            digits.push(char::from(byte));
        }
    }
    ensure!(
        digits.len() == 32 && !text.ends_with('-') && !text.contains("--"),
        "invalid UUID length"
    );
    let mut value = [0_u8; 16];
    for (index, byte) in value.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&digits[index * 2..index * 2 + 2], 16)?;
    }
    Ok(value)
}

pub(super) fn literal_text(oid: u32, text: &str) -> Result<String> {
    Ok(match decode(oid, text)? {
        Value::Timestamp(value) => value.text(oid)?,
        Value::Uuid(bytes) => {
            use std::fmt::Write;
            let mut text = String::with_capacity(32);
            for byte in bytes {
                write!(text, "{byte:02x}")?;
            }
            text
        }
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
            Self::Uuid(bytes) => {
                use std::fmt::Write;
                let mut text = String::with_capacity(32);
                for byte in bytes {
                    write!(text, "{byte:02x}")?;
                }
                text
            }
            Self::Interval(_) => anyhow::bail!("interval output is not available in-engine"),
        })
    }
}
