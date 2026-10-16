use anyhow::Result;
use serde::{Deserialize, Serialize};
mod calendar;
mod integral;
pub use calendar::Calendar;
pub use integral::Operation;
pub(super) type Signature = (&'static str, &'static str);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Function {
    Abs { oid: u32 },
    Split,
    FormatDate,
    FormatMinute,
    Integral { operation: Operation, oid: u32 },
    Numeric,
    Int2,
    Int4,
    Int8,
    Multiply,
    Greater,
    Less,
    GreaterEqual,
    LessEqual,
    Length,
    Replace,
    Subtract,
    Hour,
    Calendar(Calendar),
    Utc,
    Lower,
    TextEqual,
    RegexMatch,
}
pub(super) fn call_type(function: Function, arguments: &[u32]) -> Result<u32> {
    let result = match (function, arguments) {
        (Function::Abs { oid }, [input]) if matches!(oid, 20 | 21 | 23) && oid == *input => oid,
        (Function::Integral { oid, .. }, [left, right])
            if matches!(oid, 20 | 21 | 23) && *left == oid && *right == oid =>
        {
            oid
        }
        (Function::Int2, [20 | 23]) => 21,
        (Function::Int8, [21 | 23]) => 20,
        (Function::Numeric, [20 | 21 | 23])
        | (Function::Multiply, [1700, 1700])
        | (Function::Hour | Function::Calendar(_), [1114]) => 1700,
        (
            Function::Greater | Function::Less | Function::GreaterEqual | Function::LessEqual,
            [1700, 1700],
        )
        | (Function::TextEqual, [25 | 1043, 25 | 1043]) => 16,
        (Function::Int4, [20 | 21])
        | (Function::Length, [25 | 1043])
        | (Function::Subtract, [23, 23]) => 23,
        (Function::Replace, [25 | 1043, 25, 25])
        | (Function::Lower, [25 | 1043])
        | (Function::FormatDate | Function::FormatMinute, [1114]) => 25,
        (Function::RegexMatch | Function::Split, [25 | 1043, 25]) => 1009,
        (Function::Utc, [1184]) => 1114,
        _ => anyhow::bail!("SQL type: unsupported terminal built-in signature"),
    };
    Ok(result)
}
impl Function {
    pub(super) fn signature(self, arguments: &[u32]) -> Result<Signature> {
        call_type(self, arguments)?;
        Ok(match self {
            Self::Abs { oid: 20 } => ("pg_catalog.abs(bigint)", "int8abs"),
            Self::Abs { oid: 21 } => ("pg_catalog.abs(smallint)", "int2abs"),
            Self::Abs { oid: 23 } => ("pg_catalog.abs(integer)", "int4abs"),
            Self::Abs { .. } => anyhow::bail!("unsupported ABS type"),
            Self::Split => ("pg_catalog.string_to_array(text,text)", "text_to_array"),
            Self::FormatDate | Self::FormatMinute => {
                ("pg_catalog.to_char(timestamp,text)", "timestamp_to_char")
            }
            Self::Integral { operation, oid } => integral::signature(operation, oid)?,
            Self::Int2 | Self::Int4 | Self::Int8 => integral_cast(self, arguments)?,
            Self::Numeric => match arguments {
                [20] => ("pg_catalog.numeric(bigint)", "int8_numeric"),
                [21] => ("pg_catalog.numeric(smallint)", "int2_numeric"),
                [23] => ("pg_catalog.numeric(integer)", "int4_numeric"),
                _ => anyhow::bail!("invalid numeric coercion"),
            },
            Self::Multiply => ("pg_catalog.numeric_mul(numeric,numeric)", "numeric_mul"),
            Self::Greater => ("pg_catalog.numeric_gt(numeric,numeric)", "numeric_gt"),
            Self::Less => ("pg_catalog.numeric_lt(numeric,numeric)", "numeric_lt"),
            Self::GreaterEqual => ("pg_catalog.numeric_ge(numeric,numeric)", "numeric_ge"),
            Self::LessEqual => ("pg_catalog.numeric_le(numeric,numeric)", "numeric_le"),
            Self::Length => ("pg_catalog.length(text)", "textlen"),
            Self::Replace => ("pg_catalog.replace(text,text,text)", "replace_text"),
            Self::Subtract => ("pg_catalog.int4mi(integer,integer)", "int4mi"),
            Self::Hour | Self::Calendar(_) => {
                ("pg_catalog.extract(text,timestamp)", "extract_timestamp")
            }
            Self::Utc => ("pg_catalog.timezone(text,timestamptz)", "timestamptz_zone"),
            Self::Lower => ("pg_catalog.lower(text)", "lower"),
            Self::TextEqual => ("pg_catalog.texteq(text,text)", "texteq"),
            Self::RegexMatch => ("pg_catalog.regexp_match(text,text)", "regexp_match_no_flags"),
        })
    }
    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::Abs { .. } => "abs",
            Self::Split => "string_to_array",
            Self::FormatDate | Self::FormatMinute => "to_char",
            Self::Integral { operation, oid } => integral::name(operation, oid),
            Self::Int2 => "int2",
            Self::Int4 => "int4",
            Self::Int8 => "int8",
            Self::Numeric => "numeric",
            Self::Multiply => "numeric_mul",
            Self::Greater => "numeric_gt",
            Self::Less => "numeric_lt",
            Self::GreaterEqual => "numeric_ge",
            Self::LessEqual => "numeric_le",
            Self::Length => "length",
            Self::Replace => "replace",
            Self::Subtract => "int4mi",
            Self::Hour | Self::Calendar(_) => "extract",
            Self::Utc => "timezone",
            Self::Lower => "lower",
            Self::TextEqual => "texteq",
            Self::RegexMatch => "regexp_match",
        }
    }
}
pub fn implementation_types(implementation: &str) -> Result<(&'static str, &'static str, bool)> {
    let (args, result) = match implementation {
        "int82" => ("20", "21"),
        "i4toi2" => ("23", "21"),
        "int84" => ("20", "23"),
        "i2toi4" => ("21", "23"),
        "int28" => ("21", "20"),
        "int48" => ("23", "20"),
        "int8_numeric" => ("20", "1700"),
        "int2_numeric" => ("21", "1700"),
        "int4_numeric" => ("23", "1700"),
        "numeric_gt" | "numeric_lt" | "numeric_ge" | "numeric_le" => ("1700 1700", "16"),
        "replace_text" => ("25 25 25", "25"),
        "int4mi" => ("23 23", "23"),
        "extract_timestamp" => ("25 1114", "1700"),
        "lower" => ("25", "25"),
        "texteq" => ("25 25", "16"),
        "regexp_match_no_flags" => ("25 25", "1009"),
        _ => return integral::implementation_types(implementation),
    };
    Ok((args, result, true))
}

fn integral_cast(function: Function, arguments: &[u32]) -> Result<Signature> {
    Ok(match (function, arguments) {
        (Function::Int2, [20]) => ("pg_catalog.int2(bigint)", "int82"),
        (Function::Int2, [23]) => ("pg_catalog.int2(integer)", "i4toi2"),
        (Function::Int4, [20]) => ("pg_catalog.int4(bigint)", "int84"),
        (Function::Int4, [21]) => ("pg_catalog.int4(smallint)", "i2toi4"),
        (Function::Int8, [21]) => ("pg_catalog.int8(smallint)", "int28"),
        (Function::Int8, [23]) => ("pg_catalog.int8(integer)", "int48"),
        _ => anyhow::bail!("invalid integral cast signature"),
    })
}
