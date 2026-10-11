use anyhow::Result;
use serde::{Deserialize, Serialize};
pub(super) type Signature = (&'static str, &'static str);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Function {
    Numeric,
    Multiply,
    Greater,
    Less,
    GreaterEqual,
    LessEqual,
    Length,
    Replace,
    Subtract,
    Hour,
    Utc,
    Lower,
    TextEqual,
    RegexMatch,
}
pub(super) fn call_type(function: Function, arguments: &[u32]) -> Result<u32> {
    let result = match (function, arguments) {
        (Function::Numeric, [20 | 21 | 23])
        | (Function::Multiply, [1700, 1700])
        | (Function::Hour, [1114]) => 1700,
        (
            Function::Greater | Function::Less | Function::GreaterEqual | Function::LessEqual,
            [1700, 1700],
        )
        | (Function::TextEqual, [25 | 1043, 25 | 1043]) => 16,
        (Function::Length, [25 | 1043]) | (Function::Subtract, [23, 23]) => 23,
        (Function::Replace, [25 | 1043, 25, 25]) | (Function::Lower, [25 | 1043]) => 25,
        (Function::RegexMatch, [25 | 1043, 25]) => 1009,
        (Function::Utc, [1184]) => 1114,
        _ => anyhow::bail!("SQL type: unsupported terminal built-in signature"),
    };
    Ok(result)
}
impl Function {
    pub(super) fn signature(self, arguments: &[u32]) -> Result<Signature> {
        call_type(self, arguments)?;
        Ok(match self {
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
            Self::Hour => ("pg_catalog.extract(text,timestamp)", "extract_timestamp"),
            Self::Utc => ("pg_catalog.timezone(text,timestamptz)", "timestamptz_zone"),
            Self::Lower => ("pg_catalog.lower(text)", "lower"),
            Self::TextEqual => ("pg_catalog.texteq(text,text)", "texteq"),
            Self::RegexMatch => ("pg_catalog.regexp_match(text,text)", "regexp_match_no_flags"),
        })
    }
    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::Numeric => "numeric",
            Self::Multiply => "numeric_mul",
            Self::Greater => "numeric_gt",
            Self::Less => "numeric_lt",
            Self::GreaterEqual => "numeric_ge",
            Self::LessEqual => "numeric_le",
            Self::Length => "length",
            Self::Replace => "replace",
            Self::Subtract => "int4mi",
            Self::Hour => "extract",
            Self::Utc => "timezone",
            Self::Lower => "lower",
            Self::TextEqual => "texteq",
            Self::RegexMatch => "regexp_match",
        }
    }
}
pub fn implementation_types(implementation: &str) -> Result<(&'static str, &'static str, bool)> {
    let (args, result) = match implementation {
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
        _ => anyhow::bail!("unknown terminal program implementation"),
    };
    Ok((args, result, true))
}
