//! Closed local function capabilities: overloads, coercions and resolved kernels.
use anyhow::{Result, ensure};
use serde::Serialize;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(in crate::compiler) enum Builtin {
    Length,
    OctetLength,
    BitLength,
    Ascii,
    Chr,
    Reverse,
    Left,
    Right,
    Ltrim,
    Rtrim,
    Btrim,
    Replace,
    Translate,
    Strpos,
    StartsWith,
    Repeat,
    Lpad,
    Rpad,
    SplitPart,
    Substring,
    Hex,
    Abs,
    Round,
    Trunc,
    Ceiling,
    Floor,
    Extract,
    DateTrunc,
    Sign,
    Div,
}
pub(in crate::compiler) struct Signature {
    pub name: &'static str,
    pub inputs: &'static [u32],
    pub result: u32,
    pub operation: Builtin,
}
impl Signature {
    const fn strict(
        name: &'static str,
        inputs: &'static [u32],
        result: u32,
        operation: Builtin,
    ) -> Self {
        Self { name, inputs, result, operation }
    }
}
const SIGNATURES: &[Signature] = &[
    Signature::strict("extract", &[25, 1082], 1700, Builtin::Extract),
    Signature::strict("date_trunc", &[25, 1082], 1184, Builtin::DateTrunc),
    Signature::strict("extract", &[25, 1114], 1700, Builtin::Extract),
    Signature::strict("date_trunc", &[25, 1114], 1114, Builtin::DateTrunc),
    Signature::strict("extract", &[25, 1184], 1700, Builtin::Extract),
    Signature::strict("date_trunc", &[25, 1184], 1184, Builtin::DateTrunc),
    Signature::strict("extract", &[25, 1186], 1700, Builtin::Extract),
    Signature::strict("date_trunc", &[25, 1186], 1186, Builtin::DateTrunc),
    Signature::strict("date_trunc", &[25, 1184, 25], 1184, Builtin::DateTrunc),
    Signature::strict("length", &[25], 23, Builtin::Length),
    Signature::strict("char_length", &[25], 23, Builtin::Length),
    Signature::strict("character_length", &[25], 23, Builtin::Length),
    Signature::strict("octet_length", &[25], 23, Builtin::OctetLength),
    Signature::strict("bit_length", &[25], 23, Builtin::BitLength),
    Signature::strict("ascii", &[25], 23, Builtin::Ascii),
    Signature::strict("chr", &[23], 25, Builtin::Chr),
    Signature::strict("reverse", &[25], 25, Builtin::Reverse),
    Signature::strict("left", &[25, 23], 25, Builtin::Left),
    Signature::strict("right", &[25, 23], 25, Builtin::Right),
    Signature::strict("ltrim", &[25], 25, Builtin::Ltrim),
    Signature::strict("ltrim", &[25, 25], 25, Builtin::Ltrim),
    Signature::strict("rtrim", &[25], 25, Builtin::Rtrim),
    Signature::strict("rtrim", &[25, 25], 25, Builtin::Rtrim),
    Signature::strict("btrim", &[25], 25, Builtin::Btrim),
    Signature::strict("btrim", &[25, 25], 25, Builtin::Btrim),
    Signature::strict("replace", &[25, 25, 25], 25, Builtin::Replace),
    Signature::strict("translate", &[25, 25, 25], 25, Builtin::Translate),
    Signature::strict("strpos", &[25, 25], 23, Builtin::Strpos),
    Signature::strict("starts_with", &[25, 25], 16, Builtin::StartsWith),
    Signature::strict("lpad", &[25, 23], 25, Builtin::Lpad),
    Signature::strict("lpad", &[25, 23, 25], 25, Builtin::Lpad),
    Signature::strict("rpad", &[25, 23], 25, Builtin::Rpad),
    Signature::strict("rpad", &[25, 23, 25], 25, Builtin::Rpad),
    Signature::strict("repeat", &[25, 23], 25, Builtin::Repeat),
    Signature::strict("split_part", &[25, 25, 23], 25, Builtin::SplitPart),
    Signature::strict("substr", &[25, 23], 25, Builtin::Substring),
    Signature::strict("substr", &[25, 23, 23], 25, Builtin::Substring),
    Signature::strict("substring", &[25, 23], 25, Builtin::Substring),
    Signature::strict("substring", &[25, 23, 23], 25, Builtin::Substring),
    Signature::strict("to_hex", &[23], 25, Builtin::Hex),
    Signature::strict("to_hex", &[20], 25, Builtin::Hex),
    Signature::strict("abs", &[21], 21, Builtin::Abs),
    Signature::strict("abs", &[23], 23, Builtin::Abs),
    Signature::strict("abs", &[20], 20, Builtin::Abs),
    Signature::strict("abs", &[1700], 1700, Builtin::Abs),
    Signature::strict("round", &[1700], 1700, Builtin::Round),
    Signature::strict("round", &[1700, 23], 1700, Builtin::Round),
    Signature::strict("trunc", &[1700], 1700, Builtin::Trunc),
    Signature::strict("trunc", &[1700, 23], 1700, Builtin::Trunc),
    Signature::strict("ceil", &[1700], 1700, Builtin::Ceiling),
    Signature::strict("ceiling", &[1700], 1700, Builtin::Ceiling),
    Signature::strict("floor", &[1700], 1700, Builtin::Floor),
    Signature::strict("sign", &[1700], 1700, Builtin::Sign),
    Signature::strict("div", &[1700, 1700], 1700, Builtin::Div),
];
pub(in crate::compiler) fn contains(name: &str) -> bool {
    SIGNATURES.iter().any(|value| value.name == name)
}
pub fn names() -> Vec<&'static str> {
    let mut names = SIGNATURES.iter().map(|value| value.name).collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();
    names
}
pub(in crate::compiler) fn validate_arity(name: &str, arity: usize) -> Result<()> {
    ensure!(
        SIGNATURES.iter().any(|value| value.name == name && value.inputs.len() == arity),
        "function {name}: unsupported argument count {arity}"
    );
    Ok(())
}
pub(in crate::compiler) fn resolve(name: &str, types: &[u32]) -> Result<&'static Signature> {
    ensure!(
        !(matches!(name, "extract" | "date_trunc")
            && types.get(1).is_some_and(|oid| matches!(oid, 0 | 705))),
        "ambiguous untyped calendar input"
    );
    ensure!(
        !(name == "substring"
            && types.get(1) == Some(&0)
            && !types.get(2).is_some_and(|oid| matches!(oid, 21 | 23))),
        "function substring: untyped NULL position selects an unsupported regex overload; cast the position to integer"
    );
    ensure!(
        !(matches!(name, "round" | "trunc" | "ceil" | "ceiling" | "floor" | "sign")
            && types.len() == 1
            && types[0] != 1700),
        "function {name}: unary overload requires numeric; other PostgreSQL overloads are not available in-engine"
    );
    let signature = SIGNATURES.iter().find(|value| {
        value.name == name
            && value.inputs.len() == types.len()
            && value
                .inputs
                .iter()
                .zip(types)
                .all(|(expected, actual)| compatible(*expected, *actual, value.operation))
    });
    let Some(signature) = signature else {
        let arguments = types.iter().map(|oid| type_name(*oid)).collect::<Vec<_>>().join(", ");
        let supported = SIGNATURES
            .iter()
            .filter(|value| value.name == name)
            .map(|value| {
                format!(
                    "{}({})",
                    value.name,
                    value.inputs.iter().map(|oid| type_name(*oid)).collect::<Vec<_>>().join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        anyhow::bail!(
            "function {name}({arguments}) overload is not available in-engine; supported signatures: {supported}"
        );
    };
    Ok(signature)
}
const fn compatible(expected: u32, actual: u32, operation: Builtin) -> bool {
    if matches!(operation, Builtin::Abs | Builtin::Hex) {
        return expected == actual;
    }
    match expected {
        25 => matches!(actual, 0 | 25 | 1043 | 705),
        23 => matches!(actual, 0 | 21 | 23),
        1700 => matches!(actual, 0 | 20 | 21 | 23 | 705 | 1700),
        _ => expected == actual,
    }
}
pub(in crate::compiler) fn type_name(oid: u32) -> String {
    let name = match oid {
        0 => "untyped NULL",
        16 => "boolean",
        20 => "bigint",
        21 => "smallint",
        23 => "integer",
        25 => "text",
        1043 => "varchar",
        1700 => "numeric",
        705 => "unknown",
        1114 => "timestamp",
        1184 => "timestamptz",
        1186 => "interval",
        2950 => "uuid",
        _ => "unsupported type",
    };
    format!("{name} [OID {oid}]")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(in crate::compiler) enum Arithmetic {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    BitAnd,
    BitOr,
    BitXor,
    ShiftLeft,
    ShiftRight,
}
impl Arithmetic {
    pub(super) fn resolve(name: &str) -> Result<Self> {
        match name {
            "+" => Ok(Self::Add),
            "-" => Ok(Self::Subtract),
            "*" => Ok(Self::Multiply),
            "/" => Ok(Self::Divide),
            "%" => Ok(Self::Remainder),
            "&" => Ok(Self::BitAnd),
            "|" => Ok(Self::BitOr),
            "#" | "~" => Ok(Self::BitXor),
            "<<" => Ok(Self::ShiftLeft),
            ">>" => Ok(Self::ShiftRight),
            _ => anyhow::bail!(
                "operator {name} is not available in-engine; supported operators: +, -, *, /, %, &, |, #, ~, <<, >>"
            ),
        }
    }
    pub(super) const fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Remainder => "%",
            Self::BitAnd => "&",
            Self::BitOr => "|",
            Self::BitXor => "#",
            Self::ShiftLeft => "<<",
            Self::ShiftRight => ">>",
        }
    }
    pub(super) const fn is_shift(self) -> bool {
        matches!(self, Self::ShiftLeft | Self::ShiftRight)
    }
    pub(super) const fn is_bitwise(self) -> bool {
        self.is_shift() || matches!(self, Self::BitAnd | Self::BitOr | Self::BitXor)
    }
    pub(super) const fn integer(self, left: i64, right: i64) -> Option<i64> {
        match self {
            Self::Add => left.checked_add(right),
            Self::Subtract => left.checked_sub(right),
            Self::Multiply => left.checked_mul(right),
            Self::Divide => left.checked_div(right),
            Self::Remainder if right == -1 => Some(0),
            Self::Remainder => left.checked_rem(right),
            Self::BitAnd => Some(left & right),
            Self::BitOr => Some(left | right),
            Self::BitXor => Some(left ^ right),
            Self::ShiftLeft | Self::ShiftRight => None,
        }
    }
}

#[cfg(test)]
mod matrix_tests;
