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
    SplitPart,
    Substring,
    Hex,
    Abs,
}
pub(in crate::compiler) struct Signature {
    pub name: &'static str,
    pub inputs: &'static [u32],
    pub result: u32,
    pub operation: Builtin,
    pub strict: bool,
}
impl Signature {
    const fn strict(
        name: &'static str,
        inputs: &'static [u32],
        result: u32,
        operation: Builtin,
    ) -> Self {
        Self { name, inputs, result, operation, strict: true }
    }
}
const SIGNATURES: &[Signature] = &[
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
        !(name == "substring"
            && types.get(1) == Some(&0)
            && !types.get(2).is_some_and(|oid| matches!(oid, 21 | 23))),
        "function substring: untyped NULL position selects an unsupported regex overload; cast the position to integer"
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
}
impl Arithmetic {
    pub(super) fn resolve(name: &str) -> Result<Self> {
        match name {
            "+" => Ok(Self::Add),
            "-" => Ok(Self::Subtract),
            "*" => Ok(Self::Multiply),
            _ => anyhow::bail!(
                "operator {name} is not available in-engine; supported arithmetic: +, -, *"
            ),
        }
    }
    pub(super) const fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
        }
    }
    pub(super) const fn integer(self, left: i64, right: i64) -> Option<i64> {
        match self {
            Self::Add => left.checked_add(right),
            Self::Subtract => left.checked_sub(right),
            Self::Multiply => left.checked_mul(right),
        }
    }
}
