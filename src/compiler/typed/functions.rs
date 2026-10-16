//! Explicit `pg_catalog` signatures for pure, UTF8, locally evaluated functions.
use super::{ColumnRef, Name, Typed, Value};
use anyhow::{Result, ensure};
use pg_query::protobuf as pg;
use serde::Serialize;
mod strings;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(super) struct Call {
    pub(super) name: String,
    pub(super) arguments: Vec<Typed>,
}
type Signature = (&'static str, &'static [u32], u32);
const SIGNATURES: &[Signature] = &[
    ("length", &[25], 23),
    ("char_length", &[25], 23),
    ("character_length", &[25], 23),
    ("octet_length", &[25], 23),
    ("bit_length", &[25], 23),
    ("ascii", &[25], 23),
    ("chr", &[23], 25),
    ("reverse", &[25], 25),
    ("left", &[25, 23], 25),
    ("right", &[25, 23], 25),
    ("ltrim", &[25], 25),
    ("ltrim", &[25, 25], 25),
    ("rtrim", &[25], 25),
    ("rtrim", &[25, 25], 25),
    ("btrim", &[25], 25),
    ("btrim", &[25, 25], 25),
    ("replace", &[25, 25, 25], 25),
    ("translate", &[25, 25, 25], 25),
    ("strpos", &[25, 25], 23),
    ("starts_with", &[25, 25], 16),
    ("repeat", &[25, 23], 25),
    ("split_part", &[25, 25, 23], 25),
    ("substr", &[25, 23], 25),
    ("substr", &[25, 23, 23], 25),
    ("substring", &[25, 23], 25),
    ("substring", &[25, 23, 23], 25),
    ("to_hex", &[23], 25),
    ("to_hex", &[20], 25),
    ("abs", &[21], 21),
    ("abs", &[23], 23),
    ("abs", &[20], 20),
];
pub(super) fn bind(
    function: &pg::FuncCall,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    use crate::compiler::parser::expressions::{builtin_name, names};
    crate::compiler::parser::scalar::validate_call(function)?;
    let names = names(&function.funcname)?.0;
    let name = builtin_name(&names).unwrap_or("");
    ensure!(
        SIGNATURES.iter().any(|(candidate, _, _)| *candidate == name),
        "function {} is not available in-engine",
        names.join(".")
    );
    ensure!(function.args.len() <= 3, "function {name}: unsupported argument count");
    let arguments = function
        .args
        .iter()
        .map(|value| super::bind(value, resolve, depth + 1))
        .collect::<Result<Vec<_>>>()?;
    let types = arguments.iter().map(|value| value.oid).collect::<Vec<_>>();
    ensure!(
        !(name == "substring"
            && types.get(1) == Some(&0)
            && !types.get(2).is_some_and(|oid| matches!(oid, 21 | 23))),
        "function substring: untyped NULL position selects an unsupported regex overload; cast the position to integer"
    );
    let signature = SIGNATURES.iter().find(|(candidate, inputs, _)| {
        *candidate == name
            && inputs.len() == types.len()
            && inputs
                .iter()
                .zip(&types)
                .all(|(expected, actual)| compatible(*expected, *actual, name))
    });
    let Some((_, _, oid)) = signature else {
        anyhow::bail!(
            "function {name}({types:?}) overload is not available in-engine (argument types are PostgreSQL OIDs)"
        );
    };
    let value = if constant_null(&arguments)? {
        Value::Literal(None)
    } else {
        Value::Call(Call { name: name.into(), arguments })
    };
    Ok(Typed { oid: *oid, value })
}
fn constant_null(arguments: &[Typed]) -> Result<bool> {
    // PostgreSQL folds strict calls with a constant NULL after simplifying their
    // arguments. Binding already checked constant subtrees for errors.
    for argument in arguments {
        if argument.is_constant() && argument.evaluate(&crate::transaction::Row::new())?.is_none() {
            return Ok(true);
        }
    }
    Ok(false)
}
fn compatible(expected: u32, actual: u32, name: &str) -> bool {
    if matches!(name, "abs" | "to_hex") {
        return expected == actual;
    }
    match expected {
        25 => matches!(actual, 0 | 25 | 1043 | 705),
        23 => matches!(actual, 0 | 21 | 23),
        _ => expected == actual,
    }
}
impl Call {
    pub(super) fn evaluate(
        &self,
        rows: (&crate::transaction::Row, &crate::transaction::Row),
    ) -> Result<Option<String>> {
        // Evaluate every argument before strict NULL propagation, as PostgreSQL does.
        let values =
            self.arguments.iter().map(|value| value.text_pair(rows)).collect::<Result<Vec<_>>>()?;
        let Some(arguments) = values.into_iter().collect::<Option<Vec<_>>>() else {
            return Ok(None);
        };
        strings::evaluate(
            &self.name,
            &arguments,
            self.arguments.first().map_or(0, |value| value.oid),
        )
        .map(Some)
    }
}
