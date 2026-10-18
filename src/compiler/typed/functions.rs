//! Explicit `pg_catalog` signatures for pure, UTF8, locally evaluated functions.
use super::{ColumnRef, Name, Typed, Value};
use crate::compiler::capabilities as registry;
use anyhow::{Result, ensure};
use pg_query::protobuf as pg;
use serde::Serialize;
mod strings;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(super) struct Call {
    pub(super) operation: crate::compiler::capabilities::Builtin,
    pub(super) input_oid: u32,
    pub(super) arguments: Vec<Typed>,
}
pub(super) fn bind(
    function: &pg::FuncCall,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    use crate::compiler::parser::expressions::{builtin_name, names};
    crate::compiler::parser::scalar::validate_call(function)?;
    let names = names(&function.funcname)?.0;
    let name = builtin_name(&names).unwrap_or("");
    ensure!(registry::contains(name), "function {} is not available in-engine", names.join("."));
    registry::validate_arity(name, function.args.len())?;
    let arguments = function
        .args
        .iter()
        .map(|value| super::bind(value, resolve, depth + 1))
        .collect::<Result<Vec<_>>>()?;
    let types = arguments.iter().map(|value| value.oid).collect::<Vec<_>>();
    let signature = registry::resolve(name, &types)?;
    let value = if constant_null(&arguments)? {
        Value::Literal(None)
    } else {
        Value::Call(Call {
            operation: signature.operation,
            input_oid: signature.inputs[0],
            arguments,
        })
    };
    Ok(Typed { oid: signature.result, value })
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
        strings::evaluate(self.operation, &arguments, self.input_oid).map(Some)
    }
}
