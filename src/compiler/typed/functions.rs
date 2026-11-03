//! Explicit `pg_catalog` signatures for pure, UTF8, locally evaluated functions.
use super::{ColumnRef, Name, Typed, Value};
use crate::compiler::capabilities as registry;
use anyhow::{Result, ensure};
use pg_query::protobuf as pg;
use serde::Serialize;
mod kernels;

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
    if matches!(signature.operation, registry::Builtin::Extract | registry::Builtin::DateTrunc) {
        validate_calendar(&arguments, signature)?;
    }
    let value = Value::Call(Call {
        operation: signature.operation,
        input_oid: if matches!(
            signature.operation,
            registry::Builtin::Extract | registry::Builtin::DateTrunc
        ) {
            signature.inputs[1]
        } else {
            signature.inputs[0]
        },
        arguments,
    });
    Ok(Typed { oid: signature.result, value })
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
        kernels::evaluate(self.operation, &arguments, self.input_oid)
    }
}

fn validate_calendar(arguments: &[Typed], signature: &registry::Signature) -> Result<()> {
    use anyhow::Context;
    let field = arguments.first().context("missing calendar field")?;
    ensure!(field.is_constant(), "calendar field must be constant");
    if let Some(field) = field.evaluate(&crate::transaction::Row::new())? {
        crate::temporal::validate_field(
            &field.to_ascii_lowercase(),
            signature.inputs[1],
            signature.operation == registry::Builtin::DateTrunc,
        )?;
    }
    if let Some(zone) = arguments.get(2) {
        ensure!(zone.is_constant(), "date_trunc zone must be constant UTC");
        if let Some(zone) = zone.evaluate(&crate::transaction::Row::new())? {
            ensure!(zone.eq_ignore_ascii_case("UTC"), "date_trunc named zones are unsupported");
        }
    }
    Ok(())
}
