//! Explicit `pg_catalog` signatures for pure, UTF8, locally evaluated functions.
use super::{ColumnRef, Name, Typed, Value, collation::Derivation};
use crate::compiler::capabilities as registry;
use anyhow::{Context, Result, ensure};
use pg_query::protobuf as pg;
use serde::Serialize;
mod kernels;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(super) struct Call {
    pub(super) operation: crate::compiler::capabilities::Builtin,
    pub(super) input_oid: u32,
    pub(super) arguments: Vec<Typed>,
    // Case conversion uses the SQL input derivation before expression rewrites.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) result_collation: Option<Derivation>,
}
pub(super) fn bind(
    function: &pg::FuncCall,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    use crate::compiler::parser::expressions::{builtin_name, names};
    crate::compiler::parser::scalar::validate_call(function)?;
    let names = names(&function.funcname)?.0;
    if names == [crate::compiler::subquery::CHECK] {
        return bind_cardinality_check(&function.args, resolve, depth);
    }
    let name = builtin_name(&names).unwrap_or("");
    if name == "mod" {
        let [left, right] = function.args.as_slice() else {
            anyhow::bail!("function mod: unsupported argument count {}", function.args.len());
        };
        return super::remainder(
            super::bind(left, resolve, depth + 1)?,
            super::bind(right, resolve, depth + 1)?,
        );
    }
    if let Some((operation, minimum)) = registry::text_join(name) {
        ensure!(
            (minimum..=64).contains(&function.args.len()),
            "function {name}: unsupported argument count {}",
            function.args.len()
        );
        let arguments = bind_arguments(function, resolve, depth)?;
        ensure!(
            arguments.iter().all(|value| matches!(value.oid, 0 | 25 | 705)),
            "function {name} requires text arguments"
        );
        let arguments = arguments.into_iter().map(Typed::unknown_text).collect();
        return Ok(Typed {
            oid: 25,
            value: Value::Call(Call {
                operation,
                input_oid: 25,
                arguments,
                result_collation: None,
            }),
        });
    }
    ensure!(registry::contains(name), "function {} is not available in-engine", names.join("."));
    registry::validate_arity(name, function.args.len())?;
    bind_signature(name, bind_arguments(function, resolve, depth)?)
}

fn bind_cardinality_check(
    arguments: &[pg_query::Node],
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    let [value, multiple] = arguments else {
        anyhow::bail!("missing scalar cardinality operands");
    };
    let value = super::bind(value, resolve, depth + 1)?;
    let multiple = super::bind(multiple, resolve, depth + 1)?;
    let (Value::Column(value), Value::Column(multiple)) = (value.value, multiple.value) else {
        anyhow::bail!("scalar cardinality requires bound columns");
    };
    ensure!(multiple.oid == 16, "invalid scalar cardinality marker");
    Ok(Typed { oid: value.oid, value: Value::Scalar { value, multiple } })
}

fn bind_signature(name: &str, arguments: Vec<Typed>) -> Result<Typed> {
    let types = arguments.iter().map(|value| value.oid).collect::<Vec<_>>();
    let signature = registry::resolve(name, &types)?;
    if matches!(signature.operation, registry::Builtin::Extract | registry::Builtin::DateTrunc) {
        validate_calendar(&arguments, signature)?;
    }
    let result_collation = if signature.operation.is_ascii_case_conversion() {
        let input = arguments.first().context("missing case conversion input")?;
        let collation = input.derive_collation(&|column| Ok(column.collation))?;
        ensure!(
            matches!(collation.oid()?, 950 | 951),
            "function {name} requires C/POSIX collation"
        );
        Some(collation)
    } else {
        None
    };
    let arguments =
        if matches!(signature.operation, registry::Builtin::Sign | registry::Builtin::Div) {
            arguments.into_iter().map(numeric_argument).collect::<Result<_>>()?
        } else {
            arguments
        };
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
        result_collation,
    });
    Ok(Typed { oid: signature.result, value })
}
pub(in crate::compiler) fn like_pattern(
    value: &pg_query::Node,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<Typed> {
    use crate::compiler::parser::expressions::{names, node};
    use pg_query::NodeEnum;
    if let NodeEnum::FuncCall(call) = node(value)?
        && names(&call.funcname)?.0 == ["pg_catalog", "like_escape"]
    {
        crate::compiler::parser::scalar::validate_call(call)?;
        ensure!(call.args.len() == 2, "LIKE ESCAPE requires two arguments");
        let arguments = call
            .args
            .iter()
            .map(|value| super::operand(value, resolve)?.coerce(25))
            .collect::<Result<Vec<_>>>()?;
        return Ok(Typed {
            oid: 25,
            value: Value::Call(Call {
                operation: registry::Builtin::LikeEscape,
                input_oid: 25,
                arguments,
                result_collation: None,
            }),
        });
    }
    super::operand(value, resolve)?.coerce(25)
}

fn bind_arguments(
    function: &pg::FuncCall,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Vec<Typed>> {
    function.args.iter().map(|value| super::bind(value, resolve, depth + 1)).collect()
}

fn numeric_argument(input: Typed) -> Result<Typed> {
    match input.oid {
        0 | 705 => Typed::contextual_literal(input, 1700),
        20 | 21 | 23 => Ok(input.widened_to_numeric()),
        _ => Ok(input),
    }
}

impl Call {
    pub(super) fn is_constant(&self) -> bool {
        // PostgreSQL treats these text joins as STABLE during planning.
        !matches!(self.operation, registry::Builtin::Concat | registry::Builtin::ConcatWs)
            && self.arguments.iter().all(Typed::is_constant)
    }

    pub(super) fn nullable(&self) -> bool {
        match self.operation {
            registry::Builtin::Concat => false,
            registry::Builtin::ConcatWs => self.arguments.first().is_none_or(Typed::nullable),
            registry::Builtin::Extract => true,
            _ => self.arguments.iter().any(Typed::nullable),
        }
    }

    pub(super) fn evaluate(
        &self,
        rows: (&crate::transaction::Row, &crate::transaction::Row),
    ) -> Result<Option<String>> {
        // Evaluate every argument before strict NULL propagation, as PostgreSQL does.
        let values =
            self.arguments.iter().map(|value| value.text_pair(rows)).collect::<Result<Vec<_>>>()?;
        if !self.operation.is_strict() {
            return kernels::join_text(self.operation, &values);
        }
        let Some(arguments) = values.into_iter().collect::<Option<Vec<_>>>() else {
            return Ok(None);
        };
        kernels::evaluate(self.operation, &arguments, self.input_oid)
    }
}

fn validate_calendar(arguments: &[Typed], signature: &registry::Signature) -> Result<()> {
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
