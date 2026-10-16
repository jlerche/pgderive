//! Deliberately bounded native coercions; unsupported overloads never guess.
use super::{ColumnRef, Name, Typed, Value};
use anyhow::{Context, Result, ensure};
use pg_query::protobuf as pg;

pub(super) fn common(types: &[u32]) -> Result<u32> {
    let known = types.iter().copied().filter(|oid| !matches!(oid, 0 | 705)).collect::<Vec<_>>();
    if known.is_empty() || known.iter().all(|oid| matches!(oid, 25 | 1043)) {
        // Unknown literals do not change a resolved varchar candidate.
        return Ok(if known.iter().all(|oid| *oid == 1043) && !known.is_empty() {
            1043
        } else {
            25
        });
    }
    ensure!(
        types.iter().all(|oid| matches!(oid, 0 | 20 | 21 | 23 | 1700)),
        "mixed scalar result types are not available in-engine"
    );
    Ok(super::numeric_common(types))
}

pub(super) fn cast(
    cast: &pg::TypeCast,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    use crate::compiler::parser::expressions::{builtin_name, names, optional};
    let kind = cast.type_name.as_ref().context("missing scalar cast type")?;
    ensure!(
        crate::compiler::parser::expressions::unmodified_type(kind),
        "unsupported scalar cast modifier"
    );
    let name = names(&kind.names)?.0;
    let oid = match builtin_name(&name).context("unsupported scalar cast namespace")? {
        "int2" => 21,
        "int4" => 23,
        "int8" => 20,
        "numeric" => 1700,
        "text" => 25,
        "varchar" => 1043,
        _ => anyhow::bail!("unsupported scalar cast"),
    };
    let argument = optional(cast.arg.as_deref())?;
    let input = if oid == 1700 {
        super::numeric_cast_input(argument, resolve, depth + 1)?
    } else {
        super::bind(argument, resolve, depth + 1)?
    };
    let valid = if matches!(oid, 25 | 1043) {
        matches!(input.oid, 0 | 25 | 1043 | 705)
    } else {
        matches!(input.oid, 0 | 20 | 21 | 23) || (oid == 1700 && input.oid == 1700)
    };
    ensure!(valid, "scalar cast from OID {} to OID {oid} is not available in-engine", input.oid);
    Ok(Typed { oid, value: Value::Cast(Box::new(input)) })
}
