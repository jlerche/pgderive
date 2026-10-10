use super::{Name, column, node, optional};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
pub(in crate::compiler) struct Bin {
    pub input: Name,
    pub stride: i64,
    pub origin: String,
    pub oid: u32,
}
pub(in crate::compiler) enum Key {
    Column(Name),
    Bin(Bin),
}
pub(super) fn is_bin(value: &Node) -> Result<bool> {
    let NodeEnum::FuncCall(call) = node(value)? else {
        return Ok(false);
    };
    let names = super::expressions::names(&call.funcname)?.0;
    Ok(matches!(names.as_slice(), [name] if name == "date_bin")
        || matches!(names.as_slice(), [schema,name] if schema == "pg_catalog" && name == "date_bin"))
}
pub(super) fn key(value: &Node) -> Result<Key> {
    if !is_bin(value)? {
        return Ok(Key::Column(column(value)?));
    }
    let NodeEnum::FuncCall(call) = node(value)? else {
        anyhow::bail!("invalid date_bin call");
    };
    ensure!(
        !call.agg_star
            && !call.agg_distinct
            && !call.agg_within_group
            && !call.func_variadic
            && call.agg_order.is_empty()
            && call.agg_filter.is_none()
            && call.over.is_none()
            && call.funcformat == i32::from(pg::CoercionForm::CoerceExplicitCall),
        "date_bin modifiers unsupported"
    );
    let [stride, input, origin] = call.args.as_slice() else {
        anyhow::bail!("date_bin requires stride, native column and typed origin");
    };
    let stride = interval(stride)?;
    let NodeEnum::TypeCast(cast) = node(origin)? else {
        anyhow::bail!("date_bin origin requires explicit timestamp type");
    };
    let names =
        super::expressions::names(&cast.type_name.as_ref().context("missing origin type")?.names)?
            .0;
    let oid = match names.as_slice() {
        [schema, name] if schema == "pg_catalog" && name == "timestamp" => 1114,
        [schema, name] if schema == "pg_catalog" && name == "timestamptz" => 1184,
        [name] if name == "timestamp" => 1114,
        [name] if name == "timestamptz" => 1184,
        _ => anyhow::bail!("date_bin origin requires timestamp/timestamptz"),
    };
    let typename = cast.type_name.as_ref().context("missing origin type")?;
    ensure!(
        typename.typmods.is_empty()
            && typename.array_bounds.is_empty()
            && !typename.setof
            && !typename.pct_type,
        "origin type modifiers unsupported"
    );
    Ok(Key::Bin(Bin {
        input: column(input)?,
        stride,
        origin: string(optional(cast.arg.as_deref())?)?,
        oid,
    }))
}
fn string(value: &Node) -> Result<String> {
    let NodeEnum::AConst(value) = node(value)? else {
        anyhow::bail!("constant string required");
    };
    let Some(pg::a_const::Val::Sval(value)) = &value.val else {
        anyhow::bail!("constant string required");
    };
    ensure!(!value.sval.is_empty(), "empty scalar constant");
    Ok(value.sval.clone())
}
fn interval(value: &Node) -> Result<i64> {
    let text = if let NodeEnum::TypeCast(cast) = node(value)? {
        let typename = cast.type_name.as_ref().context("missing interval type")?;
        let names = super::expressions::names(&typename.names)?.0;
        ensure!(
            matches!(names.as_slice(), [name] if name == "interval")
                || matches!(names.as_slice(), [schema,name] if schema == "pg_catalog" && name == "interval"),
            "date_bin stride requires interval"
        );
        ensure!(
            typename.typmods.is_empty()
                && typename.array_bounds.is_empty()
                && !typename.setof
                && !typename.pct_type,
            "interval modifiers unsupported"
        );
        string(optional(cast.arg.as_deref())?)?
    } else {
        string(value)?
    };
    let words = text.split_whitespace().collect::<Vec<_>>();
    let [quantity, unit] = words.as_slice() else {
        anyhow::bail!("stride requires one fixed duration quantity/unit");
    };
    let multiplier = match unit.to_ascii_lowercase().as_str() {
        "microsecond" | "microseconds" => 1,
        "millisecond" | "milliseconds" => 1000,
        "second" | "seconds" => 1_000_000,
        "minute" | "minutes" => 60_000_000,
        "hour" | "hours" => 3_600_000_000_i64,
        "day" | "days" => 86_400_000_000,
        _ => anyhow::bail!("date_bin stride must be fixed microseconds through days"),
    };
    let (whole, fraction) = quantity.split_once('.').unwrap_or((quantity, ""));
    ensure!(
        !whole.is_empty()
            && whole.bytes().chain(fraction.bytes()).all(|byte| byte.is_ascii_digit())
            && fraction.len() <= 6,
        "stride requires positive decimal with at most six fractional digits"
    );
    let numerator: num_bigint::BigInt = format!("{whole}{fraction}").parse()?;
    let scale = num_bigint::BigInt::from(10).pow(u32::try_from(fraction.len())?);
    let micros = numerator * multiplier;
    ensure!(
        &micros % &scale == num_bigint::BigInt::from(0),
        "stride requires integral microseconds"
    );
    let stride = i64::try_from(micros / scale).context("date_bin interval out of range")?;
    ensure!(stride > 0, "date_bin stride must be positive");
    Ok(stride)
}
