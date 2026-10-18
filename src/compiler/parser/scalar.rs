use super::{Name, column, node, optional};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
#[derive(Clone)]
pub(in crate::compiler) enum Key {
    Typed(Box<Node>),
    Column(Name),
}
pub(in crate::compiler) fn is_bin(value: &Node) -> Result<bool> {
    if let NodeEnum::AExpr(expression) = node(value)? {
        return is_bin(optional(expression.lexpr.as_deref())?);
    }
    let NodeEnum::FuncCall(call) = node(value)? else {
        return Ok(false);
    };
    let names = super::expressions::names(&call.funcname)?.0;
    Ok(super::expressions::builtin_name(&names) == Some("date_bin"))
}
pub(super) fn key(value: &Node) -> Result<Key> {
    if matches!(node(value)?, NodeEnum::ColumnRef(_)) {
        Ok(Key::Column(column(value)?))
    } else {
        Ok(Key::Typed(Box::new(value.clone())))
    }
}
pub(in crate::compiler) fn bin(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<crate::compiler::ColumnRef>,
) -> Result<crate::compiler::scalar::Temporal> {
    if let NodeEnum::AExpr(_) = node(value)? {
        return shifted(value, resolve);
    }
    let NodeEnum::FuncCall(call) = node(value)? else {
        anyhow::bail!("invalid date_bin call");
    };
    validate_call(call)?;
    let [stride, input, origin] = call.args.as_slice() else {
        anyhow::bail!("date_bin requires stride, native column and typed origin");
    };
    let stride = interval(stride, true)?;
    let input = resolve(&column(input)?)?;
    let origin = bin_origin(origin, input.oid)?;
    Ok(crate::compiler::scalar::Temporal {
        input,
        bin: Some((stride, origin)),
        shift: None,
        subtract: 0,
    })
}
fn bin_origin(value: &Node, input_oid: u32) -> Result<crate::temporal::Timestamp> {
    let NodeEnum::TypeCast(cast) = node(value)? else {
        anyhow::bail!("date_bin origin requires explicit timestamp type");
    };
    let names =
        super::expressions::names(&cast.type_name.as_ref().context("missing origin type")?.names)?
            .0;
    let oid = match super::expressions::builtin_name(&names) {
        Some("timestamp") => 1114,
        Some("timestamptz") => 1184,
        _ => anyhow::bail!("date_bin origin requires timestamp/timestamptz"),
    };
    let typename = cast.type_name.as_ref().context("missing origin type")?;
    ensure!(
        crate::compiler::parser::expressions::unmodified_type(typename),
        "origin type modifiers unsupported"
    );
    ensure!(input_oid == oid, "date_bin origin must match native timestamp type");
    let origin = crate::temporal::Timestamp::parse(&string(optional(cast.arg.as_deref())?)?, oid)?;
    ensure!(
        origin.sort_value() != i64::MIN && origin.sort_value() != i64::MAX,
        "date_bin origin must be finite"
    );
    Ok(origin)
}
pub(in crate::compiler) fn validate_call(call: &pg::FuncCall) -> Result<()> {
    ensure!(
        !call.agg_star
            && !call.agg_distinct
            && !call.agg_within_group
            && !call.func_variadic
            && call.agg_order.is_empty()
            && call.agg_filter.is_none()
            && call.over.is_none()
            && call.funcformat == i32::from(pg::CoercionForm::CoerceExplicitCall),
        "scalar/series modifiers unsupported"
    );
    Ok(())
}
fn operator<'a>(value: &'a Node, op: &str) -> Result<(&'a Node, &'a Node)> {
    let NodeEnum::AExpr(expression) = node(value)? else {
        anyhow::bail!("hopping arithmetic requires an operator");
    };
    ensure!(
        expression.kind == i32::from(pg::AExprKind::AexprOp)
            && super::expressions::names(&expression.name)?.0 == [op],
        "unsupported hopping operator"
    );
    Ok((optional(expression.lexpr.as_deref())?, optional(expression.rexpr.as_deref())?))
}
fn shifted(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<crate::compiler::ColumnRef>,
) -> Result<crate::compiler::scalar::Temporal> {
    let (base, offset) = operator(value, "-")?;
    let mut bin = bin(base, resolve)?;
    ensure!(bin.shift.is_none(), "nested hopping offsets unsupported");
    let (index, duration) = operator(offset, "*")?;
    ensure!(
        matches!(node(duration)?, NodeEnum::TypeCast(_)),
        "hopping offset requires explicit interval"
    );
    let index = resolve(&column(index)?)?;
    ensure!(
        index.name == crate::compiler::expansion::FIELD,
        "hopping offset requires bounded generated ordinal"
    );
    bin.shift = Some((index, interval(duration, false)?));
    Ok(bin)
}
pub(in crate::compiler) fn string(value: &Node) -> Result<String> {
    let NodeEnum::AConst(value) = node(value)? else {
        anyhow::bail!("constant string required");
    };
    let Some(pg::a_const::Val::Sval(value)) = &value.val else {
        anyhow::bail!("constant string required");
    };
    ensure!(!value.sval.is_empty(), "empty scalar constant");
    Ok(value.sval.clone())
}
pub(in crate::compiler) fn interval(value: &Node, calendar_days: bool) -> Result<i64> {
    let text = if let NodeEnum::TypeCast(cast) = node(value)? {
        let typename = cast.type_name.as_ref().context("missing interval type")?;
        let names = super::expressions::names(&typename.names)?.0;
        ensure!(
            super::expressions::builtin_name(&names) == Some("interval"),
            "date_bin stride requires interval"
        );
        ensure!(
            crate::compiler::parser::expressions::unmodified_type(typename),
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
        "day" | "days" if calendar_days => 86_400_000_000,
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
