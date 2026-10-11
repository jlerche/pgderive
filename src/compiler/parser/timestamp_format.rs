//! Closed calendar fields; no locale names, arbitrary time zones or runtime formats.
use super::{Name, column, expressions::names, node};
use crate::catalog::terminal::Transform;
use anyhow::{Result, ensure};
use pg_query::{NodeEnum, protobuf as pg};
pub(super) fn target(call: &pg::FuncCall) -> Result<(Name, Transform, String)> {
    super::scalar::validate_call(call)?;
    let [input, pattern] = call.args.as_slice() else {
        anyhow::bail!("to_char requires timestamp and literal date/minute format");
    };
    let pattern = super::scalar::string(pattern)?;
    ensure!(matches!(pattern.as_str(), "YYYY-MM-DD" | "HH24:MI"), "unsupported calendar format");
    let (input, utc) = if let NodeEnum::FuncCall(zone) = node(input)? {
        let function = names(&zone.funcname)?.0;
        ensure!(
            function == ["timezone"] || function == ["pg_catalog", "timezone"],
            "timestamp formatting requires explicit UTC timezone"
        );
        ensure!(
            matches!(zone.funcformat, value if value == i32::from(pg::CoercionForm::CoerceSqlSyntax)
            || value == i32::from(pg::CoercionForm::CoerceExplicitCall)),
            "invalid timezone syntax"
        );
        let mut normalized = zone.as_ref().clone();
        normalized.funcformat = i32::from(pg::CoercionForm::CoerceExplicitCall);
        super::scalar::validate_call(&normalized)?;
        let [timezone, input] = zone.args.as_slice() else {
            anyhow::bail!("timezone requires UTC literal and native timestamptz column");
        };
        ensure!(
            super::scalar::string(timezone)? == "UTC",
            "only explicit UTC formatting supported"
        );
        (column(input)?, true)
    } else {
        (column(input)?, false)
    };
    Ok((input, Transform::TimestampFormat { pattern, utc }, "to_char".into()))
}
