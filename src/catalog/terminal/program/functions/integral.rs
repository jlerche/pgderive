//! `PostgreSQL` integral arithmetic uses exact resolved-width built-ins.
use super::Signature;
use anyhow::Result;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    Add,
    Subtract,
    Multiply,
}
pub(super) const fn name(operation: Operation, oid: u32) -> &'static str {
    match (operation, oid) {
        (Operation::Add, 21) => "int2pl",
        (Operation::Add, 23) => "int4pl",
        (Operation::Add, _) => "int8pl",
        (Operation::Subtract, 21) => "int2mi",
        (Operation::Subtract, 23) => "int4mi",
        (Operation::Subtract, _) => "int8mi",
        (Operation::Multiply, 21) => "int2mul",
        (Operation::Multiply, 23) => "int4mul",
        (Operation::Multiply, _) => "int8mul",
    }
}
pub(super) fn signature(operation: Operation, oid: u32) -> Result<Signature> {
    let signature = match (operation, oid) {
        (Operation::Add, 21) => "pg_catalog.int2pl(smallint,smallint)",
        (Operation::Add, 23) => "pg_catalog.int4pl(integer,integer)",
        (Operation::Add, 20) => "pg_catalog.int8pl(bigint,bigint)",
        (Operation::Subtract, 21) => "pg_catalog.int2mi(smallint,smallint)",
        (Operation::Subtract, 23) => "pg_catalog.int4mi(integer,integer)",
        (Operation::Subtract, 20) => "pg_catalog.int8mi(bigint,bigint)",
        (Operation::Multiply, 21) => "pg_catalog.int2mul(smallint,smallint)",
        (Operation::Multiply, 23) => "pg_catalog.int4mul(integer,integer)",
        (Operation::Multiply, 20) => "pg_catalog.int8mul(bigint,bigint)",
        _ => anyhow::bail!("invalid integral arithmetic width"),
    };
    Ok((signature, name(operation, oid)))
}
pub(super) fn implementation_types(
    implementation: &str,
) -> Result<(&'static str, &'static str, bool)> {
    let (arguments, result) = match implementation {
        "int2pl" | "int2mi" | "int2mul" => ("21 21", "21"),
        "int4pl" | "int4mi" | "int4mul" => ("23 23", "23"),
        "int8pl" | "int8mi" | "int8mul" => ("20 20", "20"),
        _ => anyhow::bail!("unknown terminal program implementation"),
    };
    Ok((arguments, result, true))
}
