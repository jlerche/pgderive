//! Typed pure native scalar expressions evaluated before relational state.
use super::ColumnRef;
use anyhow::{Context, Result, ensure};
use serde::Serialize;
#[derive(Clone, Serialize)]
pub struct DateBin {
    pub(crate) input: ColumnRef,
    pub(crate) stride: i64,
    pub(crate) origin: crate::temporal::Timestamp,
}
#[derive(Clone, Serialize)]
pub struct Computed {
    pub(crate) column: ColumnRef,
    pub(crate) expression: DateBin,
}
impl DateBin {
    pub(crate) fn evaluate(&self, row: &crate::transaction::Row) -> Result<Option<String>> {
        row.get(&self.input.name)
            .context("missing date_bin input")?
            .as_deref()
            .map(|value| {
                let value = crate::temporal::Timestamp::parse(value, self.input.oid)?;
                value.bin(self.stride, self.origin)?.text(self.input.oid)
            })
            .transpose()
    }
}
pub(super) fn bind(
    parsed: &super::parser::scalar::Bin,
    resolve: &impl Fn(&super::parser::Name) -> Result<ColumnRef>,
    computed: &mut Vec<Computed>,
) -> Result<ColumnRef> {
    use sha2::{Digest, Sha256};
    let input = resolve(&parsed.input)?;
    ensure!(input.oid == parsed.oid, "date_bin origin must match native timestamp type");
    let origin = crate::temporal::Timestamp::parse(&parsed.origin, parsed.oid)?;
    ensure!(
        origin.sort_value() != i64::MIN && origin.sort_value() != i64::MAX,
        "date_bin origin must be finite"
    );
    let expression = DateBin { input: input.clone(), stride: parsed.stride, origin };
    let name = format!("@scalar_{:x}", Sha256::digest(serde_json::to_vec(&expression)?));
    let column = ColumnRef { name, right: false, oid: input.oid, nullable: input.nullable };
    if !computed.iter().any(|prior| prior.column == column) {
        computed.push(Computed { column: column.clone(), expression });
    }
    Ok(column)
}
