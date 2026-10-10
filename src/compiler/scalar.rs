//! Typed pure native scalar expressions evaluated before relational state.
use super::ColumnRef;
use anyhow::{Context, Result, ensure};
use serde::Serialize;
#[derive(Clone, Serialize)]
pub struct DateBin {
    pub(crate) input: ColumnRef,
    pub(crate) stride: i64,
    pub(crate) origin: crate::temporal::Timestamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) shift: Option<(ColumnRef, i64)>,
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
                let value = value.bin(self.stride, self.origin)?;
                let value = if let Some((index, duration)) = &self.shift {
                    let index = row
                        .get(&index.name)
                        .and_then(Option::as_deref)
                        .context("missing hopping ordinal")?
                        .parse::<i64>()?;
                    value.subtract_duration(
                        index.checked_mul(*duration).context("hopping interval overflow")?,
                    )?
                } else {
                    value
                };
                value.text(self.input.oid)
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
    let shift = parsed
        .shift
        .as_ref()
        .map(|(name, duration)| -> Result<_> {
            let index = resolve(name)?;
            ensure!(
                index.name == super::expansion::FIELD,
                "hopping offset requires bounded generated ordinal"
            );
            Ok((index, *duration))
        })
        .transpose()?;
    let expression = DateBin { input: input.clone(), stride: parsed.stride, origin, shift };
    let name = format!("@scalar_{:x}", Sha256::digest(serde_json::to_vec(&expression)?));
    let column = ColumnRef { name, right: false, oid: input.oid, nullable: input.nullable };
    if !computed.iter().any(|prior| prior.column == column) {
        computed.push(Computed { column: column.clone(), expression });
    }
    Ok(column)
}
