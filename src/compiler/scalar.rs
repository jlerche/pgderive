//! Typed pure native scalar expressions evaluated before relational state.
use super::ColumnRef;
use anyhow::{Context, Result};
use serde::Serialize;
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Temporal {
    pub(crate) input: ColumnRef,
    pub(crate) bin: Option<(i64, crate::temporal::Timestamp)>,
    pub(crate) subtract: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) shift: Option<(ColumnRef, i64)>,
}
#[derive(Clone, Serialize)]
pub struct Computed {
    pub(crate) column: ColumnRef,
    pub(crate) expression: super::typed::Typed,
}
/// Intern a fully bound scalar after its caller establishes the scope/row layout.
pub(super) fn register(
    typed: super::typed::Typed,
    computed: &mut Vec<Computed>,
    prefix: &str,
) -> Result<ColumnRef> {
    use sha2::{Digest, Sha256};
    let name = format!("{prefix}{:x}", Sha256::digest(serde_json::to_vec(&typed)?));
    let column = ColumnRef { name, right: false, oid: typed.oid(), nullable: typed.nullable() };
    if !computed.iter().any(|prior| prior.column == column) {
        computed.push(Computed { column: column.clone(), expression: typed });
    }
    Ok(column)
}
impl Temporal {
    pub(super) fn evaluate(
        &self,
        row: &crate::transaction::Row,
    ) -> Result<Option<crate::temporal::Timestamp>> {
        row.get(&self.input.name)
            .context("missing temporal input")?
            .as_deref()
            .map(|text| {
                let mut value = crate::temporal::Timestamp::parse(text, self.input.oid)?;
                if let Some((stride, origin)) = self.bin {
                    value = value.bin(stride, origin)?;
                }
                let subtract = if let Some((index, duration)) = &self.shift {
                    row.get(&index.name)
                        .and_then(Option::as_deref)
                        .context("missing hopping ordinal")?
                        .parse::<i64>()?
                        .checked_mul(*duration)
                        .context("hopping interval overflow")?
                } else {
                    self.subtract
                };
                value.subtract_duration(subtract)
            })
            .transpose()
    }
}
