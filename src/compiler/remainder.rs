//! Bounded integral MOD kernel shared by predicates and lookup-key maps.
use super::{ColumnRef, relational, scalar::Computed};
use crate::transaction::Row;
use anyhow::{Context, Result, ensure};
use serde::Serialize;
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Remainder {
    pub(super) input: ColumnRef,
    pub(super) divisor: i64,
}
impl Remainder {
    pub(super) fn integer_pair(&self, rows: (&Row, &Row)) -> Result<Option<i64>> {
        let row = if self.input.right { rows.1 } else { rows.0 };
        row.get(&self.input.name)
            .context("compiled MOD input absent")?
            .as_deref()
            .map(|text| {
                let value = text.parse::<i64>()?;
                // PostgreSQL MIN % -1 is zero, without division overflow.
                let result = if self.divisor == -1 {
                    0
                } else {
                    value.checked_rem(self.divisor).context("invalid MOD divisor")?
                };
                Ok(result)
            })
            .transpose()
    }
}
pub(super) fn bind(
    mut input: ColumnRef,
    divisor: i64,
    computed: &mut Vec<Computed>,
) -> Result<ColumnRef> {
    ensure!(
        !input.right,
        "lookup MOD requires a left integral column and nonzero integral literal"
    );
    input.name = relational::field(0, &input.name);
    let typed = super::typed::Typed::remainder(input, divisor)?;
    // Lookups consume an unqualified descriptor but their Map stores scoped fields.
    let mut local = Vec::new();
    let column = super::scalar::register(typed, &mut local, "@lookup_mod_")?;
    let mut entry = local.pop().context("lookup MOD map absent")?;
    entry.column.name = relational::field(0, &column.name);
    if !computed.iter().any(|prior| prior.column == entry.column) {
        computed.push(entry);
    }
    Ok(column)
}
