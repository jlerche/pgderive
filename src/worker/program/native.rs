//! Shared immutable integer vectors; canonical SQL state remains authoritative.
use crate::{
    engine::value::{Decoded, NativeBudget},
    transaction::Row,
};
use anyhow::{Context, Result};
use num_bigint::BigInt;
use std::sync::Arc;
#[derive(Clone)]
pub(super) struct Integers {
    id: Arc<str>,
    budget: Arc<NativeBudget>,
}
impl Integers {
    pub(super) fn new(domain: &str, width: usize, budget: Arc<NativeBudget>) -> Self {
        Self { id: Arc::from(format!("{domain}:{width}")), budget }
    }
    pub(super) fn decode(
        &self,
        row: &Row,
        decode: impl FnOnce(&Row) -> Result<Vec<BigInt>>,
    ) -> Result<Decoded<Vec<BigInt>>> {
        row.decode(&self.id, &self.budget, |_| decode(row))
    }
    pub(super) fn retain(&self, row: Row, values: &[BigInt]) -> Result<Row> {
        // BigInt::clone uses BigDigits::from_slice in the pinned num-bigint:
        // this discards capacity retained by earlier large arithmetic values.
        let normalized = values.to_vec();
        let bytes = normalized.iter().try_fold(
            normalized
                .capacity()
                .checked_mul(std::mem::size_of::<BigInt>())
                .context("native integer vector size overflow")?,
            |bytes, value| {
                let limbs = usize::try_from(value.bits().div_ceil(64))?
                    .checked_mul(8)
                    .context("native integer size overflow")?;
                bytes.checked_add(limbs).context("native integer vector size overflow")
            },
        )?;
        Ok(Row::from_native(self.id.clone(), normalized, &self.budget, bytes, move |_| {
            row.into_canonical()
        }))
    }
}

#[cfg(test)]
mod tests;
