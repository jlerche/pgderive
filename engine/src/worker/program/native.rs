//! Shared immutable statistic vectors; canonical SQL state remains authoritative.
use crate::{
    engine::value::{Decoded, NativeBudget},
    transaction::Row,
};
use anyhow::{Context, Result};
use num_bigint::BigInt;
use std::sync::Arc;
#[derive(Clone)]
pub(super) struct Vector {
    id: Arc<str>,
    budget: Arc<NativeBudget>,
}
impl Vector {
    pub(super) fn new(domain: &str, width: usize, budget: Arc<NativeBudget>) -> Self {
        Self { id: Arc::from(format!("{domain}:{width}")), budget }
    }
    pub(super) fn decode<T: Send + Sync + 'static>(
        &self,
        row: &Row,
        decode: impl FnOnce(&Row) -> Result<Vec<T>>,
    ) -> Result<Decoded<Vec<T>>> {
        row.decode(&self.id, &self.budget, |_| decode(row))
    }
    pub(super) fn retain<T: NativeValue>(&self, row: Row, values: &[T]) -> Result<Row> {
        // BigInt::clone uses BigDigits::from_slice in the pinned num-bigint:
        // this discards capacity retained by earlier large arithmetic values.
        let normalized = values.to_vec();
        let bytes = normalized.iter().try_fold(
            normalized
                .capacity()
                .checked_mul(std::mem::size_of::<T>())
                .context("native integer vector size overflow")?,
            |bytes, value| {
                bytes.checked_add(value.heap_bytes()?).context("native vector size overflow")
            },
        )?;
        Ok(Row::from_native(self.id.clone(), normalized, &self.budget, bytes, move |_| {
            row.into_canonical()
        }))
    }
}

pub(super) trait NativeValue: Clone + Send + Sync + 'static {
    fn heap_bytes(&self) -> Result<usize>;
}
impl NativeValue for BigInt {
    fn heap_bytes(&self) -> Result<usize> {
        usize::try_from(self.bits().div_ceil(64))?
            .checked_mul(8)
            .context("native integer size overflow")
    }
}

#[cfg(test)]
mod tests;
