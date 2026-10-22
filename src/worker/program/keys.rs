//! SQL grouping keys retain native nullable fields with their canonical encoding.
use super::relational::Key;
use crate::engine::value::{Decoded, NativeBudget};
use anyhow::{Context, Result};
use std::sync::Arc;
const GROUP_FIELDS: &str = "sql-group-fields-v1";
pub(super) struct GroupCodec {
    id: Arc<str>,
    budget: Arc<NativeBudget>,
}
impl GroupCodec {
    pub(super) fn new(budget: Arc<NativeBudget>) -> Self {
        Self { id: Arc::from(GROUP_FIELDS), budget }
    }
    pub(super) fn group(&self, values: Vec<Option<String>>) -> Result<Key> {
        let canonical = values.iter().map(serde_json::to_string).collect::<Result<Vec<_>, _>>()?;
        let bytes = values.iter().flatten().try_fold(
            values
                .capacity()
                .checked_mul(std::mem::size_of::<Option<String>>())
                .context("native key size overflow")?,
            |bytes, value| bytes.checked_add(value.capacity()).context("native key size overflow"),
        )?;
        Ok(Key::from_native(self.id.clone(), values, &self.budget, bytes, move |_| canonical))
    }
}
pub(super) fn values(
    key: &Key,
    budget: &Arc<NativeBudget>,
) -> Result<Decoded<Vec<Option<String>>>> {
    key.decode(GROUP_FIELDS, budget, |encoded| {
        encoded
            .iter()
            .map(|field| serde_json::from_str(field))
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    })
}
