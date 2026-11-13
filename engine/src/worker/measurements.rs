//! Opt-in phase attribution; preparation includes operator CPU and object I/O.
use crate::timing::Clock;
use anyhow::Result;
use serde_json::Value;
use std::collections::BTreeMap;
pub(super) struct Phases {
    clock: Clock,
    values: Option<BTreeMap<&'static str, u128>>,
}
impl Phases {
    pub(super) fn new(enabled: bool) -> Self {
        Self { clock: Clock::new(enabled), values: enabled.then(BTreeMap::new) }
    }
    pub(super) fn mark(&mut self, name: &'static str) {
        if let Some(values) = &mut self.values {
            values.insert(name, self.clock.lap());
        }
    }
    pub(super) fn finish(mut self, decode_us: u128) -> Result<Option<Value>> {
        self.values
            .as_mut()
            .map(|values| {
                values.insert("source_decode", decode_us);
                serde_json::to_value(values)
            })
            .transpose()
            .map_err(Into::into)
    }
}
