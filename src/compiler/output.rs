//! Engine-owned output encoding and additive aggregate finalization.
use anyhow::{Result, ensure};
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum OutputMap {
    Identity,
    Average,
}
#[derive(Clone, Serialize)]
pub struct Output {
    maps: Vec<OutputMap>,
}
impl Output {
    pub(crate) fn new(maps: Vec<OutputMap>, types: &[u32]) -> Result<Self> {
        ensure!(maps.len() == types.len(), "engine output layout mismatch");
        Ok(Self { maps })
    }
    pub(crate) fn native_rendering(&self) -> bool {
        !self.maps.contains(&OutputMap::Average)
    }
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(!self.maps.is_empty(), "empty engine output layout");
        Ok(())
    }
    pub(crate) fn value(&self, index: usize, value: &str) -> Result<String> {
        match self.maps.get(index) {
            Some(OutputMap::Average) => crate::numeric::Numeric::average(value),
            Some(OutputMap::Identity) => Ok(value.into()),
            _ => anyhow::bail!("output function is not available in-engine"),
        }
    }
}
