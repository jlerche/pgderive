//! Retire only this fixture's private store, through the common mark kernel.
use super::Fixture;
use crate::catalog::GcCache;
use anyhow::Result;
impl Fixture {
    pub(super) async fn retire(&self, cache: &mut GcCache) -> Result<serde_json::Value> {
        crate::engine::tests::retire::retire(&self.store, self.trace.manifest()?.objects, cache)
            .await
    }
}
