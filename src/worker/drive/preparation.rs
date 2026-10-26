//! Memtable backpressure is outside the foreground preparation deadline.
use super::{Metrics, Open, emit, maintain, relational};
use crate::{engine::plan::relational::Prepared, timing::Profile, transaction::Row};
use anyhow::Result;
use serde_json::json;
type Work = Prepared<relational::Key, Row>;
pub(super) async fn run(
    open: &mut Open,
    input: relational::Inputs,
    metrics: &mut Metrics,
    profile: &mut Profile,
) -> Result<Work> {
    match bounded(open, input.clone(), profile).await {
        Ok(work) => Ok(work),
        Err(error) if error.is::<crate::engine::plan::relational::MemtableFull>() => {
            profile.mark("failed_preparation_tail");
            emit(&json!({"event":"checkpoint_backpressure","reason":"memtable_bytes",
                "time":open.query.time(),"checkpoint_in_flight":open.checkpoint.is_some()}))?;
            maintain(open, metrics).await?;
            profile.mark("forced_maintenance");
            bounded(open, input, profile).await.map_err(|error| {
                if error.is::<crate::engine::plan::relational::MemtableFull>() {
                    error.context(crate::engine::execution::CapacityExceeded(
                        "single logical input exceeds memtable capacity after checkpoint",
                    ))
                } else {
                    error
                }
            })
        }
        Err(error) => Err(error),
    }
}
async fn bounded(open: &Open, input: relational::Inputs, profile: &mut Profile) -> Result<Work> {
    super::super::operation::run(
        open.settings.health.operation_timeout_secs,
        open.query.prepare_memtable_profiled(input, open.settings.memtable_bytes, profile),
    )
    .await
}
