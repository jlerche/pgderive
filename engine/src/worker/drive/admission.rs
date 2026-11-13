use super::{Context, Duration, Instant, Metrics, Open, Result, emit, maintain};
use crate::{
    engine::plan::relational::{MemtableFull, PooledMemtableFull},
    worker::program::relational,
};
use serde_json::json;

pub(super) struct Prepared {
    pub(super) work:
        crate::engine::plan::relational::Prepared<relational::Key, crate::transaction::Row>,
    pub(super) disruption: Duration,
}

pub(super) async fn prepare(
    open: &mut Open,
    input: relational::Inputs,
    metrics: &mut Metrics,
    profile: &mut crate::timing::Profile,
) -> Result<Prepared> {
    let mut forced = false;
    let mut disruption = Duration::ZERO;
    loop {
        let began = Instant::now();
        let admission = profile.admission();
        let result = open
            .query
            .prepare_memtable_profiled(input.clone(), open.settings.memtable_bytes, profile)
            .await;
        let mut pooled = None;
        let reason = match result {
            Ok(work) => return Ok(Prepared { work, disruption }),
            Err(error) if error.is::<MemtableFull>() => {
                if forced {
                    return Err(error.context(crate::engine::execution::CapacityExceeded(
                        "single logical input exceeds memtable capacity after checkpoint",
                    )));
                }
                forced = true;
                "memtable_bytes"
            }
            Err(error) if error.is::<PooledMemtableFull>() => {
                let had_memory = open.query.memtable_bytes()? > 0 || open.checkpoint.is_some();
                if !had_memory
                    && error
                        .downcast_ref::<PooledMemtableFull>()
                        .is_some_and(|pressure| pressure.required > pressure.limit)
                {
                    return Err(error.context(crate::engine::execution::CapacityExceeded(
                        "single input exceeds tenant/process memtable capacity after checkpoint",
                    )));
                }
                open.replication.capacity.as_ref().context("pooled capacity missing")?.pressure(
                    error
                        .downcast_ref::<PooledMemtableFull>()
                        .context("pooled pressure missing")?
                        .tenant_only,
                );
                pooled = Some(error);
                "shared_memtable"
            }
            Err(error) => return Err(error),
        };
        disruption = disruption.saturating_add(
            began.elapsed().saturating_sub(profile.admission().saturating_sub(admission)),
        );
        let began = Instant::now();
        profile.mark("failed_preparation_tail");
        emit(
            &json!({"event":"checkpoint_backpressure","reason":reason,"time":open.query.time(),"checkpoint_in_flight":open.checkpoint.is_some()}),
        )?;
        let reclaimed = open.query.memtable_bytes()? > 0 || open.checkpoint.is_some();
        if reclaimed {
            maintain(open, metrics).await?;
        }
        if let Some(error) = pooled
            && !reclaimed
        {
            return Err(error);
        }
        disruption = disruption.saturating_add(began.elapsed());
        profile.mark("forced_maintenance");
    }
}
