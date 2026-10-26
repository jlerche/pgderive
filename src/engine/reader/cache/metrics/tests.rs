use super::*;
use crate::engine::reader::BlockCache;
use std::sync::Arc;

#[test]
fn exact_merge_probes_share_scan_and_output_events() -> Result<()> {
    let cache = Arc::new(BlockCache::new(0, 0));
    assert!(cache.take_merge_rows().is_err());
    cache.record_merge(3)?;
    cache.enable_merge_probes()?;
    BlockCache::scan(cache.clone()).record_merge(17)?;
    BlockCache::output(cache.clone()).record_merge(19)?;
    cache.enable_merge_probes()?;
    assert_eq!(cache.take_merge_rows()?, vec![17, 19]);
    assert!(cache.take_merge_rows()?.is_empty());
    assert_eq!(cache.stats()?.merges.iter().sum::<u64>(), 3);
    Ok(())
}

#[test]
fn exact_merge_probe_overflow_is_bounded_and_sticky() -> Result<()> {
    let metrics = Metrics::default();
    metrics.enable_merge_probes()?;
    for rows in 0..4096 {
        metrics.merged(rows)?;
    }
    assert!(metrics.merged(4096).is_err());
    assert!(metrics.take_merge_rows().is_err());
    metrics.enable_merge_probes()?;
    assert!(metrics.merged(1).is_err());
    assert!(metrics.take_merge_rows().is_err());
    Ok(())
}
