use super::{Durable, Resolution, resolve};
use crate::{
    catalog::{Binding, Catalog, Progress, Stored, Writer},
    engine::plan::Checkpoint,
};
use anyhow::Result;

fn writer() -> Result<Writer> {
    let checkpoint = Checkpoint {
        version: 1,
        plan_identity: "registered".into(),
        time: 1,
        arrangements: vec![],
    };
    Ok(Writer {
        catalog: Catalog::new("fixture", "query")?,
        boundary: Durable {
            stored: Stored { epoch: 1, checkpoint },
            binding: Binding {
                source: crate::source::tests::contract(),
                sink: crate::harness::tuple_sink::sink("sink"),
            },
            fence: 1,
            end: "0/0".parse()?,
            commit: "0/0".parse()?,
            xid: None,
        },
        uncertain: true,
    })
}
fn boundary(writer: &Writer) -> Durable {
    writer.boundary.clone()
}
#[test]
fn resolution_requires_exact_prior_or_exact_committed_candidate() -> Result<()> {
    let writer = writer()?;
    let before = boundary(&writer);
    let mut candidate = writer.boundary.stored.checkpoint.clone();
    candidate.time = 2;
    let progress = Progress::new(42, "0/10", "0/20")?;
    assert_eq!(resolve(&writer, &before, &candidate, &progress)?, Resolution::NotCommitted);
    let mut after = before.clone();
    after.stored.epoch = 2;
    after.stored.checkpoint = candidate.clone();
    after.commit = progress.commit;
    after.end = progress.end;
    after.xid = Some(42);
    assert_eq!(resolve(&writer, &after, &candidate, &progress)?, Resolution::Committed);
    let mut invalid = after.clone();
    invalid.xid = Some(41);
    assert!(resolve(&writer, &invalid, &candidate, &progress).is_err());
    let mut invalid = after.clone();
    invalid.stored.epoch = 3;
    assert!(resolve(&writer, &invalid, &candidate, &progress).is_err());
    let mut invalid = before;
    invalid.stored.checkpoint.plan_identity = "changed".into();
    assert!(resolve(&writer, &invalid, &candidate, &progress).is_err());
    assert!(resolve(&writer, &after, &writer.boundary.stored.checkpoint, &progress).is_err());
    assert!(resolve(&writer, &after, &candidate, &Progress::new(42, "0/0", "0/0")?).is_err());
    Ok(())
}
#[test]
fn replay_uses_wal_order_and_checks_identity_at_exact_boundary() -> Result<()> {
    let writer = writer()?;
    let mut durable = boundary(&writer);
    durable.commit = "0/10".parse()?;
    durable.end = "0/20".parse()?;
    durable.xid = Some(42);
    assert!(durable.covers(&Progress::new(u32::MAX, "0/1", "0/2")?)?);
    assert!(durable.covers(&Progress::new(42, "0/10", "0/20")?)?);
    assert!(!durable.covers(&Progress::new(1, "0/30", "0/40")?)?);
    assert!(durable.covers(&Progress::new(1, "0/10", "0/20")?).is_err());
    assert!(durable.covers(&Progress::new(43, "0/19", "0/21")?).is_err());
    let invalid = Progress { xid: 1, commit: "0/50".parse()?, end: "0/40".parse()? };
    assert!(durable.covers(&invalid).is_err());
    Ok(())
}
#[test]
fn native_progress_requires_snapshot_then_committed_transactions() -> Result<()> {
    let start = "0/10".parse()?;
    let end = "0/20".parse()?;
    assert!(Binding::valid_identity(1, None, start, start));
    assert!(!Binding::valid_identity(0, None, start, start));
    assert!(!Binding::valid_identity(1, Some(42), start, end));
    assert!(!Binding::valid_identity(1, None, start, end));
    assert!(!Binding::valid_identity(2, None, start, start));
    assert!(Binding::valid_identity(2, Some(42), start, end));
    Ok(())
}
