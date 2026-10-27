use super::{Durable, Resolution, resolve};
use crate::{
    catalog::{Binding, Catalog, Progress, Stored, Writer},
    engine::{
        plan::{Arrangement, Checkpoint, Definition, Kind, Membership, Node, Plan, Source},
        trace::Manifest,
    },
};
use anyhow::Result;

pub(in crate::catalog) fn fixture() -> Result<(Plan, Writer)> {
    let plan = Plan::new(Definition {
        revision: "registered".into(),
        sources: vec![Source { id: "rows".into(), schema: "row".into() }],
        nodes: vec![Node {
            id: "source".into(),
            kind: Kind::Source,
            inputs: vec!["rows".into()],
            schema: "row".into(),
        }],
        arrangements: vec![Arrangement {
            id: "rows".into(),
            node: "source".into(),
            schema: "row".into(),
        }],
        outputs: vec!["source".into()],
    })?;
    let checkpoint = Checkpoint {
        version: 1,
        plan_identity: plan.identity().into(),
        time: 1,
        arrangements: vec![Membership {
            id: "rows".into(),
            schema: "row".into(),
            trace: Manifest {
                version: 2,
                time: 1,
                generation: 1,
                objects: vec![],
                cohorts: vec![],
            },
        }],
    };
    let boundary = Durable {
        stored: Stored { epoch: 1, checkpoint },
        binding: Binding {
            source: crate::source::tests::contract(),
            sink: crate::harness::tuple_sink::sink("sink"),
        },
        fence: 1,
        end: "0/0".parse()?,
        commit: "0/0".parse()?,
        xid: None,
        checkpoint_end: "0/0".parse()?,
        published_time: 1,
    };
    let metadata = crate::catalog::metadata::Metadata::new(&plan, &boundary)?;
    Ok((
        plan,
        Writer { catalog: Catalog::new("fixture", "query")?, boundary, uncertain: true, metadata },
    ))
}
fn boundary(writer: &Writer) -> Durable {
    writer.boundary.clone()
}
#[test]
fn resolution_requires_exact_prior_or_exact_committed_candidate() -> Result<()> {
    let (_, writer) = fixture()?;
    let before = boundary(&writer);
    let mut candidate = writer.boundary.stored.checkpoint.clone();
    candidate.time = 2;
    candidate.arrangements[0].trace.time = 2;
    candidate.arrangements[0].trace.generation = 2;
    let progress = Progress::new(42, "0/10", "0/20")?;
    assert_eq!(resolve(&writer, &before, &candidate, &progress)?, Resolution::NotCommitted);
    let mut after = before.clone();
    after.stored.epoch = 2;
    after.stored.checkpoint = candidate.clone();
    after.commit = progress.commit;
    after.end = progress.end;
    after.checkpoint_end = progress.end;
    after.published_time = candidate.time;
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
    let (_, writer) = fixture()?;
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
