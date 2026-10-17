use super::{Deltas, Lsn, Progress, Sink};
use crate::engine::Batch;
use anyhow::Result;
use serde_json::json;

#[test]
fn source_positions_are_distinct_ordered_wal_addresses() -> Result<()> {
    assert!("bad".parse::<Lsn>().is_err());
    for value in ["/0", "0/", "100000000/0", "0/100000000", "0/G", "0/0/0"] {
        assert!(value.parse::<Lsn>().is_err());
    }
    let value: Lsn = "abcdef/12345678".parse()?;
    assert_eq!(value.to_string(), "ABCDEF/12345678");
    assert!("1/0".parse::<Lsn>()? > "0/FFFFFFFF".parse()?);
    assert!(Progress::new(4, "1/10", "1/F").is_err());
    assert_eq!(Progress::new(u32::MAX, "1/10", "1/11")?.end.to_string(), "1/11");
    Ok(())
}
#[test]
fn sink_encodings_preserve_full_tuple_and_group_null_semantics() -> Result<()> {
    let bag = Batch::from_updates([((1, String::from("a")), 2), ((1, String::from("b")), -3)])?;
    let rows = Deltas::bag(&bag)?.rows;
    assert_eq!(rows, vec![(json!([1, "a"]), 2), (json!([1, "b"]), -3)]);
    let grouped = Batch::from_updates([((None::<i64>, (1, None)), -1), ((None, (2, Some(0))), 1)])?;
    assert_eq!(
        Deltas::bag(&grouped)?.rows,
        vec![(json!([null, [1, null]]), -1), (json!([null, [2, 0]]), 1)]
    );
    for sink in [Sink::Json("bad;sql".into()), Sink::Json("pgderive_queries".into())] {
        assert!(sink.validate().is_err());
    }
    Sink::Json("derived".into()).validate()?;
    Ok(())
}

#[test]
fn maintenance_keeps_singletons_and_requires_one_generation_for_replacements() -> Result<()> {
    use crate::engine::{
        plan::{Arrangement, Checkpoint, Definition, Kind, Membership, Node, Plan, Source},
        trace::Manifest,
    };
    let plan = Plan::new(Definition {
        revision: "selective-maintenance".into(),
        sources: vec![Source { id: "rows".into(), schema: "row".into() }],
        nodes: vec![
            Node {
                id: "source".into(),
                kind: Kind::Source,
                inputs: vec!["rows".into()],
                schema: "row".into(),
            },
            Node {
                id: "output".into(),
                kind: Kind::Project,
                inputs: vec!["source".into()],
                schema: "row".into(),
            },
        ],
        arrangements: vec![Arrangement {
            id: "output".into(),
            node: "output".into(),
            schema: "row".into(),
        }],
        outputs: vec!["output".into()],
    })?;
    let prior = Checkpoint {
        version: 1,
        plan_identity: plan.identity().into(),
        time: 1,
        arrangements: vec![Membership {
            id: "output".into(),
            schema: "row".into(),
            trace: Manifest { version: 1, time: 1, generation: 1, objects: vec![] },
        }],
    };
    super::maintenance::validate(&prior, &prior, &plan)?;
    let mut next = prior.clone();
    next.arrangements[0].trace.generation += 1;
    super::maintenance::validate(&prior, &next, &plan)?;
    next.arrangements[0].trace.generation += 1;
    assert!(super::maintenance::validate(&prior, &next, &plan).is_err());
    Ok(())
}

#[test]
fn sinks_reject_removed_expression_programs() {
    assert!(
        serde_json::from_value::<Sink>(json!({"MappedJson":{"table":"rows","map":{}}})).is_err()
    );
    assert!(
        serde_json::from_value::<Sink>(json!({"Rows":{"table":"rows","columns":[],"map":null}}))
            .is_err()
    );
}

#[test]
fn checkpoint_checksum_retains_run_order_multiplicity_and_numeric_limits() -> Result<()> {
    use crate::engine::plan::Checkpoint;
    let root = |hash: &str| json!({"path":format!("pgderive/batch/{hash}"),"bytes":1,"hash":hash,"schema":"rows"});
    let first = root(&"a".repeat(64));
    let second = root(&"b".repeat(64));
    let encoded = json!({"version":1,"plan_identity":"plan","time":1,"arrangements":[{
        "id":"rows","schema":"rows","trace":{"version":1,"time":1,"generation":1,
        "objects":[first.clone(),second,first]}}]});
    let checkpoint: Checkpoint = serde_json::from_value(encoded.clone())?;
    let digest = super::checkpoint_digest(&checkpoint)?;
    let round_trip: Checkpoint = serde_json::from_str(&serde_json::to_string_pretty(&encoded)?)?;
    assert_eq!(super::checkpoint_digest(&round_trip)?, digest);
    let mut changed = checkpoint.clone();
    changed.arrangements[0].trace.objects.pop();
    assert_ne!(super::checkpoint_digest(&changed)?, digest);
    let mut changed = checkpoint.clone();
    changed.arrangements[0].trace.objects.swap(0, 1);
    assert_ne!(super::checkpoint_digest(&changed)?, digest);
    let mut changed = checkpoint;
    changed.arrangements[0].trace.generation = u64::MAX;
    assert!(super::checkpoint_digest(&changed).is_err());
    Ok(())
}
