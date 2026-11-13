use super::{Deltas, Lsn, Progress, Sink};
use crate::engine::Batch;
use anyhow::{Context, Result};
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
    for sink in [
        crate::harness::tuple_sink::sink("bad;sql"),
        crate::harness::tuple_sink::sink("pgderive_queries"),
    ] {
        assert!(sink.validate().is_err());
    }
    crate::harness::tuple_sink::sink("derived").validate()?;
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
            trace: Manifest {
                version: 2,
                time: 1,
                generation: 1,
                objects: vec![],
                cohorts: vec![],
            },
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
    let root = |hash: &str| json!({"path":format!("pgderive/batch/{hash}"),"bytes":1,"hash":hash,"schema":"rows","namespace":null,"blocks":null});
    let first = root(&"a".repeat(64));
    let second = root(&"b".repeat(64));
    let encoded = json!({"version":1,"plan_identity":"plan","time":1,"arrangements":[{
        "id":"rows","schema":"rows","trace":{"version":2,"time":1,"generation":1,"cohorts":[3],
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

#[test]
fn native_jsonb_rows_preserve_weighted_tuple_identity() -> Result<()> {
    let batch = Batch::from_updates([
        ((None::<i64>, ("quoted\"text".to_owned(), Some(0))), 2),
        ((None, ("quoted\"text".to_owned(), None)), -3),
    ])?;
    let sink = crate::harness::tuple_sink::sink("json_rows");
    sink.validate()?;
    let rows = crate::harness::tuple_sink::deltas(&batch)?.rows;
    let decoded = rows
        .iter()
        .map(|(row, weight)| {
            let text = row[1][0].as_str().ok_or_else(|| anyhow::anyhow!("missing jsonb cell"))?;
            Ok((serde_json::from_str::<serde_json::Value>(text)?, *weight))
        })
        .collect::<Result<Vec<_>>>()?;
    let canonical = |rows: Vec<(serde_json::Value, i64)>| {
        rows.into_iter()
            .map(|(row, weight)| (row.to_string(), weight))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    assert_eq!(canonical(decoded), canonical(Deltas::bag(&batch)?.rows));
    Ok(())
}

#[test]
fn keyed_sinks_require_complete_bindings_and_validate_metadata() -> Result<()> {
    let mut sink = crate::harness::tuple_sink::sink("result");
    let encoded = serde_json::to_value(&sink)?;
    assert_eq!(encoded["output"], "postgres");
    assert_eq!(encoded["primary_key"], serde_json::json!([]));
    assert_eq!(serde_json::from_value::<Sink>(encoded.clone())?, sink);
    for field in ["output", "primary_key"] {
        let mut incomplete = encoded.clone();
        incomplete.as_object_mut().context("binding object required")?.remove(field);
        assert!(serde_json::from_value::<Sink>(incomplete).is_err());
    }
    sink.primary_key = vec!["tuple".into()];
    sink.validate()?;
    sink.primary_key.push("tuple".into());
    assert!(sink.validate().is_err());
    sink.primary_key = vec!["absent".into()];
    assert!(sink.validate().is_err());
    sink.primary_key = vec!["tuple".into()];
    sink.output = crate::catalog::SinkOutput::Stdout;
    assert!(sink.validate().is_err());
    Ok(())
}
