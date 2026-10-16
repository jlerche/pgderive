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
