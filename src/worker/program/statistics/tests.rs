use super::{field, finalize};
use crate::{
    compiler::{
        ColumnRef,
        partition::{Aggregate, Function, Mode, Partition},
    },
    engine::{Batch, dataflow::TimedBatch},
    transaction::Row,
};
use anyhow::{Context, Result};

fn spec(function: Function, oid: u32) -> Partition {
    Partition {
        mode: Mode::Grouped { keys: vec![] },
        aggregates: vec![Aggregate {
            function,
            argument: Some(ColumnRef { name: "price".into(), right: false, oid, nullable: true }),
            filter: None,
            field: "result".into(),
            navigation: None,
        }],
    }
}
fn evaluate(spec: &Partition, values: &[&str]) -> Result<Option<String>> {
    let row: Row = values
        .iter()
        .enumerate()
        .map(|(index, value)| (field(index), Some((*value).into())))
        .collect();
    let output = finalize(spec)
        .evaluate(&TimedBatch { time: 1, batch: Batch::from_updates([((vec![], row), 1)])? })?;
    output
        .batch
        .iter()
        .next()
        .context("finalized output absent")?
        .0
        .1
        .get("result")
        .cloned()
        .context("result field absent")
}

#[test]
fn finalizer_preserves_null_bigint_sum_ratio_and_width_error_domains() -> Result<()> {
    for function in [Function::Sum, Function::Average] {
        assert_eq!(evaluate(&spec(function, 23), &["1", "0", "0"])?, None);
    }
    let wide = "9223372036854775808";
    assert_eq!(evaluate(&spec(Function::Sum, 20), &["2", "2", wide])?, Some(wide.into()));
    assert_eq!(
        evaluate(&spec(Function::Average, 20), &["2", "2", wide])?,
        Some(format!("{wide}/2"))
    );
    for function in [Function::Sum, Function::Average] {
        assert!(evaluate(&spec(function, 23), &["2", "2", wide]).is_err());
    }
    assert!(evaluate(&spec(Function::Count, 20), &[wide, wide, "0"]).is_err());
    assert!(evaluate(&spec(Function::Average, 20), &[wide, wide, "0"]).is_err());
    Ok(())
}

#[test]
fn finalizer_validates_other_slots_and_does_not_skip_errors_on_retractions() -> Result<()> {
    let mut spec = spec(Function::Count, 20);
    let mut extra = spec.aggregates[0].clone();
    extra.field = "other".into();
    spec.aggregates.push(extra);
    // The requested result slot is valid, but another slot has sum without contributions.
    assert!(evaluate(&spec, &["1", "1", "0", "0", "1"]).is_err());
    let row: Row = ["1", "1", "0", "2", "0"]
        .iter()
        .enumerate()
        .map(|(index, value)| (field(index), Some((*value).into())))
        .collect();
    for weight in [-1, 1] {
        let input =
            TimedBatch { time: 1, batch: Batch::from_updates([((vec![], row.clone()), weight)])? };
        assert!(finalize(&spec).evaluate(&input).is_err());
    }
    Ok(())
}
