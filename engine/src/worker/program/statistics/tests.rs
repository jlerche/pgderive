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
        previous_windows: None,
        aggregates: vec![Aggregate {
            expression: None,
            function,
            argument: Some(ColumnRef {
                name: "price".into(),
                right: false,
                oid,
                nullable: true,
                collation: 0,
                modifier: None,
            }),
            filter: None,
            field: "result".into(),
            navigation: None,
            position: None,
            string_agg: None,
        }],
    }
}
fn evaluate(spec: &Partition, values: &[&str]) -> Result<Option<String>> {
    let row: Row = values
        .iter()
        .enumerate()
        .map(|(index, value)| (field(index), Some((*value).into())))
        .collect();
    let output =
        finalize(spec, crate::engine::value::NativeBudget::new(4096)).evaluate(&TimedBatch {
            time: 1,
            batch: Batch::from_updates([((vec![].into(), row), 1)])?,
        })?;
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
        let input = TimedBatch {
            time: 1,
            batch: Batch::from_updates([((vec![].into(), row.clone()), weight)])?,
        };
        assert!(
            finalize(&spec, crate::engine::value::NativeBudget::new(4096))
                .evaluate(&input)
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn integer_accumulator_sizes_match_serde_and_capacity_errors() -> Result<()> {
    use super::scalar::Scalar;
    use crate::engine::{
        dataflow::Additive,
        execution::{CapacityExceeded, paired_record_size},
    };
    use num_bigint::BigInt;

    let mut magnitudes = vec![0, 1, u64::from(u32::MAX), 1 << 32, (1 << 32) + 1, u64::MAX];
    for power in 1..=9 {
        let boundary = 10_u64.pow(power);
        magnitudes.extend([boundary - 1, boundary, (boundary << 32) + boundary]);
    }
    let values = magnitudes
        .into_iter()
        .flat_map(|magnitude| [BigInt::from(magnitude), -BigInt::from(magnitude)])
        .chain([BigInt::from(1) << 64, -(BigInt::from(1) << 96_u32)]);
    for value in values {
        let scalar = Scalar::Integer(value);
        let bytes = serde_json::to_vec(&scalar)?.len();
        let limit = 23 + 3 + bytes;
        assert_eq!(scalar.record_size(23, limit)?, limit);
        for tight in [0, 25, limit - 1] {
            let expected =
                paired_record_size(23, &scalar, tight).err().context("reference limit")?;
            let actual = scalar.record_size(23, tight).err().context("optimized limit")?;
            assert!(actual.is::<CapacityExceeded>());
            assert_eq!(actual.to_string(), expected.to_string());
        }
        assert!(scalar.record_size(usize::MAX, usize::MAX).is_err());
    }
    Ok(())
}
