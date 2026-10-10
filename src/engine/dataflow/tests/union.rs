use crate::engine::{
    Batch,
    dataflow::{TimedBatch, Union},
    execution::Limits,
};
use anyhow::Result;
#[test]
fn union_preserves_time_and_complete_signed_identity_through_spill() -> Result<()> {
    let left = Batch::from_updates([((1_i64, 2_i64), i64::MAX), ((1, 3), 2)])?;
    let right = Batch::from_updates([((1, 2), -i64::MAX), ((1, 3), -1), ((2, 2), 3)])?;
    let input = TimedBatch { time: 7, batch: (left, right) };
    let output = Union.evaluate(&input, Limits { resident_entries: 1, ..Limits::default() })?;
    assert_eq!(output.time, 7);
    assert_eq!(output.batch, Batch::from_updates([((1, 3), 1), ((2, 2), 3)])?);
    Ok(())
}
#[test]
fn union_rejects_final_overflow_and_excess_complete_tick_work() -> Result<()> {
    let left = Batch::from_updates([((1_i64, 2_i64), i64::MAX)])?;
    let right = Batch::from_updates([((1, 2), 1)])?;
    let input = TimedBatch { time: 1, batch: (left, right) };
    assert!(Union.evaluate(&input, Limits::default()).is_err());
    let input = TimedBatch {
        time: 1,
        batch: (Batch::from_updates([((1_i64, 2_i64), 1)])?, Batch::from_updates([((1, 2), -1)])?),
    };
    assert!(Union.evaluate(&input, Limits { contributions: 1, ..Limits::default() }).is_err());
    Ok(())
}
