//! Exact persisted right-key occurrence counts and zero/positive membership.
use super::relational::Key;
use crate::{
    engine::dataflow::{Project, Statistics},
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
const SIDE: &str = "\0left_join_side";
// Sort before the side tag, so presence can be found without scanning left rows.
const PRESENCE: &str = "\0exists_membership";
const COUNT: &str = "\0exists_occurrence_count";
fn decode(row: &Row) -> Result<BigInt> {
    ensure!(row.len() == 1, "invalid right-count state fields");
    let count: BigInt = row
        .get(COUNT)
        .and_then(Option::as_deref)
        .context("missing right occurrence count")?
        .parse()?;
    ensure!(count > BigInt::from(0), "nonpositive persisted right count");
    Ok(count)
}
pub(super) fn counts() -> Statistics<Key, Row> {
    Statistics::new(
        1,
        |row: &Row| {
            ensure!(
                row.get(SIDE).and_then(Option::as_deref) == Some("right"),
                "right count received non-right row"
            );
            Ok(vec![BigInt::from(1)])
        },
        |row| Ok(vec![decode(row)?]),
        |_, values| {
            let count = values.first().context("missing right count statistic")?;
            ensure!(count >= &BigInt::from(0), "negative right occurrence count");
            Ok((count != &BigInt::from(0))
                .then(|| Row::from([(COUNT.into(), Some(count.to_string()))])))
        },
    )
}
fn member() -> Row {
    [(PRESENCE.into(), Some("1".into())), (SIDE.into(), Some("right".into()))].into()
}
pub(super) fn validate_presence(row: &Row) -> Result<()> {
    ensure!(row == &member(), "invalid right presence state");
    Ok(())
}
pub(super) fn presence() -> Project<Key, Row, Key, Row> {
    Project::new(|key: &Key, row: &Row| {
        decode(row)?;
        Ok(Some((key.clone(), member())))
    })
}
