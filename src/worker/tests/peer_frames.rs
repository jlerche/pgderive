use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
type Bag = Batch<relational::Key, Row>;
fn sql(case: usize) -> String {
    let order = if case == 4 { "" } else { " ORDER BY b.price DESC NULLS FIRST" };
    let frame = match case {
        1 => " GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
        2 => " RANGE BETWEEN CURRENT ROW AND CURRENT ROW",
        3 => " GROUPS BETWEEN 2 FOLLOWING AND 1 FOLLOWING",
        _ => "",
    };
    let window = format!("OVER(PARTITION BY b.auction{order}{frame})");
    format!("SELECT b.id,COUNT(*) {window} AS n,SUM(b.price) {window} AS s FROM source.bid b")
}
fn oracle(state: &BTreeMap<Row, i64>, case: usize) -> Result<Bag> {
    let mut groups: BTreeMap<Option<String>, Vec<Row>> = BTreeMap::new();
    for (row, weight) in state {
        for _ in 0..*weight {
            groups.entry(row["auction"].clone()).or_default().push(row.clone());
        }
    }
    let price = |row: &Row| row["price"].as_ref().and_then(|value| value.parse::<i64>().ok());
    let mut updates = Vec::new();
    for rows in groups.into_values() {
        let mut keys = rows.iter().map(price).collect::<Vec<_>>();
        keys.sort_by_key(|key| (key.is_some(), key.map(std::cmp::Reverse)));
        keys.dedup();
        for row in &rows {
            let current = keys.iter().position(|key| *key == price(row)).context("missing peer")?;
            let mut values = Vec::new();
            let mut count = 0;
            for other in &rows {
                let group =
                    keys.iter().position(|key| *key == price(other)).context("missing peer")?;
                let selected = match case {
                    1 => group.abs_diff(current) <= 1,
                    2 => group == current,
                    3 => false,
                    4 => true,
                    _ => group <= current,
                };
                if selected {
                    count += 1;
                    values.extend(price(other));
                }
            }
            let output = [
                ("0:id".into(), row["id"].clone()),
                ("@aggregate_0".into(), Some(count.to_string())),
                (
                    "@aggregate_1".into(),
                    (!values.is_empty()).then(|| values.iter().sum::<i64>().to_string()),
                ),
            ]
            .into();
            updates.push(((crate::worker::program::relational::Key::new(), output), 1));
        }
    }
    Batch::from_updates(updates)
}
async fn history(case: usize) -> Result<()> {
    let native = contract();
    let compiled = compile(&sql(case), &native)?;
    let options = settings();
    super::check_row_history(
        (&native, &compiled, &options),
        super::peer_changes(),
        |state| oracle(state, case),
        |index| index == 1,
    )
    .await?;
    Ok(())
}
#[tokio::test]
async fn weighted_peer_frames_match_independent_histories() -> Result<()> {
    for case in 0..5 {
        history(case).await?;
    }
    Ok(())
}
#[test]
fn rejects_unimplemented_distance_and_exclusions() -> Result<()> {
    for sql in [
        sql(1).replace("GROUPS", "RANGE"),
        sql(1).replace(" ORDER BY b.price DESC NULLS FIRST", ""),
        sql(2).replace("CURRENT ROW)", "CURRENT ROW EXCLUDE TIES)"),
    ] {
        assert!(compile(&sql, &contract()).is_err(), "{sql}");
    }
    let default = compile(&sql(0), &contract())?;
    let explicit = compile(
        &sql(0).replace("NULLS FIRST)", "NULLS FIRST RANGE UNBOUNDED PRECEDING)"),
        &contract(),
    )?;
    assert_eq!(serde_json::to_vec(&default)?, serde_json::to_vec(&explicit)?);
    assert_ne!(serde_json::to_vec(&default)?, serde_json::to_vec(&compile(&sql(2), &contract())?)?);
    Ok(())
}

#[tokio::test]
async fn whole_partition_aggregates_share_work_between_peers() -> Result<()> {
    let native = contract();
    let compiled = compile(&sql(4), &native)?;
    let ir = compiled.relational();
    let mut options = settings();
    options.limits.contributions = 1024;
    let mut query = relational::build(&native, &compiled, &options)?;
    let mut state = BTreeMap::new();
    let mut changes = Vec::new();
    for id in 0..64 {
        let input = row(&[
            ("id", Some(&id.to_string())),
            ("auction", Some("1")),
            ("price", Some(&id.to_string())),
        ]);
        state.insert(input.clone(), 1);
        changes.push(("bid", input, 1));
    }
    let changes = batch(changes);
    let prepared = query.prepare(relational::inputs(&changes, ir, 1)?).await?;
    assert_eq!(prepared.output().batch, oracle(&state, 4)?);
    query.commit(prepared)?;
    Ok(())
}
