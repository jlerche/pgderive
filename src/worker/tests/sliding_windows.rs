//! Complete cold ticks retain the same layout, reads and writes while frame work falls.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use futures_util::TryStreamExt;
use std::collections::BTreeMap;
fn sql() -> String {
    let window = "OVER(ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
    format!("SELECT id,COUNT(*) {window},SUM(price) {window},AVG(price) {window} FROM source.bid")
}
fn expected(source: &BTreeMap<Row, i64>) -> Result<Batch<relational::Key, Row>> {
    let mut rows = source
        .iter()
        .filter(|(_, weight)| **weight > 0)
        .map(|(row, _)| {
            Ok((row["id"].as_deref().context("missing oracle ID")?.parse::<i32>()?, row))
        })
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by_key(|(id, _)| *id);
    let mut sum = 0_i64;
    let mut present = 0;
    let mut output = Vec::new();
    for (index, (_, value)) in rows.into_iter().enumerate() {
        if let Some(price) = &value["price"] {
            sum += price.parse::<i64>()?;
            present += 1;
        }
        let sum = (present > 0).then(|| sum.to_string());
        let average = sum.as_ref().map(|sum| format!("{sum}/{present}"));
        output.push((
            (
                Vec::new(),
                row(&[
                    ("0:id", value["id"].as_deref()),
                    ("@aggregate_0", Some(&(index + 1).to_string())),
                    ("@aggregate_1", sum.as_deref()),
                    ("@aggregate_2", average.as_deref()),
                ]),
            ),
            1,
        ));
    }
    Batch::from_updates(output)
}
#[tokio::test]
async fn sliding_cold_ticks_match_source_oracle_and_scan_object_costs() -> Result<()> {
    let native = contract();
    let compiled = compile(&sql(), &native)?;
    let source = (0..400)
        .map(|id| {
            row(&[
                ("id", Some(&id.to_string())),
                ("auction", None),
                ("price", if id % 7 == 0 { None } else { Some("2") }),
            ])
        })
        .collect::<Vec<_>>();
    let initial = batch(source.iter().map(|row| ("bid", row.clone(), 1)).collect());
    let replacement = row(&[("id", Some("200")), ("auction", None), ("price", Some("7"))]);
    let delta = batch(vec![("bid", source[200].clone(), -1), ("bid", replacement, 1)]);
    let mut state = BTreeMap::new();
    super::apply_rows(&mut state, &initial);
    let before = expected(&state)?;
    super::apply_rows(&mut state, &delta);
    let expected = super::bag_delta(&before, &expected(&state)?)?;
    let mut measurements = Vec::new();
    for optimized in [false, true] {
        let mut options = settings();
        options.block_rows = 8;
        options.limits.cache_bytes = 0;
        options.limits.cache_entries = 0;
        let mut query = relational::build_scanned(&native, &compiled, &options)?;
        query.commit(
            query.prepare(relational::inputs(&initial, compiled.relational(), 1)?).await?,
        )?;
        let checkpoint = query.checkpoint()?;
        query = if optimized {
            relational::build(&native, &compiled, &options)?
        } else {
            relational::build_scanned(&native, &compiled, &options)?
        };
        query.restore_checkpoint(checkpoint).await?;
        let objects = options.store.list(None).try_collect::<Vec<_>>().await?;
        let prior = query.cache_stats()?.misses;
        let prepared = query.prepare(relational::inputs(&delta, compiled.relational(), 2)?).await?;
        assert_eq!(prepared.output().batch, expected);
        let after = options.store.list(None).try_collect::<Vec<_>>().await?;
        let writes = after
            .iter()
            .filter(|object| !objects.iter().any(|old| old.location == object.location))
            .collect::<Vec<_>>();
        measurements.push((
            query.cache_stats()?.misses - prior,
            writes.len(),
            writes.iter().map(|object| object.size).sum::<u64>(),
        ));
        query.commit(prepared)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
    }
    assert_eq!(measurements[0], measurements[1]);
    eprintln!("sliding complete cold tick scan/sliding (blocks,objects,bytes): {measurements:?}");
    Ok(())
}

#[tokio::test]
async fn sliding_accepts_bounded_work_while_scan_failure_preserves_checkpoint() -> Result<()> {
    let native = contract();
    let compiled = compile(&sql(), &native)?;
    let options = settings();
    let source = (0..100)
        .map(|id| {
            (
                "bid",
                row(&[("id", Some(&id.to_string())), ("auction", None), ("price", Some("2"))]),
                1,
            )
        })
        .collect();
    let initial = batch(source);
    let mut query = relational::build_scanned(&native, &compiled, &options)?;
    query.commit(query.prepare(relational::inputs(&initial, compiled.relational(), 1)?).await?)?;
    let checkpoint = query.checkpoint()?;
    let mut bounded = options;
    bounded.limits.contributions = 2000;
    let delta = batch(vec![(
        "bid",
        row(&[("id", Some("100")), ("auction", None), ("price", Some("3"))]),
        1,
    )]);
    let mut scan = relational::build_scanned(&native, &compiled, &bounded)?;
    scan.restore_checkpoint(checkpoint.clone()).await?;
    let prior = scan.checkpoint()?;
    assert!(scan.prepare(relational::inputs(&delta, compiled.relational(), 2)?).await.is_err());
    assert_eq!(scan.checkpoint()?, prior);
    let mut optimized = relational::build(&native, &compiled, &bounded)?;
    optimized.restore_checkpoint(checkpoint).await?;
    optimized
        .commit(optimized.prepare(relational::inputs(&delta, compiled.relational(), 2)?).await?)?;
    Ok(())
}
