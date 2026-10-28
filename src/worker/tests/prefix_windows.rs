//! Durable prefix summaries against an independent weighted raw-source oracle.
use super::{batch, contract, row, settings};
use crate::{
    compiler::{compile, relational::Node},
    engine::Batch,
    transaction::Row,
    worker::program::relational,
};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
fn sql() -> String {
    let window = "OVER(PARTITION BY bucket ORDER BY auction RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
    format!(
        "SELECT id,COUNT(*) {window} AS n,COUNT(price) FILTER(WHERE price>0) {window} AS c,SUM(abs(price)) FILTER(WHERE price>0) {window} AS s,AVG(abs(price)) FILTER(WHERE price>0) {window} AS a FROM source.bid"
    )
}
fn value(row: &Row, name: &str) -> Result<i64> {
    row.get(name)
        .and_then(Option::as_deref)
        .context("missing oracle value")?
        .parse()
        .map_err(Into::into)
}
fn oracle(state: &BTreeMap<Row, i64>) -> Result<Batch<relational::Key, Row>> {
    let mut updates = Vec::new();
    for (current, weight) in state.iter().filter(|(_, weight)| **weight > 0) {
        let id = value(current, "id")?;
        let position = value(current, "auction")?;
        let mut rows = 0_i64;
        let mut count = 0_i64;
        let mut sum = 0_i128;
        for (other, multiplicity) in state.iter().filter(|(_, weight)| **weight > 0) {
            if other["bucket"] != current["bucket"] || value(other, "auction")? > position {
                continue;
            }
            rows += multiplicity;
            if let Some(price) = &other["price"] {
                let price = price.parse::<i64>()?;
                if price > 0 {
                    count += multiplicity;
                    sum += i128::from(price) * i128::from(*multiplicity);
                }
            }
        }
        let sum = (count > 0).then(|| sum.to_string());
        let average = sum.as_ref().map(|sum| format!("{sum}/{count}"));
        updates.push((
            (
                crate::worker::program::relational::Key::new(),
                row(&[
                    ("0:id", Some(&id.to_string())),
                    ("@aggregate_0", Some(&rows.to_string())),
                    ("@aggregate_1", Some(&count.to_string())),
                    ("@aggregate_2", sum.as_deref()),
                    ("@aggregate_3", average.as_deref()),
                ]),
            ),
            *weight,
        ));
    }
    Batch::from_updates(updates)
}
#[tokio::test]
async fn prefix_signed_peer_histories_match_independent_scan_and_restore() -> Result<()> {
    for bytes in [0, 1, 4 * 1024 * 1024] {
        prefix_history(bytes).await?;
    }
    Ok(())
}
async fn prefix_history(native_cache_bytes: usize) -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[1].oid = 20;
    native.relations[1].columns[1].nullable = false;
    native.relations[1].columns.push(crate::source::tests::column(4, "bucket", 23));
    let compiled = super::raw_statistics_compiled(compile(&sql(), &native)?);
    assert!(
        compiled.relational().nodes.iter().any(|node| matches!(node, Node::PrefixWindow { .. }))
    );
    let mut options = settings();
    options.limits.native_cache_bytes = native_cache_bytes;
    let scan = relational::build_scanned(&native, &compiled, &options)?;
    let mut a =
        row(&[("id", Some("1")), ("auction", Some("-9223372036854775808")), ("price", Some("2"))]);
    let mut b = row(&[("id", Some("3")), ("auction", Some("0")), ("price", None)]);
    let mut c =
        row(&[("id", Some("5")), ("auction", Some("0")), ("price", Some("-9223372036854775808"))]);
    let mut d =
        row(&[("id", Some("2")), ("auction", Some("9223372036854775807")), ("price", Some("7"))]);
    let mut moved =
        row(&[("id", Some("3")), ("auction", Some("9223372036854775807")), ("price", Some("9"))]);
    for value in [&mut a, &mut b, &mut c, &mut moved] {
        value.insert("bucket".into(), None);
    }
    d.insert("bucket".into(), Some("2".into()));
    let histories = [
        vec![(a.clone(), 2), (b.clone(), 1), (c.clone(), 3), (d.clone(), 1)],
        vec![(b.clone(), -1), (moved.clone(), 1), (a.clone(), -1)],
        vec![],
        vec![(a, -1), (c, -3), (d, -1), (moved, -1)],
    ];
    let query = super::History {
        scanned: Some(scan),
        compact: true,
        ..super::History::new((&native, &compiled, &options))
    }
    .run(
        histories.into_iter().map(|changes| {
            batch(changes.into_iter().map(|(row, weight)| ("bid", row, weight)).collect())
        }),
        oracle,
        super::apply_rows,
        |_| true,
    )
    .await?;

    let before = query.checkpoint()?;
    let invalid = batch(vec![(
        "bid",
        row(&[("id", Some("999")), ("bucket", None), ("auction", Some("0")), ("price", Some("2"))]),
        -1,
    )]);
    assert!(query.prepare(relational::inputs(&invalid, compiled.relational(), 5)?).await.is_err());
    assert_eq!(query.checkpoint()?, before);
    Ok(())
}

#[tokio::test]
async fn prefix_late_update_reduces_cold_tick_reads_with_explicit_write_cost() -> Result<()> {
    let native = contract();
    let window = "OVER(ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
    let compiled = super::raw_statistics_compiled(compile(
        &format!(
            "SELECT id,COUNT(*) {window},SUM(price) {window},AVG(price) {window} FROM source.bid"
        ),
        &native,
    )?);
    let baseline = relational::scan_prefix(&compiled);
    let source = (0..4096)
        .map(|id| {
            row(&[
                ("id", Some(&id.to_string())),
                ("auction", None),
                ("price", Some(&(id % 19).to_string())),
            ])
        })
        .collect::<Vec<_>>();
    let initial = batch(source.iter().map(|row| ("bid", row.clone(), 1)).collect());
    let changed = row(&[("id", Some("4095")), ("auction", None), ("price", Some("77"))]);
    let delta = batch(vec![("bid", source[4095].clone(), -1), ("bid", changed, 1)]);
    let mut measurements = Vec::new();
    let mut expected = None;
    for (variant, definition) in [&baseline, &compiled].into_iter().enumerate() {
        let options = super::cold_settings(8);
        let query = super::cold_query(definition, &initial, |_| {
            relational::build(&native, definition, &options)
        })
        .await?;
        let checkpoint = query.checkpoint()?;
        let probe = super::Probe::start(&query, &options).await?;
        let update = query.prepare(relational::inputs(&delta, definition.relational(), 2)?).await?;
        if let Some(expected) = &expected {
            assert_eq!(&update.output().batch, expected);
        } else {
            expected = Some(update.output().batch.clone());
        }
        measurements.push(probe.finish(&query, &options).await?);
        let mut bounded = options.clone();
        bounded.limits.contributions = 1000;
        let mut limited = relational::build(&native, definition, &bounded)?;
        limited.restore_checkpoint(checkpoint).await?;
        let attempt =
            limited.prepare(relational::inputs(&delta, definition.relational(), 2)?).await;
        if variant == 1 {
            assert!(attempt.is_ok());
        } else {
            assert!(attempt.is_err());
        }
        assert_eq!(limited.time(), 1);
    }
    assert!(measurements[1].0 < measurements[0].0, "scan/index cold tick reads: {measurements:?}");
    eprintln!(
        "prefix complete cold tick scan/index (reads,new objects,new bytes): {measurements:?}"
    );
    Ok(())
}

#[tokio::test]
async fn prefix_sql_aggregate_overflow_aborts_without_advancing_state() -> Result<()> {
    for count in [false, true] {
        overflow(count).await?;
    }
    Ok(())
}
async fn overflow(count: bool) -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 23;
    let function = if count { "COUNT(*)" } else { "SUM(price)" };
    let sql = format!(
        "SELECT id,{function} OVER(ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM source.bid"
    );
    let compiled = compile(&sql, &native)?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("auction", None), ("price", Some("2147483647"))]);
    let b = row(&[("id", Some("2")), ("auction", None), ("price", Some("2147483647"))]);
    let initial = batch(vec![("bid", a.clone(), if count { i64::MAX } else { 1 })]);
    query.commit(query.prepare(relational::inputs(&initial, compiled.relational(), 1)?).await?)?;
    let before = query.checkpoint()?;
    let invalid = batch(vec![("bid", b.clone(), if count { 1 } else { i64::MAX })]);
    let result = query.prepare(relational::inputs(&invalid, compiled.relational(), 2)?).await;
    let error = match result {
        Ok(_) => anyhow::bail!("overflow preparation unexpectedly succeeded"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("overflow"), "{error}");
    assert_eq!(query.checkpoint()?, before);
    let retry =
        batch(if count { vec![("bid", a, -1), ("bid", b, 1)] } else { vec![("bid", b, 1)] });
    query.commit(query.prepare(relational::inputs(&retry, compiled.relational(), 2)?).await?)?;
    super::restart_relational(&mut query, &native, &compiled, &options).await?;
    assert_eq!(query.time(), 2);
    Ok(())
}

#[test]
fn prefix_eligibility_preserves_existing_exact_window_families() -> Result<()> {
    let native = contract();
    for (sql, expected) in [
        ("SELECT COUNT(*) OVER(ORDER BY id) FROM source.bid", true),
        ("SELECT COUNT(*) OVER(ORDER BY id DESC) FROM source.bid", false),
        ("SELECT COUNT(*) OVER(ORDER BY auction) FROM source.bid", false),
        (
            "SELECT COUNT(*) OVER(ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM source.bid",
            false,
        ),
        (
            "SELECT COUNT(*) OVER(ORDER BY id GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM source.bid",
            false,
        ),
        (
            "SELECT COUNT(*) OVER(ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM source.bid",
            false,
        ),
        ("SELECT MIN(price) OVER(ORDER BY id) FROM source.bid", false),
        ("SELECT SUM(price::numeric) OVER(ORDER BY id) FROM source.bid", false),
        ("SELECT COUNT(*) OVER(ORDER BY id,price) FROM source.bid", false),
    ] {
        let compiled = compile(sql, &native)?;
        assert_eq!(
            compiled
                .relational()
                .nodes
                .iter()
                .any(|node| matches!(node, Node::PrefixWindow { .. })),
            expected,
            "{sql}"
        );
    }
    Ok(())
}
#[tokio::test]
async fn prefix_composes_with_downstream_positional_window_and_old_layout_fails_closed()
-> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT d.id,SUM(d.n) OVER(ORDER BY d.id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM (SELECT id,COUNT(*) OVER(ORDER BY id) AS n FROM source.bid) d",
        &native,
    )?;
    let options = settings();
    let mut direct = relational::build(&native, &compiled, &options)?;
    let mut scan = relational::build_scanned(&native, &compiled, &options)?;
    let initial = batch(
        (1..=3)
            .map(|id| {
                (
                    "bid",
                    row(&[("id", Some(&id.to_string())), ("auction", None), ("price", None)]),
                    1,
                )
            })
            .collect(),
    );
    let prepared = direct.prepare(relational::inputs(&initial, compiled.relational(), 1)?).await?;
    let expected = scan.prepare(relational::inputs(&initial, compiled.relational(), 1)?).await?;
    assert_eq!(prepared.output().batch, expected.output().batch);
    direct.commit(prepared)?;
    scan.commit(expected)?;
    let mut fresh = relational::build(&native, &compiled, &options)?;
    assert!(fresh.restore_checkpoint(scan.checkpoint()?).await.is_err());
    super::restart_relational(&mut direct, &native, &compiled, &options).await?;
    Ok(())
}
