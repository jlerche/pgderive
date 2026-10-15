use super::{batch, contract, row, settings};
use crate::{
    compiler::{Compiled, compile, partition::Mode, relational::Node},
    engine::Batch,
    worker::program::relational,
};
use anyhow::{Context, Result};

#[tokio::test]
async fn distinct_occurrence_counts_exceed_i64_without_narrowing_membership() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT b.auction AS g,COUNT(DISTINCT b.price) AS n FROM source.bid b GROUP BY b.auction",
        &native,
    )?;
    let ir = compiled.relational();
    let options = settings();
    let a = row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("7"))]);
    let b = row(&[("id", Some("2")), ("auction", Some("1")), ("price", Some("7"))]);
    let expected = row(&[("0:auction", Some("1")), ("@aggregate_0", Some("1"))]);
    let mut query = relational::build(&native, &compiled, &options)?;
    let histories = [
        (vec![("bid", a.clone(), i64::MAX), ("bid", b.clone(), i64::MAX)], 1),
        (vec![("bid", a, -i64::MAX)], 0),
        (vec![("bid", b, -i64::MAX)], -1),
    ];
    for (updates, weight) in histories {
        let input = batch(updates);
        let prepared = query.prepare(relational::inputs(&input, ir, query.time() + 1)?).await?;
        assert_eq!(
            prepared.output().batch,
            Batch::from_updates([((Vec::new(), expected.clone()), weight)])?
        );
        query.commit(prepared)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
    }
    Ok(())
}
fn scan_baseline(compiled: &Compiled) -> Result<Compiled> {
    let mut baseline = compiled.clone();
    let ir = &mut baseline.relational;
    let spec = ir
        .nodes
        .iter()
        .find_map(|node| match node {
            Node::DistinctCounts { spec, .. } => Some(spec.clone()),
            _ => None,
        })
        .context("missing distinct count spec")?;
    let Mode::Grouped { keys } = &spec.mode else {
        anyhow::bail!("missing grouped keys");
    };
    ir.nodes = vec![
        Node::Source { id: "source".into(), source: 0 },
        Node::PartitionBy {
            id: "partition_input".into(),
            input: "source".into(),
            keys: keys.clone(),
        },
        Node::Partition { id: "partition".into(), input: "partition_input".into(), spec },
        Node::Output { id: "project".into(), input: "partition".into() },
    ];
    Ok(baseline)
}
#[tokio::test]
async fn distinct_delta_uses_fewer_cold_blocks_than_affected_group_scan() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT b.auction AS g,COUNT(*) AS n,COUNT(DISTINCT b.price) AS d FROM source.bid b GROUP BY b.auction",
        &native,
    )?;
    let baseline = scan_baseline(&compiled)?;
    let mut options = settings();
    options.block_rows = 8;
    options.limits.cache_entries = 0;
    options.limits.cache_bytes = 0;
    let source = (0..1000)
        .map(|id| {
            row(&[
                ("id", Some(&id.to_string())),
                ("auction", Some("1")),
                ("price", Some(&(id % 10).to_string())),
            ])
        })
        .collect::<Vec<_>>();
    let initial = batch(source.iter().map(|row| ("bid", row.clone(), 1)).collect());
    let changed = row(&[("id", Some("0")), ("auction", Some("1")), ("price", Some("1"))]);
    let delta = batch(vec![("bid", source[0].clone(), -1), ("bid", changed, 1)]);
    let mut measurements = Vec::new();
    for definition in [&baseline, &compiled] {
        let ir = definition.relational();
        let mut query = relational::build(&native, definition, &options)?;
        query.commit(query.prepare(relational::inputs(&initial, ir, 1)?).await?)?;
        let checkpoint = query.checkpoint()?;
        query = relational::build(&native, definition, &options)?;
        query.restore_checkpoint(checkpoint).await?;
        let before = query.cache_stats()?.misses;
        let update = query.prepare(relational::inputs(&delta, ir, 2)?).await?;
        assert_eq!(update.output().batch, Batch::from_updates([])?);
        let reads = query.cache_stats()?.misses - before;
        measurements.push(reads);
        assert_eq!(query.cache_stats()?.bytes, 0);
    }
    assert!(
        measurements[1] < measurements[0],
        "cold block misses baseline/optimized: {measurements:?}"
    );
    eprintln!("distinct cold block misses baseline/optimized: {measurements:?}");
    Ok(())
}

#[derive(Default)]
struct MixedTotal {
    rows: i64,
    present: i64,
    sum: i128,
    values: std::collections::BTreeSet<i64>,
}
fn mixed_oracle(
    state: &std::collections::BTreeMap<crate::transaction::Row, i64>,
) -> Result<Batch<relational::Key, crate::transaction::Row>> {
    let mut groups = std::collections::BTreeMap::<Option<String>, MixedTotal>::new();
    for (source, weight) in state.iter().filter(|(_, weight)| **weight > 0) {
        let total = groups.entry(source["auction"].clone()).or_default();
        total.rows += weight;
        if let Some(price) = &source["price"] {
            let price = price.parse::<i64>()?;
            total.present += weight;
            total.sum += i128::from(price) * i128::from(*weight);
            total.values.insert(price);
        }
    }
    Batch::from_updates(groups.into_iter().map(|(key, total)| {
        let output = row(&[
            ("0:auction", key.as_deref()),
            ("@aggregate_0", Some(&total.rows.to_string())),
            ("@aggregate_1", (total.present != 0).then(|| total.sum.to_string()).as_deref()),
            (
                "@aggregate_2",
                (total.present != 0).then(|| format!("{}/{}", total.sum, total.present)).as_deref(),
            ),
            ("@aggregate_3", Some(&total.values.len().to_string())),
            (
                "@aggregate_4",
                Some(&total.values.iter().filter(|value| **value < 0).count().to_string()),
            ),
            ("@aggregate_5", Some(if key.is_some() { "1" } else { "0" })),
        ]);
        ((Vec::new(), output), 1)
    }))
}
#[tokio::test]
async fn mixed_distinct_sum_average_filters_and_group_moves_restore() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT b.auction AS g,COUNT(*) AS n,SUM(b.price) AS s,AVG(b.price) AS a,COUNT(DISTINCT b.price) AS d,COUNT(DISTINCT b.price) FILTER(WHERE b.price<0) AS negative,COUNT(DISTINCT b.auction) AS keys FROM source.bid b GROUP BY b.auction",
        &native,
    )?;
    let ir = compiled.relational();
    let options = settings();
    let a = row(&[("id", Some("1")), ("auction", None), ("price", Some("2"))]);
    let b = row(&[("id", Some("2")), ("auction", None), ("price", None)]);
    let c = row(&[("id", Some("3")), ("auction", Some("9")), ("price", Some("-7"))]);
    let e = row(&[("id", Some("4")), ("auction", None), ("price", Some("-7"))]);
    let moved = row(&[("id", Some("4")), ("auction", Some("9")), ("price", Some("7"))]);
    let histories = [
        vec![
            ("bid", a.clone(), 3),
            ("bid", b.clone(), 2),
            ("bid", c.clone(), 1),
            ("bid", e.clone(), 1),
        ],
        vec![("bid", a.clone(), -2), ("bid", e.clone(), 1)],
        vec![("bid", a, -1), ("bid", b.clone(), -2)],
        vec![("bid", e, -2), ("bid", moved.clone(), 2)],
        vec![("bid", c, -1), ("bid", moved, -2)],
        vec![("bid", b, 1)],
        vec![],
    ];
    let mut state = std::collections::BTreeMap::new();
    let mut query = relational::build(&native, &compiled, &options)?;
    for updates in histories {
        let before = mixed_oracle(&state)?;
        let input = batch(updates);
        for update in &input.updates {
            *state.entry(update.tuple.row.clone()).or_default() += update.weight;
        }
        let after = mixed_oracle(&state)?;
        let expected = super::bag_delta(&before, &after)?;
        let prepared = query.prepare(relational::inputs(&input, ir, query.time() + 1)?).await?;
        assert_eq!(prepared.output().batch, expected);
        query.commit(prepared)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
    }
    let before = query.checkpoint()?;
    let absent = row(&[("id", Some("absent")), ("auction", None), ("price", Some("-7"))]);
    let added = row(&[("id", Some("added")), ("auction", None), ("price", Some("-7"))]);
    let invalid = batch(vec![("bid", absent, -1), ("bid", added, 1)]);
    assert!(query.prepare(relational::inputs(&invalid, ir, query.time() + 1)?).await.is_err());
    assert_eq!(query.checkpoint()?, before);
    Ok(())
}
