use super::{batch, contract, row, settings};
use crate::{
    compiler::{Compiled, compile, partition::Mode, relational::Node},
    engine::Batch,
    worker::program::relational,
};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
fn baseline(compiled: &Compiled) -> Result<Compiled> {
    let mut result = compiled.clone();
    let ir = &mut result.relational;
    let spec = ir
        .nodes
        .iter()
        .find_map(|node| {
            if let Node::OrderedFirst { spec, .. } = node { Some(spec.clone()) } else { None }
        })
        .context("missing ordered spec")?;
    let Mode::Grouped { keys } = &spec.mode else {
        anyhow::bail!("missing grouping");
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
    Ok(result)
}
#[tokio::test]
async fn ordered_extrema_reduce_cold_reads_with_measured_write_amplification() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT b.auction AS g,MIN(b.price) AS lo,MAX(b.price) AS hi FROM source.bid b GROUP BY b.auction",
        &native,
    )?;
    let scan = baseline(&compiled)?;
    let source = (0..1000)
        .map(|id| {
            row(&[
                ("id", Some(&id.to_string())),
                ("auction", Some("1")),
                ("price", Some(&id.to_string())),
            ])
        })
        .collect::<Vec<_>>();
    let initial = batch(source.iter().map(|row| ("bid", row.clone(), 1)).collect());
    let changed = row(&[("id", Some("500")), ("auction", Some("1")), ("price", Some("501"))]);
    let delta = batch(vec![("bid", source[500].clone(), -1), ("bid", changed, 1)]);
    let mut measured = Vec::new();
    for definition in [&scan, &compiled] {
        let options = super::cold_settings(8);
        let ir = definition.relational();
        let query = super::cold_query(definition, &initial, |_| {
            relational::build(&native, definition, &options)
        })
        .await?;
        let probe = super::Probe::start(&query, &options).await?;
        let work = query.prepare(relational::inputs(&delta, ir, 2)?).await?;
        assert_eq!(work.output().batch, Batch::from_updates([])?);
        measured.push(probe.finish(&query, &options).await?);
    }
    assert!(
        measured[1].0 < measured[0].0,
        "ordered baseline/optimized (coldblocks,objects,bytes): {measured:?}"
    );
    eprintln!("ordered baseline/optimized (coldblocks,objects,bytes): {measured:?}");
    Ok(())
}
fn oracle(
    source: &BTreeMap<crate::transaction::Row, i64>,
) -> Result<Batch<relational::Key, crate::transaction::Row>> {
    let mut grouped = BTreeMap::<Option<String>, Vec<i64>>::new();
    for (row, weight) in source.iter().filter(|(_, weight)| **weight > 0) {
        let values = grouped.entry(row["auction"].clone()).or_default();
        if let Some(value) = &row["price"] {
            let value = value.parse::<i64>()?;
            if *weight > 0 {
                values.push(value);
            }
        }
    }
    Batch::from_updates(grouped.into_iter().map(|(group, values)| {
        let lo = values.iter().min().map(ToString::to_string);
        let hi = values.iter().max().map(ToString::to_string);
        let positive = values.iter().filter(|value| **value > 0).min().map(ToString::to_string);
        (
            (
                crate::worker::program::relational::Key::new(),
                row(&[
                    ("0:auction", group.as_deref()),
                    ("@aggregate_0", lo.as_deref()),
                    ("@aggregate_1", hi.as_deref()),
                    ("@aggregate_2", positive.as_deref()),
                ]),
            ),
            1,
        )
    }))
}
#[tokio::test]
async fn ordered_extrema_history_full_identity_null_filter_and_cold_restart() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT b.auction AS g,MIN(b.price) AS lo,MAX(b.price) AS hi,MIN(b.price) FILTER(WHERE b.price>0) AS p FROM source.bid b GROUP BY b.auction",
        &native,
    )?;
    let ir = compiled.relational();
    let options = settings();
    let a = row(&[("id", Some("1")), ("auction", None), ("price", Some("-9"))]);
    let b = row(&[("id", Some("2")), ("auction", None), ("price", Some("7"))]);
    let c = row(&[("id", Some("3")), ("auction", Some("1")), ("price", None)]);
    let moved = row(&[("id", Some("2")), ("auction", Some("1")), ("price", Some("7"))]);
    let histories = [
        vec![(a.clone(), i64::MAX), (b.clone(), i64::MAX), (c.clone(), 1)],
        vec![(a.clone(), -i64::MAX)],
        vec![(b.clone(), -i64::MAX), (moved.clone(), 1)],
        vec![(moved, -1)],
        vec![(c.clone(), -1)],
        vec![(a.clone(), 1)],
        vec![],
        vec![(a, -1)],
    ];
    let mut query =
        super::History { snapshot: true, ..super::History::new((&native, &compiled, &options)) }
            .run(
                histories.into_iter().map(|updates| {
                    batch(updates.into_iter().map(|(row, weight)| ("bid", row, weight)).collect())
                }),
                oracle,
                super::apply_rows,
                |_| true,
            )
            .await?;

    let live = row(&[("id", Some("10")), ("auction", None), ("price", Some("-999"))]);
    let live = batch(vec![("bid", live, 1)]);
    query.commit(query.prepare(relational::inputs(&live, ir, query.time() + 1)?).await?)?;
    let absent = row(&[("id", Some("999")), ("auction", None), ("price", Some("999"))]);
    let substitute = row(&[("id", Some("998")), ("auction", None), ("price", Some("999"))]);
    let before = query.checkpoint()?;
    let invalid = batch(vec![("bid", absent, -1), ("bid", substitute, 1)]);
    assert!(query.prepare(relational::inputs(&invalid, ir, query.time() + 1)?).await.is_err());
    assert_eq!(query.checkpoint()?, before);
    Ok(())
}

#[tokio::test]
async fn ordered_layout_rejects_previous_partition_checkpoint() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT b.auction AS g,MIN(b.price) AS lo FROM source.bid b GROUP BY b.auction",
        &native,
    )?;
    let old = baseline(&compiled)?;
    let options = settings();
    let previous = relational::build(&native, &old, &options)?;
    let mut current = relational::build(&native, &compiled, &options)?;
    assert_ne!(previous.plan().identity(), current.plan().identity());

    assert!(current.restore_checkpoint(previous.checkpoint()?).await.is_err());
    Ok(())
}
