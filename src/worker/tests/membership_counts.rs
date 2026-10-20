//! Persisted presence thresholds avoid hot-left scans; old layouts fail closed.
use super::{batch, contract, row, settings};
use crate::{
    compiler::{Compiled, compile, relational::Node},
    engine::Batch,
    worker::program::relational,
};
use anyhow::Result;
#[tokio::test]
async fn positive_right_count_changes_have_constant_cold_reads_and_zero_left_delta() -> Result<()> {
    let mut native = contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    let compiled = compile(
        "SELECT a.id FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE b.auction=a.other)",
        &native,
    )?;
    let ir = compiled.relational();
    let mut options = settings();
    options.block_rows = 1;
    options.limits.cache_bytes = 0;
    options.limits.cache_entries = 0;
    let right = row(&[("id", Some("1")), ("auction", Some("1")), ("price", None)]);
    let second = row(&[("id", Some("2")), ("auction", Some("1")), ("price", None)]);
    let mut measurements = Vec::new();
    for hot in [8, 128, 1024] {
        let left = (0..hot)
            .map(|id| {
                row(&[("id", Some(&id.to_string())), ("category", None), ("other", Some("1"))])
            })
            .collect::<Vec<_>>();
        let initial = batch(
            left.iter()
                .map(|row| ("auction", row.clone(), 1))
                .chain([("bid", right.clone(), 1), ("bid", second.clone(), 1)])
                .collect(),
        );
        let mut query = relational::build(&native, &compiled, &options)?;
        query.commit(query.prepare(relational::inputs(&initial, ir, 1)?).await?)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
        let delta = batch(vec![("bid", right.clone(), -1)]);
        let before = query.cache_stats()?.misses;
        let prepared = query.prepare(relational::inputs(&delta, ir, 2)?).await?;
        assert_eq!(prepared.output().batch, Batch::from_updates([])?);
        let reads = query.cache_stats()?.misses - before;
        assert!(reads <= 20, "{hot}: {reads}");
        measurements.push(reads);
        query.commit(prepared)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
        let last = batch(vec![("bid", second.clone(), -1)]);
        let prepared = query.prepare(relational::inputs(&last, ir, 3)?).await?;
        assert_eq!(prepared.output().batch.iter().count(), usize::try_from(hot)?);
        assert!(prepared.output().batch.iter().all(|(_, weight)| *weight == -1));
    }
    assert!(measurements.iter().all(|reads| *reads == measurements[0]), "{measurements:?}");
    eprintln!("EXISTS positive-count cold reads at hot-left 8/128/1024: {measurements:?}");
    Ok(())
}
fn legacy(mut compiled: Compiled) -> Compiled {
    let edges = compiled
        .relational
        .nodes
        .iter()
        .filter_map(|node| {
            if let Node::RightCounts { id, input } = node {
                Some((id.clone(), input.clone()))
            } else {
                None
            }
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let presence = compiled
        .relational
        .nodes
        .iter()
        .filter_map(|node| {
            if let Node::RightPresence { id, input } = node {
                edges.get(input).map(|input| (id.clone(), input.clone()))
            } else {
                None
            }
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    compiled.relational.nodes.retain_mut(|node| {
        if let Node::Union { right, .. } = node
            && let Some(input) = presence.get(right)
        {
            *right = input.clone();
        }
        !matches!(node, Node::RightCounts { .. } | Node::RightPresence { .. })
    });
    compiled
}
#[tokio::test]
async fn old_membership_layout_requires_explicit_rebuild_and_counts_exceed_i64() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT a.id FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE b.auction=a.id)",
        &native,
    )?;
    let old = legacy(compiled.clone());
    let options = settings();
    let mut old_query = relational::build_scanned(&native, &old, &options)?;
    let initial = batch(vec![
        ("auction", row(&[("id", Some("1")), ("category", None), ("other", None)]), 1),
        ("bid", row(&[("id", Some("1")), ("auction", Some("1")), ("price", None)]), i64::MAX),
        ("bid", row(&[("id", Some("2")), ("auction", Some("1")), ("price", None)]), i64::MAX),
    ]);
    old_query
        .commit(old_query.prepare(relational::inputs(&initial, old.relational(), 1)?).await?)?;
    let mut query = relational::build(&native, &compiled, &options)?;
    assert_ne!(old_query.plan().identity(), query.plan().identity());
    assert!(query.restore_checkpoint(old_query.checkpoint()?).await.is_err());
    let prepared = query.prepare(relational::inputs(&initial, compiled.relational(), 1)?).await?;
    assert_eq!(prepared.output().batch.iter().count(), 1);
    query.commit(prepared)?;
    super::restart_relational(&mut query, &native, &compiled, &options).await?;
    let delta = batch(vec![(
        "bid",
        row(&[("id", Some("1")), ("auction", Some("1")), ("price", None)]),
        -i64::MAX,
    )]);
    let prepared = query.prepare(relational::inputs(&delta, compiled.relational(), 2)?).await?;
    assert_eq!(prepared.output().batch, Batch::from_updates([])?);
    Ok(())
}
