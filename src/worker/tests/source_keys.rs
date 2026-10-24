use super::{batch, contract, row, settings};
use crate::{compiler::compile, worker::program::relational};
use anyhow::Result;

const SQL: &str = "SELECT b.auction,COUNT(*),COUNT(b.price),SUM(b.price),AVG(b.price) FROM source.bid b GROUP BY b.auction";

#[test]
fn authoritative_linear_plan_omits_input_and_changes_physical_identity() -> Result<()> {
    let native = contract();
    let compiled = compile(SQL, &native)?;
    let strict = relational::plan(&native, &compiled, compiled.relational())?;
    let committed = relational::committed_plan(&native, &compiled)?;
    assert_eq!(strict.definition().arrangements.len(), 3);
    assert_eq!(committed.definition().arrangements.len(), 2);
    assert_ne!(strict.definition().revision, committed.definition().revision);
    for sql in [
        "SELECT b.auction,MIN(b.price) FROM source.bid b GROUP BY b.auction",
        "SELECT a.category,SUM(b.price) FROM source.auction a JOIN source.bid b ON a.id=b.auction GROUP BY a.category",
    ] {
        let compiled = compile(sql, &native)?;
        assert_eq!(
            relational::plan(&native, &compiled, compiled.relational())?.definition().revision,
            relational::committed_plan(&native, &compiled)?.definition().revision
        );
    }
    Ok(())
}

#[tokio::test]
async fn source_statistics_match_strict_execution_across_moves_deletes_and_restart() -> Result<()> {
    let native = contract();
    let compiled = compile(SQL, &native)?;
    let options = settings();
    let mut strict = relational::build(&native, &compiled, &options)?;
    let mut committed = relational::build_committed(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("auction", None), ("price", Some("-2"))]);
    let b = row(&[("id", Some("2")), ("auction", None), ("price", None)]);
    let c = row(&[("id", Some("3")), ("auction", Some("1")), ("price", Some("7"))]);
    let moved = row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("3"))]);
    let histories = [
        vec![("bid", a.clone(), 1), ("bid", b.clone(), 1), ("bid", c.clone(), 1)],
        vec![("bid", a, -1), ("bid", moved.clone(), 1)],
        vec![("bid", b, -1)],
        vec![("bid", c, -1), ("bid", moved, -1)],
        vec![],
    ];
    for updates in histories {
        let delta = batch(updates);
        let input = || relational::inputs(&delta, compiled.relational(), committed.time() + 1);
        let expected = strict.prepare(input()?).await?;
        let actual = committed.prepare(input()?).await?;
        assert_eq!(actual.output().batch, expected.output().batch);
        strict.commit(expected)?;
        committed.commit(actual)?;
        let checkpoint = committed.checkpoint()?;
        let mut restored = relational::build_committed(&native, &compiled, &options)?;
        restored.restore_checkpoint(checkpoint.clone()).await?;
        assert!(strict.restore_checkpoint(checkpoint).await.is_err());
        committed = restored;
    }
    let before = serde_json::to_vec(&committed.checkpoint()?)?;
    let invalid =
        batch(vec![("bid", row(&[("id", Some("4")), ("auction", None), ("price", None)]), 2)]);
    assert!(
        committed
            .prepare(relational::inputs(&invalid, compiled.relational(), committed.time() + 1)?)
            .await
            .is_err()
    );
    assert_eq!(before, serde_json::to_vec(&committed.checkpoint()?)?);
    Ok(())
}

#[test]
fn complete_composite_keys_survive_rebinding_but_partial_keys_do_not() -> Result<()> {
    use crate::compiler::relational::{Binding, Node};
    let mut native = contract();
    native.relations[1].columns[1].primary = true;
    native.relations[1].columns[1].nullable = false;
    for (sql, optimized) in [
        (
            "SELECT x.auction,SUM(x.price) FROM (SELECT b.id,b.auction,b.price FROM source.bid b WHERE b.price>0) x GROUP BY x.auction",
            true,
        ),
        (
            "SELECT x.auction,SUM(x.price) FROM (SELECT b.auction,b.price FROM source.bid b) x GROUP BY x.auction",
            true,
        ),
        (
            "SELECT b.auction,SUM(b.price+1) FROM source.bid b WHERE b.price>0 GROUP BY b.auction",
            true,
        ),
    ] {
        let compiled = compile(sql, &native)?;
        let plan = relational::committed_plan(&native, &compiled)?;
        assert_eq!(
            plan.definition().revision.starts_with("source-key-statistics-v2:"),
            optimized,
            "{sql}"
        );
    }
    // Flattened simple subqueries keep hidden source identity in canonical rows.
    // Exercise a true projection boundary explicitly, including partial PK loss.
    for (keep, optimized) in
        [(vec!["id", "auction", "price"], true), (vec!["auction", "price"], false)]
    {
        let mut compiled = compile(SQL, &native)?;
        let fields = native.relations[1]
            .columns
            .iter()
            .filter(|column| keep.contains(&column.name.as_str()))
            .map(|column| {
                let input = crate::compiler::ColumnRef {
                    name: crate::compiler::relational::field(0, &column.name),
                    right: false,
                    oid: column.oid,
                    nullable: column.nullable,
                };
                Binding { output: input.clone(), input }
            })
            .collect();
        let source = match &compiled.relational.nodes[0] {
            Node::Source { id, .. } => id.clone(),
            _ => anyhow::bail!("missing fixture source"),
        };
        for node in &mut compiled.relational.nodes {
            if let Node::PartitionBy { input, .. } = node {
                *input = "key_boundary".into();
            }
        }
        compiled
            .relational
            .nodes
            .insert(1, Node::Rebind { id: "key_boundary".into(), input: source, fields });
        assert_eq!(
            relational::committed_plan(&native, &compiled)?
                .definition()
                .revision
                .starts_with("source-key-statistics-v2:"),
            optimized
        );
    }

    Ok(())
}

#[tokio::test]
async fn fixed_delta_cold_reads_do_not_grow_with_unrelated_source_rows() -> Result<()> {
    for oid in [20, 1700] {
        let mut native = contract();
        native.relations[1].columns[2].oid = oid;
        let sql = if oid == 1700 { SQL.replace(",AVG(b.price)", "") } else { SQL.into() };
        fixed_delta_reads(&native, &sql).await?;
    }
    Ok(())
}
async fn fixed_delta_reads(native: &crate::source::Contract, sql: &str) -> Result<()> {
    let compiled = compile(sql, native)?;
    let mut options = settings();
    options.block_rows = 64;
    options.limits.cache_bytes = 0;
    options.limits.cache_entries = 0;
    let mut measurements = Vec::new();
    for rows in [64, 1024, 8192] {
        let mut query = relational::build_committed(native, &compiled, &options)?;
        let original = row(&[("id", Some("0")), ("auction", Some("0")), ("price", Some("1"))]);
        let initial = batch(
            (0..rows)
                .map(|id| {
                    (
                        "bid",
                        row(&[
                            ("id", Some(&id.to_string())),
                            ("auction", Some(&(id % 64).to_string())),
                            ("price", Some("1")),
                        ]),
                        1,
                    )
                })
                .collect(),
        );
        query.commit(
            query.prepare(relational::inputs(&initial, compiled.relational(), 1)?).await?,
        )?;
        let checkpoint = query.checkpoint()?;
        query = relational::build_committed(native, &compiled, &options)?;
        query.restore_checkpoint(checkpoint).await?;
        let delta = batch(vec![("bid", original, -1)]);
        let before = query.cache_stats()?.misses;
        let work = query.prepare(relational::inputs(&delta, compiled.relational(), 2)?).await?;
        let reads = query.cache_stats()?.misses - before;
        measurements.push(reads);
        query.commit(work)?;
    }
    assert!(measurements.iter().all(|reads| *reads == measurements[0]), "{measurements:?}");
    assert!(measurements[0] <= 8, "{measurements:?}");
    Ok(())
}

#[tokio::test]
async fn other_consumers_keep_certified_statistics_input_retained() -> Result<()> {
    use crate::compiler::relational::Node;
    let native = contract();
    let mut compiled =
        compile("SELECT b.auction,COUNT(*) FROM source.bid b GROUP BY b.auction", &native)?;
    let (input, spec) = compiled
        .relational
        .nodes
        .iter()
        .find_map(|node| {
            if let Node::Statistics { input, spec, .. } = node {
                Some((input.clone(), spec.clone()))
            } else {
                None
            }
        })
        .ok_or_else(|| anyhow::anyhow!("missing fixture statistics"))?;
    let position = compiled.relational.nodes.len() - 1;
    compiled.relational.nodes.insert(
        position,
        Node::Partition { id: "other_consumer".into(), input: input.clone(), spec },
    );
    let output_parent = match compiled.relational.nodes.last_mut() {
        Some(Node::Output { input, .. }) => std::mem::replace(input, "combined".into()),
        _ => anyhow::bail!("missing fixture output"),
    };
    compiled.relational.nodes.insert(
        position + 1,
        Node::Union { id: "combined".into(), left: output_parent, right: "other_consumer".into() },
    );
    let options = settings();
    let mut query = relational::build_committed(&native, &compiled, &options)?;
    assert!(query.plan().definition().arrangements.iter().any(|member| member.node == input));
    let original = row(&[("id", Some("1")), ("auction", None), ("price", Some("7"))]);
    for updates in [vec![("bid", original.clone(), 1)], vec![("bid", original, -1)]] {
        let delta = batch(updates);
        let prepared = query
            .prepare(relational::inputs(&delta, compiled.relational(), query.time() + 1)?)
            .await?;
        query.commit(prepared)?;
        let checkpoint = query.checkpoint()?;
        let mut restored = relational::build_committed(&native, &compiled, &options)?;
        restored.restore_checkpoint(checkpoint).await?;
        query = restored;
    }
    Ok(())
}
