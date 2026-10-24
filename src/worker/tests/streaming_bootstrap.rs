//! Snapshot transport boundaries never become logical transaction boundaries.
use super::{batch, contract, program::relational, row, settings};
use crate::{source::Contract, weighted};
use anyhow::{Context, Result};

async fn compare(
    sql: &str,
    native: &Contract,
    changes: &weighted::Batch,
    chunk_size: usize,
) -> Result<()> {
    let compiled = crate::compiler::compile(sql, native).with_context(|| sql.to_owned())?;
    let options = settings();
    let mut streamed = relational::build(native, &compiled, &options)?;
    let whole = relational::build(native, &compiled, &options)?;
    let mut workspace = streamed.bootstrap_workspace(64 * 1024 * 1024)?;
    for updates in changes.updates.chunks(chunk_size) {
        workspace
            .append(relational::chunk(
                &weighted::Batch { updates: updates.to_vec() },
                compiled.relational(),
            )?)
            .await?;
        assert_eq!(streamed.time(), 0);
    }
    let work = streamed.prepare_bootstrap_unprotected(workspace).await?;
    let expected = whole.prepare(relational::inputs(changes, compiled.relational(), 1)?).await?;
    assert_eq!(work.output().batch, expected.output().batch);
    streamed.commit(work)?;
    super::restart_relational(&mut streamed, native, &compiled, &options).await?;
    assert_eq!(streamed.time(), 1);
    Ok(())
}
#[tokio::test]
async fn snapshot_union_self_join_and_nonlinear_nodes_match_complete_input() -> Result<()> {
    let native = contract();
    let changes = batch(vec![
        ("bid", row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("3"))]), 1),
        ("bid", row(&[("id", Some("2")), ("auction", Some("1")), ("price", None)]), 1),
        ("auction", row(&[("id", Some("1")), ("category", Some("books")), ("other", None)]), 1),
    ]);
    for sql in [
        super::SQL,
        "SELECT a.id,b.id FROM source.bid a JOIN source.bid b ON a.auction=b.auction",
        "SELECT DISTINCT auction FROM source.bid",
        "SELECT a.id,b.price FROM source.auction a LEFT JOIN source.bid b ON a.id=b.auction",
    ] {
        for chunk in [1, 3] {
            compare(sql, &native, &changes, chunk).await?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn snapshot_does_not_run_final_aggregate_expression_on_partial_state() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 23;
    let changes = batch(vec![
        ("bid", row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("0"))]), 1),
        ("bid", row(&[("id", Some("2")), ("auction", Some("1")), ("price", Some("2"))]), 1),
    ]);
    compare(
        "SELECT g.auction,abs(g.total-9223372036854775807-1) AS value FROM (SELECT auction,SUM(price) AS total FROM source.bid GROUP BY auction) g",
        &native,
        &changes,
        1,
    )
    .await
}
#[tokio::test]
async fn quoted_colon_columns_and_pruned_cdc_fields_share_snapshot_identity() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].name = "price:total".into();
    let before = row(&[("id", Some("1")), ("auction", Some("1")), ("price:total", Some("7"))]);
    compare(
        "SELECT id,\"price:total\" FROM source.bid",
        &native,
        &batch(vec![("bid", before.clone(), 1)]),
        1,
    )
    .await?;
    let compiled = crate::compiler::compile(
        "SELECT auction,COUNT(*) FROM source.bid GROUP BY auction",
        &native,
    )?;
    assert!(!compiled.relational().projection[0].contains("price:total"));
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let mut workspace = query.bootstrap_workspace(64 * 1024 * 1024)?;
    workspace
        .append(relational::chunk(&batch(vec![("bid", before.clone(), 1)]), compiled.relational())?)
        .await?;
    let work = query.prepare_bootstrap_unprotected(workspace).await?;
    query.commit(work)?;
    let after = row(&[("id", Some("1")), ("auction", Some("1")), ("price:total", Some("9"))]);
    let changes = batch(vec![("bid", before, -1), ("bid", after, 1)]);
    let work = query
        .prepare_memtable(relational::inputs(&changes, compiled.relational(), 2)?, 64 * 1024 * 1024)
        .await?;
    assert_eq!(work.output().batch.iter().count(), 0);
    Ok(())
}
#[tokio::test]
async fn failed_snapshot_is_poisoned_and_never_advances_visibility() -> Result<()> {
    let native = contract();
    let compiled =
        crate::compiler::compile("SELECT id,id+2147483647 AS value FROM source.bid", &native)?;
    let query = relational::build(&native, &compiled, &settings())?;
    let mut workspace = query.bootstrap_workspace(64 * 1024 * 1024)?;
    let changes =
        batch(vec![("bid", row(&[("id", Some("1")), ("auction", None), ("price", None)]), 1)]);
    assert!(workspace.append(relational::chunk(&changes, compiled.relational())?).await.is_err());
    assert!(
        workspace.append(relational::chunk(&batch(vec![]), compiled.relational())?).await.is_err()
    );
    assert!(query.prepare_bootstrap_unprotected(workspace).await.is_err());
    assert_eq!(query.time(), 0);
    Ok(())
}
#[tokio::test]
async fn snapshot_numeric_specials_scale_nulls_and_empty_groups_match_complete_input() -> Result<()>
{
    let mut native = contract();
    native.relations[1].columns[2].oid = 1700;
    let values = [
        ("1", Some("1.00")),
        ("1", Some("2.000")),
        ("1", None),
        ("2", Some("Infinity")),
        ("3", Some("-Infinity")),
        ("4", Some("NaN")),
        ("5", None),
        ("6", Some("Infinity")),
        ("6", Some("-Infinity")),
    ];
    let updates = values
        .iter()
        .enumerate()
        .map(|(index, (group, value))| {
            let id = (index + 1).to_string();
            ("bid", row(&[("id", Some(&id)), ("auction", Some(group)), ("price", *value)]), 1)
        })
        .collect();
    compare(
        "SELECT auction,COUNT(price),SUM(price) FROM source.bid GROUP BY auction",
        &native,
        &batch(updates),
        1,
    )
    .await?;
    compare(
        "SELECT auction,COUNT(price),SUM(price) FROM source.bid GROUP BY auction",
        &native,
        &batch(vec![]),
        1,
    )
    .await
}
