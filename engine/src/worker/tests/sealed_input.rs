//! Transport chunk boundaries cannot expose provisional SQL results.
use super::{batch, contract, row, settings};
use crate::{engine::execution::Limits, worker::program::relational};
use anyhow::Result;
fn bounded() -> Limits {
    Limits { resident_entries: 1, spill_runs: 2, ..Limits::default() }
}
#[tokio::test]
async fn canceled_source_rows_do_not_run_fallible_scalar_expressions() -> Result<()> {
    let native = contract();
    let compiled =
        crate::compiler::compile("SELECT id,id+2147483647 AS offset_id FROM source.bid", &native)?;
    let mut options = settings();
    options.limits = bounded();
    let mut query = relational::build(&native, &compiled, &options)?;
    let bad = row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("2"))]);
    let good = row(&[("id", Some("-1")), ("auction", Some("1")), ("price", Some("3"))]);
    let mut input = relational::Input::new(compiled.relational(), options.limits)?;
    input.append(&batch(vec![("bid", bad.clone(), 1)]), compiled.relational())?;
    input.append(&batch(vec![("bid", good, 1)]), compiled.relational())?;
    input.append(&batch(vec![("bid", bad, -1)]), compiled.relational())?;
    assert_eq!(query.time(), 0);
    let prepared = query.prepare(input.seal(1)?).await?;
    assert_eq!(prepared.output().time, 1);
    assert_eq!(prepared.output().batch.iter().count(), 1);
    let output = super::projected_batch(&prepared.output().batch, &compiled)?;
    let values = output.iter().next().map(|(((), values), _)| values.clone());
    assert_eq!(
        values,
        Some(vec![
            Some(crate::compiler::Cell::Integer(-1)),
            Some(crate::compiler::Cell::Integer(2_147_483_646))
        ])
    );
    query.commit(prepared)?;
    assert_eq!(query.time(), 1);
    Ok(())
}
#[tokio::test]
async fn snapshot_chunks_and_transaction_chunks_have_identical_join_aggregate_state() -> Result<()>
{
    let native = contract();
    let compiled = crate::compiler::compile(super::SQL, &native)?;
    let mut options = settings();
    options.limits = bounded();
    let mut streamed = relational::build(&native, &compiled, &options)?;
    let mut whole = relational::build(&native, &compiled, &options)?;
    let chunks = vec![
        batch(vec![(
            "auction",
            row(&[("id", Some("1")), ("category", Some("books")), ("other", None)]),
            1,
        )]),
        batch(vec![(
            "bid",
            row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("7"))]),
            1,
        )]),
        batch(vec![("bid", row(&[("id", Some("2")), ("auction", Some("1")), ("price", None)]), 1)]),
    ];
    let mut pending = streamed.bootstrap_workspace(64 * 1024 * 1024)?;
    let mut all = crate::weighted::Batch { updates: Vec::new() };
    for chunk in chunks {
        pending.append(relational::chunk(&chunk, compiled.relational())?).await?;
        all.updates.extend(chunk.updates);
    }
    let candidate = streamed.prepare_bootstrap_unprotected(pending).await?;
    let reference = whole.prepare(relational::inputs(&all, compiled.relational(), 1)?).await?;
    assert_eq!(candidate.output().batch, reference.output().batch);
    streamed.commit(candidate)?;
    whole.commit(reference)?;
    assert_eq!(
        super::grouped_output(&streamed, &compiled).await?,
        super::grouped_output(&whole, &compiled).await?
    );
    assert_eq!(streamed.checkpoint()?.time, 1);
    Ok(())
}

#[tokio::test]
async fn spilled_transaction_fits_memtable_and_commits_once() -> Result<()> {
    let native = contract();
    let compiled = crate::compiler::compile("SELECT id,price FROM source.bid", &native)?;
    let mut options = settings();
    options.limits = bounded();
    let mut query = relational::build(&native, &compiled, &options)?;
    let mut pending = relational::Input::new(compiled.relational(), options.limits)?;
    for id in 1..=50 {
        let id = id.to_string();
        let value = row(&[("id", Some(&id)), ("auction", Some("1")), ("price", Some("7"))]);
        pending.append(&batch(vec![("bid", value, 1)]), compiled.relational())?;
    }
    let prepared = query.prepare_memtable(pending.seal(1)?, 1_000_000).await?;
    assert_eq!(prepared.output().batch.iter().count(), 50);
    assert_eq!(query.time(), 0);
    query.commit(prepared)?;
    assert_eq!(query.time(), 1);
    Ok(())
}
