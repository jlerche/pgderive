//! Scoped Cartesian products preserve bag coefficients, NULLs and synchronized deltas.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, worker::program::relational};
use anyhow::{Context, Result};

#[tokio::test]
async fn native_scopes_cross_null_tuples_and_restore() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT l.auction,r.other FROM(SELECT auction FROM source.bid) l CROSS JOIN(SELECT other FROM source.auction) r",
        &native,
    )?;
    let ir = compiled.relational().context("missing cross join graph")?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let bid = row(&[("id", Some("1")), ("auction", None), ("price", None)]);
    let auction = row(&[("id", Some("1")), ("category", None), ("other", None)]);
    let mut integrated = 0;
    for (changes, expected) in [
        (batch(vec![("bid", bid.clone(), 2), ("auction", auction.clone(), 3)]), 6),
        (batch(vec![("bid", bid.clone(), -1), ("auction", auction.clone(), -1)]), 2),
        (batch(vec![("auction", auction.clone(), -2)]), 0),
        (batch(vec![("auction", auction, 1)]), 1),
        (batch(vec![("bid", bid, -1)]), 0),
    ] {
        let prepared = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        for ((_, value), weight) in prepared.output().batch.iter() {
            assert_eq!(ir.output.row(value)?, vec![None, None]);
            integrated += weight;
        }
        assert_eq!(integrated, expected);
        query.commit(prepared)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
    }
    Ok(())
}

#[test]
fn native_scopes_keep_filters_and_reject_deferred_inputs() -> Result<()> {
    let native = contract();
    let sql = "SELECT l.auction,r.other FROM(SELECT auction FROM source.bid WHERE price>=0 OR price IS NULL) l CROSS JOIN(SELECT other FROM source.auction WHERE category IS NULL) r";
    let compiled = compile(sql, &native)?;
    let ir = compiled.relational().context("missing cross join graph")?;
    assert_eq!(ir.sources.len(), 2);
    assert_eq!(
        ir.nodes
            .iter()
            .filter(|node| matches!(node, crate::compiler::relational::Node::Filter { .. }))
            .count(),
        2
    );
    for invalid in [
        sql.replace("l.auction,r.other", "l.price,r.other"),
        sql.replace("CROSS JOIN", "LEFT JOIN"),
        sql.replace("SELECT auction FROM", "SELECT abs(price) AS auction FROM"),
    ] {
        assert!(compile(&invalid, &native).is_err(), "{invalid}");
    }
    Ok(())
}
