//! Native numeric AVG retains exact SUM scales/specials through reversible histories.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, worker::program::relational};
use anyhow::{Context, Result};
#[tokio::test]
async fn numeric_average_recovers_scales_and_finite_values_after_special_retractions() -> Result<()>
{
    let mut native = contract();
    native.relations[1].columns[2].oid = 1700;
    for grouped in [false, true] {
        let group = if grouped { "auction," } else { "" };
        let suffix = if grouped { " GROUP BY auction" } else { "" };
        let compiled = compile(
            &format!("SELECT {group} AVG(price),SUM(price) FROM source.bid{suffix}"),
            &native,
        )?;
        let options = settings();
        let mut query = relational::build(&native, &compiled, &options)?;
        let value = |id, price| row(&[("id", Some(id)), ("auction", Some("1")), ("price", price)]);
        let a = value("1", Some("1.2000"));
        let b = value("2", Some("2.30"));
        let null = value("3", None);
        let nan = value("4", Some("NaN"));
        let positive = value("5", Some("Infinity"));
        let negative = value("6", Some("-Infinity"));
        let histories = [
            (
                vec![("bid", a.clone(), 2), ("bid", b.clone(), 1), ("bid", null.clone(), 1)],
                Some((Some("1.5666666666666667"), Some("4.7000"))),
            ),
            (vec![("bid", a.clone(), -1)], Some((Some("1.7500000000000000"), Some("3.5000")))),
            (
                vec![
                    ("bid", nan.clone(), 1),
                    ("bid", positive.clone(), 1),
                    ("bid", negative.clone(), 1),
                ],
                Some((Some("NaN"), Some("NaN"))),
            ),
            (
                vec![("bid", nan, -1), ("bid", negative, -1)],
                Some((Some("Infinity"), Some("Infinity"))),
            ),
            (vec![("bid", a, -1)], Some((Some("Infinity"), Some("Infinity")))),
            (vec![("bid", positive, -1)], Some((Some("2.3000000000000000"), Some("2.30")))),
            (vec![("bid", b, -1)], Some((None, None))),
            (vec![("bid", null, -1)], if grouped { None } else { Some((None, None)) }),
        ];
        let mut previous = Batch::default();
        for (changes, expected) in histories {
            let after = Batch::from_updates(expected.map(|(average, sum)| {
                let mut values = grouped.then_some(Some("1")).into_iter().collect::<Vec<_>>();
                values.extend(<[Option<&str>; 2]>::from((average, sum)));
                let row = compiled
                    .output()
                    .columns
                    .iter()
                    .zip(values)
                    .map(|(column, value)| (column.column.name.clone(), value.map(str::to_owned)))
                    .collect();
                ((relational::Key::new(), row), 1)
            }))?;
            let changes = batch(changes);
            let work = query
                .prepare(relational::inputs(&changes, compiled.relational(), query.time() + 1)?)
                .await?;
            assert_eq!(work.output().batch, super::bag_delta(&previous, &after)?);
            query.commit(work)?;
            previous = after;
            super::restart_relational(&mut query, &native, &compiled, &options).await?;
            let compact = query.prepare_compaction().await?;
            query.commit_compaction(compact)?;
        }
    }
    Ok(())
}

#[test]
fn unary_numeric_arithmetic_compiles_through_the_full_sql_frontend() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT id,id/2 AS a,-id/2 AS b,id/(-2) AS c,id::bigint/2 AS d,-id::bigint AS e,-(id::numeric) AS f FROM source.bid",
        &native,
    )?;
    relational::build(&native, &compiled, &settings())?;
    Ok(())
}

#[tokio::test]
async fn row_dependent_failures_are_marked_and_preserve_the_prior_boundary() -> Result<()> {
    let mut native = super::contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    let compiled = crate::compiler::compile("SELECT 1/other AS v FROM source.auction", &native)?;
    let ir = compiled.relational();
    let query = crate::worker::program::relational::build(&native, &compiled, &super::settings())?;
    let poison = super::row(&[("id", Some("1")), ("category", None), ("other", Some("0"))]);
    let insert = super::batch(vec![("auction", poison.clone(), 1)]);
    let inputs = crate::worker::program::relational::inputs(&insert, ir, 1)?;
    let error = query.prepare(inputs).await.err().context("division accepted")?;
    assert!(error.is::<crate::engine::execution::EvaluationFailed>(), "{error:#}");
    assert_eq!(query.time(), 0);
    let cancelled = super::batch(vec![("auction", poison.clone(), 1), ("auction", poison, -1)]);
    let inputs = crate::worker::program::relational::inputs(&cancelled, ir, 1)?;
    let prepared = query.prepare(inputs).await?;
    assert_eq!(prepared.output().batch.iter().count(), 0);
    Ok(())
}
