//! Frame evaluation domains and internal SQL-value boundaries.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, worker::program::relational};
use anyhow::Result;

#[tokio::test]
async fn empty_frames_and_filters_do_not_evaluate_overflowing_arguments() -> Result<()> {
    for (frame, filter, succeeds) in [
        ("2 FOLLOWING AND 1 FOLLOWING", "", true),
        ("CURRENT ROW AND CURRENT ROW", " FILTER(WHERE price>0)", true),
        ("CURRENT ROW AND CURRENT ROW", "", false),
    ] {
        let native = contract();
        let sql = format!(
            "SELECT id,SUM(abs(price)){filter} OVER(PARTITION BY abs(auction) ORDER BY id ROWS BETWEEN {frame}) AS v FROM source.bid"
        );
        let compiled = compile(&sql, &native)?;
        let options = settings();
        let mut query = relational::build(&native, &compiled, &options)?;
        let before = query.checkpoint()?;
        let delta = batch(vec![(
            "bid",
            row(&[("id", Some("1")), ("auction", None), ("price", Some("-9223372036854775808"))]),
            1,
        )]);
        let result = query.prepare(relational::inputs(&delta, compiled.relational(), 1)?).await;
        if succeeds {
            let prepared = result?;
            let output = prepared.output().batch.iter().next().map(|((_, row), _)| row);
            assert_eq!(output.and_then(|row| row.get("@aggregate_0")), Some(&None));
            query.commit(prepared)?;
            super::restart_relational(&mut query, &native, &compiled, &options).await?;
        } else {
            assert!(result.is_err());
            assert_eq!(query.checkpoint()?, before);
        }
    }
    Ok(())
}

#[tokio::test]
async fn intermediate_average_is_rendered_once_before_numeric_arithmetic() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT d.g,COALESCE(d.v,0::numeric)+1 AS v FROM (SELECT auction AS g,AVG(price) AS v FROM source.bid GROUP BY auction) d WHERE d.v>1",
        &native,
    )?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("auction", None), ("price", Some("1"))]);
    let b = row(&[("id", Some("2")), ("auction", None), ("price", Some("2"))]);
    for (time, weight, total) in [(1, 1, "2.5000000000000000"), (2, -1, "2.5000000000000000")] {
        let delta = batch(vec![("bid", a.clone(), weight), ("bid", b.clone(), weight)]);
        let prepared =
            query.prepare(relational::inputs(&delta, compiled.relational(), time)?).await?;
        let output = compiled.output().columns[1].column.name.clone();
        let expected = crate::engine::Batch::from_updates([(
            (
                crate::worker::program::relational::Key::new(),
                row(&[("0:auction", None), (&output, Some(total))]),
            ),
            weight,
        )])?;
        assert_eq!(prepared.output().batch, expected);
        query.commit(prepared)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
    }
    Ok(())
}

#[test]
fn unsupported_window_identity_and_join_domains_still_reject() {
    for sql in [
        "SELECT id,SUM(abs(price)) OVER(ORDER BY abs(id) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM source.bid",
        "SELECT id,COUNT(DISTINCT abs(price)) OVER(PARTITION BY auction) FROM source.bid",
        "SELECT id,LAG(abs(price)) OVER(ORDER BY id) FROM source.bid",
        "SELECT a.id FROM source.auction a LEFT JOIN source.bid b ON a.id+b.id=b.auction",
        "SELECT a.id FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE abs(a.id+b.id)=b.auction)",
        "SELECT d.v::bigint FROM (SELECT auction,AVG(price) AS v FROM source.bid GROUP BY auction) d",
    ] {
        assert!(compile(sql, &contract()).is_err(), "{sql}");
    }
}

#[tokio::test]
async fn computed_outer_and_existence_preserve_weights_and_error_atomicity() -> Result<()> {
    for kind in ["outer", "exists", "anti"] {
        weighted_join(kind).await?;
    }
    Ok(())
}
async fn weighted_join(kind: &str) -> Result<()> {
    let native = contract();
    let condition = "abs(a.id)=abs(b.auction)";
    let sql = if kind == "outer" {
        format!(
            "SELECT a.category,b.price FROM source.auction a LEFT JOIN source.bid b ON {condition}"
        )
    } else {
        let not = if kind == "anti" { "NOT " } else { "" };
        format!(
            "SELECT a.category FROM source.auction a WHERE {not}EXISTS(SELECT 1 FROM source.bid b WHERE {condition})"
        )
    };
    let compiled = compile(&sql, &native)?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("category", Some("same")), ("other", None)]);
    let b = row(&[("id", Some("1")), ("auction", Some("-1")), ("price", Some("7"))]);
    let cases = [
        (vec![("auction", a, 2), ("bid", b.clone(), 3)], join_expected(kind, 3)?),
        (vec![("bid", b.clone(), -2)], join_expected(kind, 1)?),
        (vec![("bid", b.clone(), -1)], join_expected(kind, 0)?),
        (vec![("bid", b.clone(), 1)], join_expected(kind, 1)?),
        (vec![("bid", b, -1)], join_expected(kind, 0)?),
    ];
    let mut prior = crate::engine::Batch::from_updates([])?;
    for (updates, expected) in cases {
        let delta = batch(updates);
        let prepared = query
            .prepare(relational::inputs(&delta, compiled.relational(), query.time() + 1)?)
            .await?;
        assert_eq!(prepared.output().batch, super::bag_delta(&prior, &expected)?);
        query.commit(prepared)?;
        prior = expected;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
    }
    // Opposite side is empty; ON evaluation still rejects an overflowing input.
    let before = query.checkpoint()?;
    let delta = batch(vec![(
        "auction",
        row(&[("id", Some("-2147483648")), ("category", None), ("other", None)]),
        1,
    )]);
    assert!(
        query
            .prepare(relational::inputs(&delta, compiled.relational(), query.time() + 1)?)
            .await
            .is_err()
    );
    assert_eq!(query.checkpoint()?, before);
    Ok(())
}
fn join_expected(
    kind: &str,
    matches: i64,
) -> Result<crate::engine::Batch<relational::Key, crate::transaction::Row>> {
    let mut output = row(&[("0:category", Some("same"))]);
    let weight = match kind {
        "outer" => {
            output.insert("1:price".into(), (matches > 0).then(|| "7".into()));
            if matches > 0 { 2 * matches } else { 2 }
        }
        "exists" => {
            if matches > 0 {
                2
            } else {
                0
            }
        }
        _ => {
            if matches > 0 {
                0
            } else {
                2
            }
        }
    };
    crate::engine::Batch::from_updates([(
        (crate::worker::program::relational::Key::new(), output),
        weight,
    )])
}

#[tokio::test]
async fn independent_average_scopes_join_on_rendered_numeric_values() -> Result<()> {
    for (inner, expected) in [
        (
            "SELECT auction AS g,AVG(price) AS v FROM source.bid GROUP BY auction",
            "2.0000000000000000",
        ),
        ("SELECT id AS g,abs(price) AS v FROM source.bid", "2"),
    ] {
        independent_scopes(inner, expected).await?;
    }
    Ok(())
}
async fn independent_scopes(inner: &str, expected: &str) -> Result<()> {
    let native = contract();
    let compiled = compile(
        &format!("SELECT a.v AS x,b.v AS y FROM ({inner}) a JOIN ({inner}) b ON a.v=b.v"),
        &native,
    )?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let input = row(&[("id", Some("1")), ("auction", None), ("price", Some("2"))]);
    for weight in [1, -1] {
        let delta = batch(vec![("bid", input.clone(), weight)]);
        let prepared = query
            .prepare(relational::inputs(&delta, compiled.relational(), query.time() + 1)?)
            .await?;
        let output = compiled
            .output()
            .columns
            .iter()
            .map(|column| (column.column.name.clone(), Some(expected.into())))
            .collect();
        let expected = crate::engine::Batch::from_updates([(
            (crate::worker::program::relational::Key::new(), output),
            weight,
        )])?;
        assert_eq!(prepared.output().batch, expected);
        query.commit(prepared)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
    }
    Ok(())
}
