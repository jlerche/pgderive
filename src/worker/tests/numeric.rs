//! Exact decimal circuit histories with an independent fixed-hundredths oracle.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, worker::program::relational};
use anyhow::{Context, Result};
#[tokio::test]
async fn numeric_grouped_history_uses_raw_scale_and_cold_restart() -> Result<()> {
    let native = numeric_contract();
    let compiled = compile(
        "SELECT auction AS g, COUNT(price) AS n, SUM(price) AS s, MIN(price) AS lo, MAX(price) AS hi FROM source.bid GROUP BY auction",
        &native,
    )?;
    let ir = compiled.relational();
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let inputs = [
        row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("1.20"))]),
        row(&[("id", Some("2")), ("auction", Some("1")), ("price", Some("2.30"))]),
        row(&[("id", Some("3")), ("auction", Some("1")), ("price", None)]),
    ];
    let mut state = std::collections::BTreeMap::new();
    let mut previous = Batch::from_updates([])?;
    for (index, changes) in [
        vec![
            ("bid", inputs[0].clone(), 2),
            ("bid", inputs[1].clone(), 1),
            ("bid", inputs[2].clone(), 1),
        ],
        vec![("bid", inputs[0].clone(), -1)],
        vec![("bid", inputs[1].clone(), -1)],
        vec![("bid", inputs[0].clone(), -1)],
        vec![("bid", inputs[2].clone(), -1)],
    ]
    .into_iter()
    .enumerate()
    {
        for (_, row, weight) in &changes {
            *state.entry(row.clone()).or_insert(0_i64) += weight;
        }
        let values = state
            .iter()
            .flat_map(|(row, weight)| {
                let value =
                    row["price"].as_ref().and_then(|v| v.replace('.', "").parse::<i64>().ok());
                (0..*weight).filter_map(move |_| value)
            })
            .collect::<Vec<_>>();
        let decimal = |v: i64| format!("{}.{:02}", v / 100, v % 100);
        let after = if state.values().sum::<i64>() == 0 {
            Batch::from_updates([])?
        } else {
            Batch::from_updates([(
                (
                    crate::worker::program::relational::Key::new(),
                    [
                        ("0:auction".into(), Some("1".into())),
                        ("@aggregate_0".into(), Some(values.len().to_string())),
                        (
                            "@aggregate_1".into(),
                            (!values.is_empty()).then(|| decimal(values.iter().sum())),
                        ),
                        ("@aggregate_2".into(), values.iter().min().map(|v| decimal(*v))),
                        ("@aggregate_3".into(), values.iter().max().map(|v| decimal(*v))),
                    ]
                    .into(),
                ),
                1,
            )])?
        };
        let expected = super::bag_delta(&previous, &after)?;
        let changes = batch(changes);
        let candidate = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(candidate.output().batch, expected);
        query.commit(candidate)?;
        previous = after;
        if index == 1 {
            super::restart_relational(&mut query, &native, &compiled, &options).await?;
        }
    }
    Ok(())
}
#[test]
fn numeric_binding_rejects_unimplemented_rounding_and_division() {
    let mut native = contract();
    native.relations[1].columns[2].oid = 1700;
    let live = "SELECT \"B\".auction AS auction,COUNT(*) AS n,COUNT(\"B\".price) AS present,SUM(\"B\".price) AS total,MIN(\"B\".price) AS lo,MAX(\"B\".price) AS hi,COUNT(*) FILTER(WHERE \"B\".price<0) AS negative FROM (SELECT b.auction AS auction,b.price * 1.20 + 0.30 - 0.10 AS price FROM source.bid b WHERE b.price<0 OR b.price>=0 OR b.price IS NULL) \"B\" GROUP BY \"B\".auction";
    assert!(compile(live, &native).is_ok());
    for sql in [
        "SELECT auction, AVG(price) FROM source.bid GROUP BY auction",
        "SELECT auction, SUM(price / 2) FROM source.bid GROUP BY auction",
        "SELECT price::numeric(10,2) AS p FROM source.bid",
        "SELECT price::integer AS p FROM source.bid",
    ] {
        assert!(compile(sql, &native).is_err(), "{sql}");
    }
}

fn numeric_contract() -> crate::source::Contract {
    let mut native = contract();
    native.relations[1].columns[2].oid = 1700;
    native
}

#[tokio::test]
async fn computed_numeric_group_keys_and_native_join_keys_use_value_equality() -> Result<()> {
    let native = numeric_contract();
    for sql in [
        "SELECT q.p AS g, SUM(q.p) AS total FROM (SELECT price * 1.20 AS p FROM source.bid) q GROUP BY q.p",
        "SELECT a.id AS a,b.id AS b FROM source.bid a JOIN source.bid b ON a.price=b.price",
    ] {
        let compiled = compile(sql, &native)?;
        let ir = compiled.relational();
        let options = settings();
        let mut query = relational::build(&native, &compiled, &options)?;
        let change = batch(vec![
            ("bid", row(&[("id", Some("1")), ("auction", None), ("price", Some("1.0"))]), 1),
            ("bid", row(&[("id", Some("2")), ("auction", None), ("price", Some("1.00"))]), 1),
        ]);
        let candidate = query.prepare(relational::inputs(&change, ir, 1)?).await?;
        let count = candidate.output().batch.iter().map(|(_, weight)| weight).sum::<i64>();
        assert_eq!(count, if sql.contains("JOIN") { 4 } else { 1 });
        query.commit(candidate)?;
        let checkpoint = query.checkpoint()?;
        let mut query = relational::build(&native, &compiled, &options)?;
        query.restore_checkpoint(checkpoint).await?;
        let deletion = batch(vec![(
            "bid",
            row(&[("id", Some("1")), ("auction", None), ("price", Some("1.0"))]),
            -1,
        )]);
        let candidate = query.prepare(relational::inputs(&deletion, ir, 2)?).await?;
        let count = candidate.output().batch.iter().map(|(_, weight)| weight).sum::<i64>();
        assert_eq!(count, if sql.contains("JOIN") { -3 } else { 0 });
        query.commit(candidate)?;
    }
    Ok(())
}
#[tokio::test]
async fn numeric_sum_retracts_highest_scale_and_special_inputs() -> Result<()> {
    let native = numeric_contract();
    let compiled =
        compile("SELECT auction AS g,SUM(price) AS s FROM source.bid GROUP BY auction", &native)?;
    let ir = compiled.relational();
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let high = row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("1.2000"))]);
    let low = row(&[("id", Some("2")), ("auction", Some("1")), ("price", Some("2.30"))]);
    let nan = row(&[("id", Some("3")), ("auction", Some("1")), ("price", Some("NaN"))]);
    for (changes, expected) in [
        (vec![("bid", high.clone(), 1), ("bid", low, 1)], "3.5000"),
        (vec![("bid", high, -1)], "2.30"),
        (vec![("bid", nan.clone(), 1)], "NaN"),
        (vec![("bid", nan, -1)], "2.30"),
    ] {
        let changes = batch(changes);
        let candidate = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        let positive = candidate
            .output()
            .batch
            .iter()
            .find(|(_, weight)| **weight > 0)
            .context("missing numeric sum replacement")?;
        assert_eq!(positive.0.1["@aggregate_0"].as_deref(), Some(expected));
        query.commit(candidate)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
    }
    Ok(())
}
