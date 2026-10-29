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
fn numeric_binding_admits_exact_rounding_division_and_average() {
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
        assert!(compile(sql, &native).is_ok(), "{sql}");
    }
}

fn numeric_contract() -> crate::source::Contract {
    let mut native = contract();
    native.relations[1].columns[2].oid = 1700;
    native
}

#[tokio::test]
async fn numeric_grouped_extrema_share_one_scale_representative_through_retractions() -> Result<()>
{
    let native = numeric_contract();
    let options = settings();
    for aggregates in ["MIN(auction),MAX(auction)", "MIN(auction)", "MAX(auction)"] {
        let compiled =
            compile(&format!("SELECT price,{aggregates} FROM source.bid GROUP BY price"), &native)?;
        assert!(
            compiled
                .relational()
                .nodes
                .iter()
                .any(|node| matches!(node, crate::compiler::relational::Node::Partition { .. }))
        );
        let mut history = super::History::new((&native, &compiled, &options));
        history.snapshot = true;
        history.compact = true;
        history
            .run(
                numeric_key_changes(),
                |state| numeric_key_extrema(state, aggregates),
                super::apply_rows,
                |_| true,
            )
            .await?;
    }
    Ok(())
}

fn numeric_key_changes() -> Vec<crate::weighted::Batch> {
    let member =
        |id, value, group| row(&[("id", Some(id)), ("auction", Some(value)), ("price", group)]);
    let low = member("1", "1", Some("1.20"));
    let high = member("2", "5", Some("1.2000"));
    let moved = member("1", "9", Some("1.20"));
    let rescaled = member("1", "9", Some("1.2"));
    let null = member("3", "2", None);
    [
        vec![("bid", low.clone(), 2), ("bid", high.clone(), 1), ("bid", null.clone(), 1)],
        vec![("bid", low.clone(), -1), ("bid", moved.clone(), 1)],
        vec![("bid", low, -1)],
        vec![("bid", high, -1)],
        vec![("bid", moved, -1), ("bid", rescaled.clone(), 1)],
        vec![("bid", rescaled, -1)],
        vec![("bid", null, -1)],
    ]
    .into_iter()
    .map(batch)
    .collect()
}

fn numeric_key_extrema(state: &super::RowState, aggregates: &str) -> Result<super::RowBatch> {
    let mut groups =
        std::collections::BTreeMap::<bool, Vec<(i64, &crate::transaction::Row)>>::new();
    for (row, weight) in state {
        if *weight > 0 {
            // The fixture's three finite spellings are the same fixed decimal;
            // its other group is NULL. The oracle uses no production numeric code.
            let key = row["price"].is_some();
            groups.entry(key).or_default().push((
                row["auction"].as_deref().context("missing oracle argument")?.parse()?,
                row,
            ));
        }
    }
    let rows = groups
        .into_values()
        .map(|members| {
            let low = members.iter().min_by_key(|(value, _)| value).context("empty group")?;
            let high = members.iter().max_by_key(|(value, _)| value).context("empty group")?;
            let values = if aggregates.contains(',') {
                vec![low.0, high.0]
            } else if aggregates.starts_with("MIN") {
                vec![low.0]
            } else {
                vec![high.0]
            };
            let mut output =
                crate::transaction::Row::from([("0:price".into(), low.1["price"].clone())]);
            for (index, value) in values.into_iter().enumerate() {
                output.insert(format!("@aggregate_{index}"), Some(value.to_string()));
            }
            Ok(((relational::Key::new(), output), 1))
        })
        .collect::<Result<Vec<_>>>()?;
    Batch::from_updates(rows)
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

#[tokio::test]
async fn mixed_numeric_statistics_keep_scale_null_filters_and_distinct_through_restart()
-> Result<()> {
    for bytes in [0, 1, 4 * 1024 * 1024] {
        mixed_statistics_history(bytes).await?;
    }
    Ok(())
}
async fn mixed_statistics_history(native_cache_bytes: usize) -> Result<()> {
    let native = numeric_contract();
    let compiled = compile(
        "SELECT auction,COUNT(*),COUNT(price),SUM(price),SUM(id),AVG(id),SUM(price) FILTER(WHERE id<2),COUNT(DISTINCT id) FROM source.bid GROUP BY auction",
        &native,
    )?;
    assert_additive_graph(&compiled);
    let mut options = settings();
    options.limits.native_cache_bytes = native_cache_bytes;
    options.limits.resident_entries = 1;
    let mut query = relational::build(&native, &compiled, &options)?;
    for (changes, expected) in mixed_changes().into_iter().zip(MIXED_EXPECTED) {
        let changes = batch(changes);
        let candidate = query
            .prepare(relational::inputs(&changes, compiled.relational(), query.time() + 1)?)
            .await?;
        let positive = candidate.output().batch.iter().find(|(_, weight)| **weight > 0);
        if let Some(expected) = expected {
            let output = &positive.context("missing mixed statistic replacement")?.0.1;
            for (column, value) in compiled.output().columns.iter().skip(1).zip(expected) {
                assert_eq!(
                    output
                        .get(&column.column.name)
                        .context("missing mixed output column")?
                        .as_deref(),
                    value
                );
            }
        } else {
            assert!(positive.is_none());
        }
        query.commit(candidate)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
    }
    Ok(())
}

fn assert_additive_graph(compiled: &crate::compiler::Compiled) {
    assert!(
        compiled
            .relational()
            .nodes
            .iter()
            .any(|node| matches!(node, crate::compiler::relational::Node::Statistics { .. }))
    );
    assert!(
        !compiled
            .relational()
            .nodes
            .iter()
            .any(|node| matches!(node, crate::compiler::relational::Node::Partition { .. }))
    );
}

type MixedChanges = Vec<(&'static str, crate::transaction::Row, i64)>;
fn mixed_changes() -> [MixedChanges; 5] {
    let high = row(&[("id", Some("1")), ("auction", None), ("price", Some("1.2000"))]);
    let low = row(&[("id", Some("2")), ("auction", None), ("price", Some("2.30"))]);
    let null = row(&[("id", Some("3")), ("auction", None), ("price", None)]);
    let replacement = row(&[("id", Some("1")), ("auction", None), ("price", Some("1.2"))]);
    [
        vec![("bid", high.clone(), 1), ("bid", low.clone(), 1), ("bid", null.clone(), 1)],
        vec![("bid", high, -1), ("bid", replacement.clone(), 1)],
        vec![("bid", replacement, -1)],
        vec![("bid", low, -1)],
        vec![("bid", null, -1)],
    ]
}
type MixedExpected = Option<[Option<&'static str>; 7]>;
const MIXED_EXPECTED: [MixedExpected; 5] = [
    Some([
        Some("3"),
        Some("2"),
        Some("3.5000"),
        Some("6"),
        Some("2.0000000000000000"),
        Some("1.2000"),
        Some("3"),
    ]),
    Some([
        Some("3"),
        Some("2"),
        Some("3.50"),
        Some("6"),
        Some("2.0000000000000000"),
        Some("1.2"),
        Some("3"),
    ]),
    Some([
        Some("2"),
        Some("1"),
        Some("2.30"),
        Some("5"),
        Some("2.5000000000000000"),
        None,
        Some("2"),
    ]),
    Some([Some("1"), Some("0"), None, Some("3"), Some("3.0000000000000000"), None, Some("1")]),
    None,
];
