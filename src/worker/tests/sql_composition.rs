//! SQL composition uses the ordinary retained circuit and independent weighted oracles.
use super::{History, RowState, batch, contract, row, settings};
use crate::{
    compiler::{Compiled, compile},
    engine::Batch,
    worker::program::relational,
};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
type Summaries = BTreeMap<Option<String>, (i64, Option<i64>)>;

fn output(
    compiled: &Compiled,
    rows: impl IntoIterator<Item = (Vec<Option<String>>, i64)>,
) -> Result<super::RowBatch> {
    Batch::from_updates(rows.into_iter().map(|(values, weight)| {
        let row = compiled
            .relational()
            .output
            .columns
            .iter()
            .zip(values)
            .map(|(column, value)| (column.column.name.clone(), value))
            .collect();
        ((relational::Key::new(), row), weight)
    }))
}
fn changes() -> Vec<crate::weighted::Batch> {
    let a = row(&[("id", Some("1")), ("auction", None), ("price", None)]);
    let b = row(&[("id", Some("2")), ("auction", Some("1")), ("price", Some("7"))]);
    let c = row(&[("id", Some("3")), ("auction", Some("1")), ("price", Some("-3"))]);
    vec![
        batch(vec![]),
        batch(vec![("bid", a.clone(), 2), ("bid", b.clone(), 3)]),
        batch(vec![("bid", b.clone(), -2), ("bid", c.clone(), 2)]),
        batch(vec![("bid", a, -2)]),
        batch(vec![("bid", b, -1), ("bid", c, -2)]),
        batch(vec![]),
    ]
}
#[tokio::test]
async fn global_aggregate_zero_row_survives_empty_restart_and_compaction() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT COUNT(*) AS n,SUM(price) AS s,MIN(price) AS lo,MAX(price) AS hi,COUNT(DISTINCT price) AS d FROM source.bid HAVING COUNT(*)>=0",
        &native,
    )?;
    let options = settings();
    let mut history = History::new((&native, &compiled, &options));
    history.compact = true;
    history
        .run(
            changes(),
            |state: &RowState| {
                let count: i64 = state.values().sum();
                let mut present = BTreeMap::new();
                for (row, weight) in state {
                    if let Some(value) = &row["price"] {
                        *present.entry(value.parse::<i64>()?).or_insert(0_i64) += weight;
                    }
                }
                present.retain(|_, weight| *weight > 0);
                let sum = (!present.is_empty()).then(|| {
                    present.iter().map(|(value, weight)| value * weight).sum::<i64>().to_string()
                });
                output(
                    &compiled,
                    [(
                        vec![
                            Some(count.to_string()),
                            sum,
                            present.first_key_value().map(|(v, _)| v.to_string()),
                            present.last_key_value().map(|(v, _)| v.to_string()),
                            Some(present.len().to_string()),
                        ],
                        1,
                    )],
                )
            },
            super::apply_rows,
            |_| true,
        )
        .await?;
    Ok(())
}
#[tokio::test]
async fn having_hidden_aggregates_filter_before_grouped_window() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT auction AS g,SUM(price) AS s,MAX(COUNT(*)) OVER() AS m FROM source.bid GROUP BY auction HAVING COUNT(*)>=2",
        &native,
    )?;
    let options = settings();
    let mut history = History::new((&native, &compiled, &options));
    history.compact = true;
    history
        .run(
            changes(),
            |state: &RowState| {
                let mut groups = Summaries::new();
                for (row, weight) in state {
                    let value = groups.entry(row["auction"].clone()).or_default();
                    value.0 += weight;
                    if let Some(price) = &row["price"] {
                        value.1 = Some(value.1.unwrap_or(0) + price.parse::<i64>()? * weight);
                    }
                }
                groups.retain(|_, (count, _)| *count >= 2);
                let maximum = groups.values().map(|(count, _)| *count).max();
                output(
                    &compiled,
                    groups.into_iter().map(|(group, (_, sum))| {
                        (vec![group, sum.map(|v| v.to_string()), maximum.map(|v| v.to_string())], 1)
                    }),
                )
            },
            super::apply_rows,
            |_| true,
        )
        .await?;
    Ok(())
}
#[tokio::test]
async fn repeated_cte_union_preserves_weights_across_ordered_cutoff() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "WITH c(g) AS (SELECT price FROM source.bid) SELECT c.g FROM c UNION ALL SELECT c.g FROM c ORDER BY g DESC NULLS LAST LIMIT 4 OFFSET 1",
        &native,
    )?;
    let options = settings();
    let mut history = History::new((&native, &compiled, &options));
    history.compact = true;
    history
        .run(
            changes(),
            |state: &RowState| {
                let mut values = Vec::new();
                for (row, weight) in state {
                    let value = row["price"].as_deref().map(str::parse::<i64>).transpose()?;
                    for _ in 0..2 * weight {
                        values.push(value);
                    }
                }
                values.sort_by_key(|value| (value.is_none(), value.map(std::cmp::Reverse)));
                output(
                    &compiled,
                    values
                        .into_iter()
                        .skip(1)
                        .take(4)
                        .map(|value| (vec![value.map(|v| v.to_string())], 1)),
                )
            },
            super::apply_rows,
            |_| true,
        )
        .await?;
    Ok(())
}
#[test]
fn sql_composition_validates_scopes_and_common_native_types() -> Result<()> {
    let native = contract();
    for sql in [
        "SELECT 1 UNION ALL SELECT NULL ORDER BY 1",
        "SELECT true UNION ALL SELECT NULL",
        "SELECT 1::numeric UNION ALL SELECT '2'",
        "SELECT 'yes' UNION ALL SELECT true",
        "WITH c(x) AS (SELECT price FROM source.bid),d AS(SELECT x FROM c) SELECT x FROM d",
        "WITH c AS(SELECT price FROM source.bid) SELECT q.id FROM(WITH c AS(SELECT id FROM source.bid) SELECT id FROM c)q",
        "SELECT COUNT(*) FROM source.bid",
        "SELECT a.category,COUNT(*) OVER(),SUM(b.price) FROM source.auction a JOIN source.bid b ON a.id=b.auction WHERE a.other IS NOT NULL GROUP BY a.category",
        "SELECT ROW_NUMBER() OVER(ORDER BY COUNT(*)) FROM source.bid",
        "WITH c AS(SELECT auction AS g FROM source.bid UNION ALL SELECT id FROM source.auction), d AS(SELECT g,COUNT(*) AS n FROM c GROUP BY g HAVING COUNT(*)>=2) SELECT g,n FROM d ORDER BY n DESC,g LIMIT 4",
        "SELECT 7 FROM source.bid HAVING TRUE",
        "SELECT COUNT(*) FROM source.bid WHERE false HAVING COUNT(*)=0",
        "SELECT auction,COUNT(*) FROM source.bid b GROUP BY b.auction HAVING auction>0",
        "SELECT auction,COUNT(*) FROM source.bid b GROUP BY auction HAVING b.auction>0",
        "SELECT auction FROM source.bid GROUP BY auction HAVING AVG(price)>0",
        "SELECT auction FROM source.bid GROUP BY auction HAVING COUNT(*) FILTER(WHERE price>0)>=2",
    ] {
        let compiled = compile(sql, &native).with_context(|| sql.to_owned())?;
        relational::build(&native, &compiled, &settings()).with_context(|| sql.to_owned())?;
    }
    Ok(())
}
#[test]
fn sql_composition_rejects_invalid_unused_bindings_and_clause_scopes() {
    let native = contract();
    for sql in [
        "WITH c(x,y) AS(SELECT id FROM source.bid) SELECT id FROM source.bid",
        "WITH c AS(SELECT missing FROM source.bid) SELECT id FROM source.bid",
        "WITH c AS(SELECT nonsense(id) FROM source.bid) SELECT id FROM source.bid",
        "WITH RECURSIVE c AS(SELECT 1) SELECT 1",
        "WITH c AS(DELETE FROM source.bid RETURNING id) SELECT 1",
        "WITH c AS(SELECT id FROM c) SELECT id FROM c",
        "WITH c AS(SELECT id FROM d),d AS(SELECT id FROM source.bid) SELECT id FROM c",
        "SELECT auction AS g,COUNT(*) AS n FROM source.bid GROUP BY auction HAVING n>0",
        "SELECT auction FROM source.bid GROUP BY auction HAVING price>0",
        "SELECT auction FROM source.bid GROUP BY auction HAVING ROW_NUMBER() OVER()>0",
        "SELECT COUNT(*) FROM source.bid HAVING SUM(COUNT(*))>0",
        "SELECT b.id FROM source.bid b WHERE EXISTS(SELECT 1 FROM source.auction a WHERE a.id=b.auction HAVING COUNT(*)=0)",
        "SELECT b.id FROM source.bid b WHERE NOT EXISTS(SELECT 1 FROM source.auction a WHERE a.id=b.auction HAVING COUNT(*)=0)",
        "SELECT b.id,d.id FROM source.bid b LEFT JOIN LATERAL(SELECT a.id FROM source.auction a WHERE a.id=b.auction HAVING FALSE ORDER BY a.id LIMIT 1)d ON TRUE",
        "SELECT b.id,d.id FROM source.bid b LEFT JOIN LATERAL(SELECT a.id FROM source.auction a WHERE a.id=b.auction ORDER BY a.id LIMIT 1)d ON TRUE HAVING FALSE",
        "SELECT 1 UNION SELECT 1",
        "SELECT 1 INTERSECT ALL SELECT 1",
        "SELECT 1 EXCEPT ALL SELECT 1",
        "SELECT 1 UNION ALL SELECT true",
        "SELECT 1 UNION ALL SELECT 1,2",
        "SELECT 1 AS x UNION ALL SELECT 2 ORDER BY x+1",
        "SELECT id FROM source.bid UNION ALL SELECT id FROM source.bid ORDER BY source.bid.id",
        "SELECT 1 UNION ALL SELECT 'invalid'",
        "SELECT DISTINCT NULL AS x UNION ALL SELECT 1",
        "(SELECT NULL AS x ORDER BY 1) UNION ALL SELECT 1",
        "SELECT 1 WHERE false UNION ALL SELECT true LIMIT 0",
    ] {
        assert!(compile(sql, &native).is_err(), "accepted {sql}");
    }
    let chain = (0..20)
        .map(|index| {
            if index == 0 {
                "c0 AS(SELECT id FROM source.bid)".into()
            } else {
                format!("c{index} AS(SELECT id FROM c{})", index - 1)
            }
        })
        .collect::<Vec<String>>()
        .join(",");
    let error = compile(&format!("WITH {chain} SELECT id FROM c19"), &native)
        .err()
        .map(|error| format!("{error:#}"));
    assert!(error.is_some_and(|error| error.contains("expanded CTE scope budget")));
}

#[tokio::test]
async fn constant_relation_where_precedes_global_group_and_union() -> Result<()> {
    let native = contract();
    let options = settings();
    for (sql, value) in [
        ("SELECT 7::bigint WHERE FALSE", None),
        ("SELECT 7::bigint WHERE NULL", None),
        ("SELECT COUNT(*) WHERE FALSE", Some(Some("0"))),
        ("SELECT SUM(1) WHERE NULL", Some(None)),
        ("SELECT COUNT(*) WHERE FALSE HAVING COUNT(*)=0", Some(Some("0"))),
        ("SELECT 7 WHERE FALSE UNION ALL SELECT 8", Some(Some("8"))),
    ] {
        let compiled = compile(sql, &native).with_context(|| sql.to_owned())?;
        let mut query = relational::build(&native, &compiled, &options)?;
        let prepared = query
            .prepare(relational::inputs(&batch(vec![]), compiled.relational(), 1)?)
            .await
            .with_context(|| sql.to_owned())?;
        let expected =
            output(&compiled, value.into_iter().map(|value| (vec![value.map(str::to_owned)], 1)))?;
        assert_eq!(prepared.output().batch, expected, "{sql}");
        query.commit(prepared)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
        let next =
            query.prepare(relational::inputs(&batch(vec![]), compiled.relational(), 2)?).await?;
        assert!(next.output().batch.iter().next().is_none(), "{sql}");
    }
    Ok(())
}

#[test]
fn union_text_candidate_and_collation_follow_native_branches() -> Result<()> {
    let mut native = contract();
    native.relations[0].columns[1].collation = 950;
    native.relations[0].columns[2].oid = 1043;
    native.relations[0].columns[2].collation = 950;
    native.relations[0].columns[2].modifier = 7;
    for (left, right, oid) in [("category", "other", 25), ("other", "category", 1043)] {
        let compiled = compile(
            &format!(
                "SELECT {left} AS x FROM source.auction UNION ALL SELECT {right} FROM source.auction ORDER BY x LIMIT 2"
            ),
            &native,
        )?;
        assert_eq!(compiled.output().columns[0].column.oid, oid);
        assert!(
            !crate::compiler::properties::Analysis::infer(&native, compiled.relational())?
                .edge("project")?
                .unique_rows()
        );
        relational::build(&native, &compiled, &settings())?;
    }
    native.relations[0].columns[1].collation = 951;
    assert!(compile("SELECT category AS x FROM source.auction UNION ALL SELECT other FROM source.auction ORDER BY x LIMIT 2",&native).is_err());
    Ok(())
}

#[tokio::test]
async fn global_neutral_seed_does_not_hide_negative_source_membership() -> Result<()> {
    let native = contract();
    let options = settings();
    let missing = row(&[("id", Some("9")), ("auction", Some("1")), ("price", Some("7"))]);
    for aggregates in ["COUNT(*)", "MIN(price),MAX(price)", "COUNT(DISTINCT price)"] {
        let compiled = compile(&format!("SELECT {aggregates} FROM source.bid"), &native)?;
        let mut query = relational::build(&native, &compiled, &options)?;
        query.commit(
            query.prepare(relational::inputs(&batch(vec![]), compiled.relational(), 1)?).await?,
        )?;
        let before = query.checkpoint()?;
        let changes = batch(vec![("bid", missing.clone(), -1)]);
        assert!(
            query.prepare(relational::inputs(&changes, compiled.relational(), 2)?).await.is_err(),
            "{aggregates}"
        );
        assert_eq!(query.checkpoint()?, before);
        let retry =
            query.prepare(relational::inputs(&batch(vec![]), compiled.relational(), 2)?).await?;
        assert!(retry.output().batch.iter().next().is_none());
    }
    Ok(())
}

#[tokio::test]
async fn global_having_restores_zero_after_simultaneous_join_input_changes() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT COUNT(*) AS n FROM source.auction a JOIN source.bid b ON a.id=b.auction HAVING COUNT(*)=0",
        &native,
    )?;
    let options = settings();
    let a = row(&[("id", Some("1")), ("category", None), ("other", None)]);
    let next_a = row(&[("id", Some("2")), ("category", None), ("other", None)]);
    let b = row(&[("id", Some("1")), ("auction", Some("2")), ("price", Some("7"))]);
    let next_b = row(&[("id", Some("1")), ("auction", Some("2")), ("price", Some("5"))]);
    let mut history = History::new((&native, &compiled, &options));
    history.compact = true;
    history.snapshot = true;
    history
        .run(
            [
                batch(vec![]),
                batch(vec![("auction", a.clone(), 1), ("bid", b.clone(), 1)]),
                batch(vec![
                    ("auction", a, -1),
                    ("auction", next_a.clone(), 1),
                    ("bid", b, -1),
                    ("bid", next_b.clone(), 1),
                ]),
                batch(vec![("auction", next_a, -1), ("bid", next_b, -1)]),
            ],
            |state: &super::JoinedState| {
                let count: i64 = state
                    .0
                    .iter()
                    .flat_map(|(a, wa)| {
                        state
                            .1
                            .iter()
                            .filter(move |(b, _)| b["auction"].is_some() && a["id"] == b["auction"])
                            .map(move |(_, wb)| wa * wb)
                    })
                    .sum();
                output(&compiled, (count == 0).then(|| (vec![Some("0".into())], 1)))
            },
            super::apply_joined,
            |_| true,
        )
        .await?;
    Ok(())
}
