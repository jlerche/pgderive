use super::super::{
    program::{self, relational},
    runtime,
};
use super::{batch, contract, row, settings};
use crate::{
    compiler::{Cell, compile},
    engine::Batch,
    transaction::Row,
};
use anyhow::Result;
use std::collections::BTreeMap;
const SQL: &str = "SELECT a.category AS c, a.other AS o, a.category AS again FROM source.auction a WHERE a.id >= 1 AND (a.other IS NOT NULL OR a.category IS NULL)";
fn oracle(rows: &BTreeMap<Row, i64>) -> Result<super::ProjectedBag> {
    Batch::from_updates(
        rows.iter()
            .filter(|(row, weight)| {
                **weight != 0
                    && row["id"]
                        .as_ref()
                        .is_some_and(|id| id.parse::<i64>().is_ok_and(|id| id >= 1))
                    && (row["other"].is_some() || row["category"].is_none())
            })
            .map(|(row, weight)| {
                let text = |name: &str| row[name].clone().map(Cell::Text);
                (((), vec![text("category"), text("other"), text("category")]), *weight)
            }),
    )
}
#[tokio::test]
async fn projection_collisions_retractions_nulls_and_cold_restart() -> Result<()> {
    let native = contract();
    let compiled = compile(SQL, &native)?;
    let options = settings();
    let mut query = super::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("category", None), ("other", None)]);
    let b = row(&[("id", Some("2")), ("category", None), ("other", None)]);
    let mut changed = b.clone();
    changed.insert("category".into(), Some("excluded".into()));
    let histories = [
        batch(vec![("auction", a.clone(), 1), ("auction", b.clone(), 1)]),
        batch(vec![("auction", b.clone(), -1), ("auction", changed.clone(), 1)]),
        batch(vec![("auction", changed, -1), ("auction", b.clone(), 1)]),
        batch(vec![("auction", a, -1)]),
        batch(vec![(
            "bid",
            row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("7"))]),
            1,
        )]),
        batch(vec![("auction", b, -1)]),
    ];
    let mut rows = BTreeMap::new();
    for (index, changes) in histories.into_iter().enumerate() {
        let before = oracle(&rows)?;
        for change in &changes.updates {
            if change.tuple.table == "auction" {
                *rows.entry(change.tuple.row.clone()).or_default() += change.weight;
            }
        }
        let after = oracle(&rows)?;
        let expected = super::bag_delta(&before, &after)?;
        let input = relational::inputs(&changes, compiled.relational(), query.time() + 1)?;
        let prepared = query.prepare(input).await?;
        assert_eq!(&super::projected_batch(&prepared.output().batch, &compiled)?, &expected);
        let candidate = query.prepared_checkpoint(&prepared)?;
        assert_eq!(candidate.time, query.time() + 1);
        query.commit(prepared)?;
        assert_eq!(super::projected_output(&query, &compiled).await?, after);
        if index == 2 {
            let checkpoint = query.checkpoint()?;
            query = super::build(&native, &compile(SQL, &native)?, &options)?;
            query.restore_checkpoint(checkpoint).await?;
        }
    }
    assert_eq!(query.time(), 6);
    assert!(super::projected_output(&query, &compiled).await?.iter().next().is_none());
    Ok(())
}
#[tokio::test]
async fn exact_layout_identity_ownership_and_failed_evaluation() -> Result<()> {
    let native = contract();
    let compiled = compile(SQL, &native)?;
    let mut query = super::build(&native, &compiled, &settings())?;
    let before = query.checkpoint()?;
    let bad = batch(vec![("auction", row(&[("id", Some("1"))]), 1)]);
    assert!(query.prepare(relational::inputs(&bad, compiled.relational(), 1)?).await.is_err());
    assert_eq!(query.checkpoint()?, before);
    let input = relational::inputs(&batch(vec![]), compiled.relational(), 1)?;
    let prepared = query.prepare(input.clone()).await?;
    let mut other = super::build(&native, &compiled, &settings())?;
    assert!(other.prepared_checkpoint(&prepared).is_err());
    assert!(other.commit(prepared).is_err());
    let prepared = query.prepare(input).await?;
    query.commit(prepared)?;
    assert!(query.restore_checkpoint(before.clone()).await.is_err());
    let changed = compile(&SQL.replace("AS c", "AS renamed"), &native)?;
    assert_ne!(
        program::plan(&native, &compiled)?.identity(),
        program::plan(&native, &changed)?.identity()
    );
    let mut changed_query = super::build(&native, &changed, &settings())?;
    assert!(changed_query.restore_checkpoint(before).await.is_err());
    assert_eq!(
        program::plan(&native, &compiled)?.identity(),
        program::plan(
            &native,
            &compile(&SQL.replace("a.", "x.").replace("auction a", "auction x"), &native)?
        )?
        .identity()
    );
    let mut alternate = native.clone();
    alternate.relations[0].columns[1].nullable = false;
    assert_ne!(
        program::plan(&native, &compiled)?.identity(),
        program::plan(&alternate, &compile(SQL, &alternate)?)?.identity()
    );
    let compiled = compile(
        "SELECT category, COUNT(*), SUM(price) FROM source.auction JOIN source.bid ON auction.id=bid.auction GROUP BY category",
        &{
            let mut n = native.clone();
            n.relations[0].columns[1].oid = 23;
            n.relations[0].columns[1].collation = 0;
            n
        },
    )?;
    let n = {
        let mut n = native;
        n.relations[0].columns[1].oid = 23;
        n.relations[0].columns[1].collation = 0;
        n
    };
    let grouped = runtime::Runtime::build(&n, &compiled, &settings())?;
    let prepared =
        other.prepare(relational::inputs(&batch(vec![]), changed.relational(), 1)?).await?;
    assert!(grouped.checkpoint()?.plan_identity != other.checkpoint()?.plan_identity);
    assert!(other.prepared_checkpoint(&prepared).is_ok());
    assert!(changed_query.prepared_checkpoint(&prepared).is_err());
    assert!(changed_query.commit(prepared).is_err());
    Ok(())
}
#[test]
fn projection_allowlist_native_codec_and_aliases() -> Result<()> {
    let mut native = contract();
    let mut boolean = native.relations[0].columns[2].clone();
    boolean.position = 4;
    boolean.name = "enabled".into();
    boolean.oid = 16;
    boolean.collation = 0;
    let mut uuid = boolean.clone();
    uuid.position = 5;
    uuid.name = "token".into();
    uuid.oid = 2950;
    native.relations[0].columns.extend([boolean, uuid]);
    let compiled = compile(
        "SELECT id,enabled,token,category,category AS duplicate FROM source.auction",
        &native,
    )?;
    let projection = compiled.output();
    let row = row(&[
        ("id", Some("-1")),
        ("enabled", Some("t")),
        ("token", Some("00000000-0000-0000-0000-000000000000")),
        ("category", None),
    ]);
    let row = row
        .into_iter()
        .map(|(name, value)| (crate::compiler::relational::field(0, &name), value))
        .collect();
    let value = projection.row(&row)?;
    assert_eq!(
        serde_json::to_value(&value)?,
        serde_json::json!([-1, true, "00000000-0000-0000-0000-000000000000", null, null])
    );
    assert_eq!(
        serde_json::from_value::<Vec<Option<Cell>>>(serde_json::to_value(value)?)?,
        projection.row(&row)?
    );
    for sql in [
        "SELECT * FROM source.auction",
        "SELECT id FROM source.auction LIMIT 1",
        "SELECT id FROM source.auction ORDER BY id",
        "SELECT DISTINCT ON (category) category FROM source.auction",
        "SELECT auction.id FROM source.auction a",
        "SELECT b.id FROM source.auction a",
        "SELECT COUNT(*) FROM source.auction",
        "SELECT id FROM source.auction,source.bid",
        "SELECT id FROM source.auction JOIN source.bid ON auction.id=bid.auction",
        "SELECT id FROM source.auction WHERE random()>0",
        "SELECT id FROM source.auction WHERE category<'x'",
        "SELECT id FROM source.auction WHERE id::text='1'",
    ] {
        assert!(compile(sql, &native).is_err(), "accepted {sql}");
    }
    assert!(
        compile(
            &format!(
                "SELECT {} FROM source.auction",
                std::iter::repeat_n("id", 65).collect::<Vec<_>>().join(",")
            ),
            &native
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn unavailable_functions_reject_without_pg_computation() {
    let native = contract();
    for sql in [
        "SELECT abs(category) FROM source.auction",
        "SELECT length(id) FROM source.auction",
        "SELECT now() FROM source.auction",
        "SELECT random() FROM source.auction",
        "SELECT public.abs(id) FROM source.auction",
        "SELECT abs(id) FROM source.auction WHERE abs(id)>0",
        "SELECT abs(DISTINCT id) FROM source.auction",
        "SELECT abs(id) FILTER (WHERE id>0) FROM source.auction",
        "SELECT length(category) OVER () FROM source.auction",
    ] {
        assert!(compile(sql, &native).is_err(), "accepted {sql}");
    }
}

#[test]
fn text_equality_revision_binds_each_expression_boundary() -> Result<()> {
    let native = contract();
    for sql in [
        "SELECT id FROM source.auction WHERE category='OR'",
        "SELECT id,CASE WHEN category='OR' THEN 1 ELSE 0 END AS flag FROM source.auction",
        "SELECT id,COUNT(*) FILTER(WHERE category='OR') AS n FROM source.auction GROUP BY id",
        "SELECT q.id FROM(SELECT id,category,ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) AS n FROM source.auction) q WHERE q.category='OR'",
    ] {
        let compiled =
            compile(sql, &native).map_err(|error| anyhow::anyhow!("{sql}: {error:#}"))?;

        let changed = compile(&sql.replace("'OR'", "'ID'"), &native)?;
        assert_ne!(serde_json::to_vec(&compiled)?, serde_json::to_vec(&changed)?, "{sql}");
    }
    assert!(
        compile("SELECT id FROM source.auction WHERE category IS NULL", &native)?
            .output()
            .mapping
            .is_none()
    );

    Ok(())
}

#[test]
fn postgres_only_output_functions_are_unavailable() {
    for sql in [
        "SELECT length(category) AS n FROM source.auction",
        "SELECT abs(id) AS n FROM source.auction",
        "SELECT to_char(id,'999') AS n FROM source.auction",
    ] {
        assert!(compile(sql, &contract()).is_err(), "{sql}");
    }
}

#[test]
fn named_hot_auction_and_highest_bid_queries_lower() -> Result<()> {
    let mut native = contract();
    let template = native.relations[1].columns[2].clone();
    for (name, oid) in [("date_time", 1184), ("bidder", 20), ("extra", 25)] {
        let mut column = template.clone();
        column.position = i16::try_from(native.relations[1].columns.len() + 1)?;
        column.name = name.into();
        column.oid = oid;
        column.collation = if oid == 25 { 100 } else { 0 };
        native.relations[1].columns.push(column);
    }
    for sql in [
        include_str!("../../../queries/nexmark/q5.sql"),
        include_str!("../../../queries/nexmark/q7.sql"),
    ] {
        let sql = sql.replace("nexmark.", "source.");
        let compiled = compile(&sql, &native)?;
        assert!(!compiled.relational().nodes.is_empty());
        let equivalent =
            compile(&sql.replace("b.", "\"B\".").replace("bid b", "bid AS \"B\""), &native)?;
        assert_eq!(serde_json::to_vec(&compiled)?, serde_json::to_vec(&equivalent)?);
        let changed = compile(
            &sql.replace("2 seconds", "3 seconds").replace("10 seconds", "11 seconds"),
            &native,
        )?;
        assert_ne!(serde_json::to_vec(&compiled)?, serde_json::to_vec(&changed)?);
    }
    Ok(())
}
