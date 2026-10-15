use super::super::{
    program::{self, projected},
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
fn oracle(rows: &BTreeMap<Row, i64>) -> Result<Batch<(), projected::Output>> {
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
    let mut query = projected::build(&native, &compiled, options.clone())?;
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
        let input = projected::inputs(
            &changes,
            compiled.projection().ok_or_else(|| anyhow::anyhow!("missing projection"))?,
            query.time() + 1,
        )?;
        let prepared = query.prepare(input).await?;
        assert_eq!(&prepared.output().batch, &expected);
        let candidate = query.prepared_checkpoint(&prepared)?;
        assert_eq!(candidate.time, query.time() + 1);
        query.commit(prepared)?;
        assert_eq!(query.snapshot().output.materialize().await?, after);
        if index == 2 {
            let checkpoint = query.checkpoint()?;
            query = projected::build(&native, &compile(SQL, &native)?, options.clone())?;
            query.restore_checkpoint(checkpoint).await?;
        }
    }
    assert_eq!(query.time(), 6);
    assert!(query.snapshot().output.materialize().await?.iter().next().is_none());
    Ok(())
}
#[tokio::test]
async fn exact_layout_identity_ownership_and_failed_evaluation() -> Result<()> {
    let native = contract();
    let compiled = compile(SQL, &native)?;
    let mut query = projected::build(&native, &compiled, settings())?;
    let before = query.checkpoint()?;
    let bad = batch(vec![("auction", row(&[("id", Some("1"))]), 1)]);
    assert!(
        query
            .prepare(projected::inputs(
                &bad,
                compiled.projection().ok_or_else(|| anyhow::anyhow!("missing projection"))?,
                1
            )?)
            .await
            .is_err()
    );
    assert_eq!(query.checkpoint()?, before);
    let input = crate::engine::dataflow::TimedBatch { time: 1, batch: Batch::from_updates([])? };
    let prepared = query.prepare(input.clone()).await?;
    let mut other = projected::build(&native, &compiled, settings())?;
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
    let mut changed_query = projected::build(&native, &changed, settings())?;
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
    let mut grouped = runtime::Runtime::build(&n, &compiled, settings())?;
    let prepared = other
        .prepare(crate::engine::dataflow::TimedBatch { time: 1, batch: Batch::from_updates([])? })
        .await?;
    let work = runtime::Prepared::Projection(prepared);
    assert!(grouped.prepared_checkpoint(&work).is_err());
    assert!(grouped.commit(work).is_err());
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
    let projection = compiled.projection().ok_or_else(|| anyhow::anyhow!("missing projection"))?;
    let row = row(&[
        ("id", Some("-1")),
        ("enabled", Some("t")),
        ("token", Some("00000000-0000-0000-0000-000000000000")),
        ("category", None),
    ]);
    let value = projection.row(&row)?;
    assert_eq!(
        serde_json::to_value(&value)?,
        serde_json::json!([-1, true, "00000000-0000-0000-0000-000000000000", null, null])
    );
    assert_eq!(
        serde_json::from_value::<projected::Output>(serde_json::to_value(value)?)?,
        projection.row(&row)?
    );
    for sql in [
        "SELECT * FROM source.auction",
        "SELECT id+1 FROM source.auction",
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
fn terminal_map_requires_binding_and_rejects_unsafe_positions() -> Result<()> {
    let native = contract();
    let mapped = compile(
        "SELECT pg_catalog.abs(id) AS positive, length(category) AS characters FROM source.auction",
        &native,
    )?;
    assert!(mapped.projection().is_some());
    assert!(program::plan(&native, &mapped).is_err());
    assert!(mapped.sink("sink").validate().is_err());
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
    Ok(())
}

#[test]
fn text_equality_revision_binds_each_expression_boundary() -> Result<()> {
    let native = contract();
    let suffix = "pg-deterministic-text-equality-v1";
    for sql in [
        "SELECT id FROM source.auction WHERE category='OR'",
        "SELECT id,CASE WHEN category='OR' THEN 1 ELSE 0 END AS flag FROM source.auction",
        "SELECT id,COUNT(*) FILTER(WHERE category='OR') AS n FROM source.auction GROUP BY id",
        "SELECT q.id FROM(SELECT id,category,ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) AS n FROM source.auction) q WHERE q.category='OR'",
    ] {
        let compiled =
            compile(sql, &native).map_err(|error| anyhow::anyhow!("{sql}: {error:#}"))?;
        assert!(compiled.revision.as_deref().is_some_and(|value| value.contains(suffix)), "{sql}");
        let changed = compile(&sql.replace("'OR'", "'ID'"), &native)?;
        assert_ne!(serde_json::to_vec(&compiled)?, serde_json::to_vec(&changed)?, "{sql}");
    }
    let plain = compile("SELECT id FROM source.auction WHERE category IS NULL", &native)?;
    assert!(plain.revision.as_deref().is_some_and(|value| !value.contains(suffix)));
    Ok(())
}

#[test]
fn timestamp_terminal_formats_normalize_context_and_reject_unsafe_shapes() -> Result<()> {
    let mut native = contract();
    let column = &mut native.relations[0].columns[2];
    column.oid = 1184;
    column.collation = 0;
    let sql = "SELECT to_char(other AT TIME ZONE 'UTC','HH24:MI') AS minute FROM source.auction";
    let compiled = compile(sql, &native)?;
    assert!(
        compiled
            .revision
            .as_deref()
            .is_some_and(|value| value.contains("pg-terminal-utc-calendar-format-v1"))
    );
    assert_eq!(
        serde_json::to_vec(&compiled)?,
        serde_json::to_vec(&compile(
            &sql.replace("other AT TIME ZONE 'UTC'", "pg_catalog.timezone('UTC',other)"),
            &native
        )?)?
    );
    for invalid in [
        sql.replace("'UTC'", "'America/New_York'"),
        sql.replace("'HH24:MI'", "'Month'"),
        sql.replace("other AT TIME ZONE 'UTC'", "other"),
        sql.replace("'HH24:MI'", "category"),
    ] {
        assert!(compile(&invalid, &native).is_err(), "{invalid}");
    }
    let derived = "SELECT q.id,to_char(q.latest AT TIME ZONE 'UTC','HH24:MI') AS minute FROM(SELECT id,MAX(other) AS latest FROM source.auction GROUP BY id) q";
    assert!(compile(derived, &native)?.relational().is_some());
    let consumed = format!("SELECT z.minute FROM({derived}) z");
    assert!(compile(&consumed, &native).is_err());
    native.relations[0].columns[2].oid = 1114;
    assert!(compile(&sql.replace("other AT TIME ZONE 'UTC'", "other"), &native).is_ok());
    assert!(compile(sql, &native).is_err());
    Ok(())
}

#[test]
fn named_calendar_queries_lower_text_groups_and_terminal_max() -> Result<()> {
    let mut native = contract();
    let template = native.relations[1].columns[2].clone();
    for (name, oid) in [("date_time", 1184), ("channel", 25), ("bidder", 20), ("extra", 25)] {
        let mut column = template.clone();
        column.position = i16::try_from(native.relations[1].columns.len() + 1)?;
        column.name = name.into();
        column.oid = oid;
        column.collation = if oid == 25 { 100 } else { 0 };
        native.relations[1].columns.push(column);
    }
    for sql in [
        include_str!("../../../queries/nexmark/q10.sql"),
        include_str!("../../../queries/nexmark/q16.sql"),
    ] {
        let compiled = compile(&sql.replace("nexmark.", "source."), &native)?;
        assert!(
            compiled
                .revision
                .as_deref()
                .is_some_and(|value| value.contains("pg-terminal-utc-calendar-format-v1"))
        );
        if compiled.relational().is_some() {
            assert!(
                compiled
                    .revision
                    .as_deref()
                    .is_some_and(|value| value.contains("pg-deterministic-text-partition-v1"))
            );
        }
    }
    Ok(())
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
        assert!(compiled.relational().is_some());
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
