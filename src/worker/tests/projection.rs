use super::super::program::{self, relational};
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
    let grouped = relational::build(&n, &compiled, &settings())?;
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
    compile("SELECT id FROM source.auction WHERE id::text='1'", &native)?;
    for sql in [
        "SELECT missing.* FROM source.auction",
        "SELECT id FROM source.auction LIMIT -1",
        "SELECT id FROM source.auction ORDER BY category",
        "SELECT DISTINCT ON (category) category FROM source.auction",
        "SELECT auction.id FROM source.auction a",
        "SELECT b.id FROM source.auction a",
        "SELECT id,COUNT(*) FROM source.auction",
        "SELECT id FROM source.auction,source.bid",
        "SELECT id FROM source.auction JOIN source.bid ON auction.id=bid.auction",
        "SELECT id FROM source.auction WHERE random()>0",
        "SELECT id FROM source.auction WHERE category<'x'",
        "SELECT id FROM source.auction WHERE (id + 0.0)::text='1'",
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
        "SELECT round(id) FROM source.auction WHERE abs(id)>0",
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
    let compiled = compile("SELECT id FROM source.auction WHERE category IS NULL", &native)?;
    assert!(
        compiled
            .relational()
            .nodes
            .iter()
            .any(|node| matches!(node, crate::compiler::relational::Node::Filter { .. }))
    );

    Ok(())
}

#[test]
fn postgres_only_output_functions_are_unavailable() {
    for sql in [
        "SELECT lower(category) AS n FROM source.auction",
        "SELECT round(id) AS n FROM source.auction",
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

#[test]
fn calendar_extract_rejects_named_zone_and_date_part() -> Result<()> {
    let mut native = contract();
    let column = &mut native.relations[1].columns[2];
    column.name = "date_time".into();
    column.oid = 1184;
    let fields = [
        "year", "month", "day", "minute", "second", "epoch", "week", "isoyear", "isodow", "dow",
        "doy", "quarter",
    ];
    let calendar = fields
        .iter()
        .map(|field| format!("extract({field} FROM date_time AT TIME ZONE 'UTC') AS {field}"))
        .collect::<Vec<_>>()
        .join(",");
    for query in [
        format!("SELECT {calendar} FROM source.bid"),
        "SELECT date_part('year',date_time AT TIME ZONE 'UTC') FROM source.bid".into(),
        "SELECT extract(year FROM date_time AT TIME ZONE 'America/Los_Angeles') FROM source.bid"
            .into(),
    ]
    .into_iter()
    .chain(fields.iter().map(|field| {
        format!("SELECT extract({field} FROM date_time AT TIME ZONE 'UTC') FROM source.bid")
    })) {
        let error = match compile(&query, &native) {
            Ok(_) => anyhow::bail!("calendar extraction must remain outside the engine allowlist"),
            Err(error) => format!("{error:#}"),
        };
        assert!(
            error.contains("not available in-engine") || error.contains("unsupported"),
            "{query}: {error}"
        );
    }
    Ok(())
}

#[test]
fn shared_temporal_target_binding_keeps_nested_and_on_allowlists() -> Result<()> {
    use crate::compiler::relational::Node;
    let mut native = contract();
    native.relations[1].columns[2].name = "moment".into();
    native.relations[1].columns[2].oid = 1184;
    compile(
        "SELECT price+1::bigint AS value,COUNT(*) FROM source.bid GROUP BY price+1::bigint",
        &contract(),
    )?;
    let bucket = "date_bin(interval '1 hour',moment,timestamptz '2000-01-01 00:00:00+00')";
    let source = row(&[("0:moment", Some("2000-01-01 12:34:56+00"))]);
    for (expression, expected) in [
        (bucket.to_owned(), "2000-01-01 12:00:00+00"),
        ("moment+interval '1 hour'".to_owned(), "2000-01-01 13:34:56+00"),
    ] {
        let compiled = compile(&format!("SELECT {expression} AS v FROM source.bid"), &native)?;
        assert_eq!(compiled.output().columns[0].column.oid, 1184);
        let values = compiled
            .relational()
            .nodes
            .iter()
            .filter_map(|node| match node {
                Node::Map { computed, .. } => Some(computed),
                _ => None,
            })
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].expression.evaluate(&source)?.as_deref(), Some(expected));
        compile(
            &format!(
                "SELECT d.v FROM(SELECT {expression} AS v FROM source.bid) d WHERE d.v IS NOT NULL"
            ),
            &native,
        )?;
    }
    for sql in [
        format!("SELECT COALESCE({bucket},moment) FROM source.bid"),
        format!(
            "SELECT b.id FROM source.bid b JOIN source.bid c ON {}=c.moment",
            bucket.replace("moment", "b.moment")
        ),
        "SELECT date_part('hour',moment) FROM source.bid".into(),
        format!("SELECT {bucket} OVER() FROM source.bid"),
        "SELECT bid.* FROM source.bid b".into(),
        "SELECT id,COUNT(*) FROM source.bid".into(),
    ] {
        assert!(compile(&sql, &native).is_err(), "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn concatenation_budget_failure_preserves_the_committed_checkpoint() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT repeat(category, 2097153) || repeat(category, 2097153) AS joined FROM source.auction",
        &native,
    )?;
    let mut query = super::build(&native, &compiled, &settings())?;
    let ir = compiled.relational();
    let initial = row(&[("id", Some("1")), ("category", Some(""))]);
    let first = batch(vec![("auction", initial.clone(), 1)]);
    let prepared = query.prepare(relational::inputs(&first, ir, 1)?).await?;
    query.commit(prepared)?;
    let before = query.checkpoint()?;
    let poison = row(&[("id", Some("1")), ("category", Some("🙂"))]);
    let update = batch(vec![("auction", initial, -1), ("auction", poison, 1)]);
    let Err(error) = query.prepare(relational::inputs(&update, ir, 2)?).await else {
        anyhow::bail!("accepted a concatenation beyond the output budget");
    };
    assert!(error.is::<crate::engine::execution::EvaluationFailed>(), "{error:#}");
    assert!(format!("{error:#}").contains("concatenation output exceeds"), "{error:#}");
    assert_eq!(query.checkpoint()?, before);
    Ok(())
}

#[test]
fn single_table_star_uses_visible_column_order_and_metadata() -> Result<()> {
    let mut native = contract();
    let columns = &mut native.relations[0].columns;
    columns[1].position = 4;
    columns[1].name = "Label".into();
    columns[1].oid = 1043;
    columns[1].modifier = 11;
    columns[1].collation = 950;
    columns[2].position = 8;
    columns[2].name = "amount".into();
    columns[2].oid = 1700;
    columns[2].modifier = (9 << 16) + 3 + 4;
    columns[2].collation = 0;
    let expected = [
        ("id", 23, None, 0, false),
        ("Label", 1043, Some(11), 950, true),
        ("amount", 1700, Some((9 << 16) + 3 + 4), 0, true),
    ];
    for sql in [
        "SELECT * FROM source.auction",
        "SELECT auction.* FROM source.auction",
        "SELECT source.auction.* FROM source.auction",
        r#"SELECT "Alias".* FROM source.auction AS "Alias""#,
        r#"SELECT q.id,q."Label",q.amount FROM (SELECT * FROM source.auction) q"#,
        r#"WITH q AS (SELECT * FROM source.auction) SELECT q.id,q."Label",q.amount FROM q"#,
    ] {
        let result = compile(sql, &native)?;
        assert_eq!(projection_metadata(&result), expected, "{sql}");
    }
    let mixed = compile(
        "SELECT 0 AS first,a.*,id+1 AS last FROM source.auction a ORDER BY 4,2 LIMIT 1",
        &native,
    )?;
    let metadata = projection_metadata(&mixed);
    assert_eq!(
        metadata.iter().map(|column| column.0).collect::<Vec<_>>(),
        ["first", "id", "Label", "amount", "last"]
    );
    assert_eq!(metadata[1..4], expected);
    for sql in [
        "SELECT a.* FROM source.auction AS b",
        "SELECT source.auction.* FROM source.auction AS a",
        "SELECT a.* FROM source.auction a JOIN source.bid b ON b.auction=a.id",
        "SELECT * FROM (SELECT id FROM source.auction) q",
    ] {
        assert!(compile(sql, &native).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn single_table_star_counts_expanded_columns_against_projection_budget() -> Result<()> {
    let mut native = contract();
    let columns = &mut native.relations[0].columns;
    let template = columns[2].clone();
    for position in 4..=64 {
        columns.push(crate::source::Column {
            position,
            name: format!("c{position}"),
            ..template.clone()
        });
    }
    let compiled = compile("SELECT * FROM source.auction", &native)?;
    assert_eq!(compiled.output().columns.len(), 64);
    assert!(compile("SELECT 1 AS extra,* FROM source.auction", &native).is_err());
    Ok(())
}

type ProjectionMetadata<'a> = (&'a str, u32, Option<i32>, u32, bool);

fn projection_metadata(compiled: &crate::compiler::Compiled) -> Vec<ProjectionMetadata<'_>> {
    compiled
        .output()
        .columns
        .iter()
        .map(|output| {
            let column = &output.column;
            (output.label.as_str(), column.oid, column.modifier, column.collation, column.nullable)
        })
        .collect()
}
