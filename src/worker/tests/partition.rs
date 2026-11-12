use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::Result;
use std::collections::BTreeMap;
const GROUPED: &str = "SELECT b.auction AS g,COUNT(*) AS n,COUNT(b.price) AS present,SUM(b.price) AS total,MIN(b.price) AS lo,MAX(b.price) AS hi,COUNT(*) FILTER(WHERE b.price<0) AS negative FROM source.bid b GROUP BY b.auction";
const WINDOW: &str = "SELECT b.id AS id,b.auction AS g,COUNT(*) OVER (PARTITION BY b.auction ORDER BY b.id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS n,SUM(b.price) OVER (PARTITION BY b.auction ORDER BY b.id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS total FROM source.bid b";
type Bag = Batch<relational::Key, Row>;
fn oracle(state: &BTreeMap<Row, i64>, window: bool) -> Result<Bag> {
    let mut groups: BTreeMap<Option<String>, Vec<Row>> = BTreeMap::new();
    for (row, weight) in state {
        if *weight > 0 {
            groups.entry(row["auction"].clone()).or_default().push(row.clone());
        }
    }
    let mut outputs = Vec::new();
    for (key, mut rows) in groups {
        rows.sort_by_key(|row| row["id"].as_ref().and_then(|value| value.parse::<i64>().ok()));
        for index in 0..if window { rows.len() } else { 1 } {
            let frame = if window {
                &rows[index.saturating_sub(1)..rows.len().min(index + 2)]
            } else {
                &rows[..]
            };
            let prices = frame
                .iter()
                .filter_map(|row| row["price"].as_ref().and_then(|value| value.parse::<i64>().ok()))
                .collect::<Vec<_>>();
            let mut output: Row = [
                ("0:auction".into(), key.clone()),
                ("@aggregate_0".into(), Some(frame.len().to_string())),
            ]
            .into();
            if window {
                output.insert("0:id".into(), rows[index]["id"].clone());
                output.insert(
                    "@aggregate_1".into(),
                    (!prices.is_empty()).then(|| prices.iter().sum::<i64>().to_string()),
                );
            } else {
                output.insert("@aggregate_1".into(), Some(prices.len().to_string()));
                output.insert(
                    "@aggregate_2".into(),
                    (!prices.is_empty()).then(|| prices.iter().sum::<i64>().to_string()),
                );
                output.insert("@aggregate_3".into(), prices.iter().min().map(ToString::to_string));
                output.insert("@aggregate_4".into(), prices.iter().max().map(ToString::to_string));
                output.insert(
                    "@aggregate_5".into(),
                    Some(prices.iter().filter(|value| **value < 0).count().to_string()),
                );
            }
            outputs.push(((crate::worker::program::relational::Key::new(), output), 1));
        }
    }
    Batch::from_updates(outputs)
}
async fn history(window: bool) -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 23;
    let sql = if window { WINDOW } else { GROUPED };
    let compiled = compile(sql, &native)?;
    let options = settings();
    let a = row(&[("id", Some("1")), ("auction", None), ("price", Some("-2"))]);
    let b = row(&[("id", Some("2")), ("auction", None), ("price", None)]);
    let c = row(&[("id", Some("3")), ("auction", None), ("price", Some("7"))]);
    let changed = row(&[("id", Some("1")), ("auction", Some("1")), ("price", None)]);
    let histories = [
        batch(vec![("bid", a.clone(), 1), ("bid", b.clone(), 1), ("bid", c.clone(), 1)]),
        batch(vec![("bid", a.clone(), -1), ("bid", changed.clone(), 1)]),
        batch(vec![("bid", c, -1)]),
        batch(vec![("bid", changed, -1), ("bid", a, 1)]),
        batch(vec![("bid", b, -1)]),
        batch(vec![]),
    ];
    super::History { deltas: true, ..super::History::new((&native, &compiled, &options)) }
        .run(
            histories,
            |state: &super::RowState| oracle(state, window),
            super::apply_rows,
            |index| index == 2,
        )
        .await?;

    Ok(())
}
#[tokio::test]
async fn grouped_null_filters_extrema_and_retractions() -> Result<()> {
    history(false).await
}
#[tokio::test]
async fn rows_frames_revise_neighbors_and_cold_restart() -> Result<()> {
    history(true).await
}
#[test]
fn partition_binding_rejects_unsupported_semantics() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 23;
    for sql in [
        WINDOW.replace("ORDER BY b.id ROWS", "ORDER BY b.id,b.price RANGE"),
        WINDOW.replace("ORDER BY b.id", "ORDER BY b.price"),
        WINDOW.replace("1 PRECEDING", "NULL PRECEDING"),
        WINDOW.replace("1 FOLLOWING", "2 FOLLOWING EXCLUDE UNKNOWN"),
        GROUPED.replace("COUNT(*) AS n", "COUNT(DISTINCT b.price,b.id) AS n"),
        GROUPED.replace("b.auction AS g", "b.id AS g"),
    ] {
        assert!(compile(&sql, &native).is_err(), "{sql}");
    }
    let first = compile(WINDOW, &native)?;
    let alias = compile(&WINDOW.replace("b.", "\"B\".").replace("bid b", "bid AS \"B\""), &native)?;
    assert_eq!(serde_json::to_vec(&first)?, serde_json::to_vec(&alias)?);
    Ok(())
}

#[tokio::test]
async fn accepted_frame_boundaries_match_independent_positions() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 23;
    let cases = [
        ("UNBOUNDED PRECEDING AND CURRENT ROW", None, Some(0), false),
        ("CURRENT ROW AND UNBOUNDED FOLLOWING", Some(0), None, true),
        ("2 PRECEDING AND 1 PRECEDING", Some(-2), Some(-1), false),
        ("1 FOLLOWING AND 2 FOLLOWING", Some(1), Some(2), true),
        ("2 FOLLOWING AND 1 FOLLOWING", Some(2), Some(1), false),
        ("UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", None, None, true),
    ];
    let source = [(1, None), (2, Some(-7)), (3, Some(9)), (4, None)];
    let changes = batch(
        source
            .iter()
            .map(|(id, price)| {
                (
                    "bid",
                    [
                        ("id".into(), Some(id.to_string())),
                        ("auction".into(), None),
                        ("price".into(), price.map(|price| price.to_string())),
                    ]
                    .into(),
                    1,
                )
            })
            .collect(),
    );
    for (frame, start, end, descending) in cases {
        let direction = if descending { "DESC NULLS FIRST" } else { "ASC NULLS LAST" };
        let sql = format!(
            "SELECT b.id,COUNT(*) OVER (ORDER BY b.price {direction},b.id ROWS BETWEEN {frame}) FROM source.bid b"
        );
        let compiled = compile(&sql, &native)?;
        let ir = compiled.relational();
        let mut query = relational::build(&native, &compiled, &settings())?;
        let mut order = source.to_vec();
        order.sort_by_key(|(id, price)| {
            if descending {
                (price.is_some(), -price.unwrap_or(0), *id)
            } else {
                (price.is_none(), price.unwrap_or(0), *id)
            }
        });
        let expected = Batch::from_updates(order.iter().enumerate().map(|(index, (id, _))| {
            let left =
                start.map_or(0, |offset| (i64::try_from(index).unwrap_or(0) + offset).clamp(0, 4));
            let right = end
                .map_or(4, |offset| (i64::try_from(index).unwrap_or(0) + offset + 1).clamp(0, 4));
            let output = [
                ("0:id".into(), Some(id.to_string())),
                ("@aggregate_0".into(), Some((right - left).max(0).to_string())),
            ]
            .into();
            ((crate::worker::program::relational::Key::new(), output), 1)
        }))?;
        let prepared = query.prepare(relational::inputs(&changes, ir, 1)?).await?;
        assert_eq!(prepared.output().batch, expected, "{sql}");
        query.commit(prepared)?;
    }
    Ok(())
}
#[tokio::test]
async fn window_resource_failure_preserves_root_and_time() -> Result<()> {
    let native = contract();
    let compiled = compile(&WINDOW.replace("SUM(b.price)", "COUNT(b.price)"), &native)?;
    let ir = compiled.relational();
    let mut options = settings();
    options.limits.output_bytes = 200;
    let query = relational::build(&native, &compiled, &options)?;
    let before = query.checkpoint()?;
    let value = row(&[("id", Some("1")), ("auction", None), ("price", Some("2"))]);
    let weighted = batch(vec![("bid", value, 100)]);
    assert!(query.prepare(relational::inputs(&weighted, ir, 1)?).await.is_err());
    assert_eq!(query.time(), 0);
    assert_eq!(query.checkpoint()?, before);
    let mut options = settings();
    options.limits.contributions = 3;
    let query = relational::build(&native, &compiled, &options)?;
    let before = query.checkpoint()?;
    let rows = batch(vec![
        ("bid", row(&[("id", Some("1")), ("auction", None), ("price", None)]), 1),
        ("bid", row(&[("id", Some("2")), ("auction", Some("2")), ("price", None)]), 1),
    ]);
    assert!(query.prepare(relational::inputs(&rows, ir, 1)?).await.is_err());
    assert_eq!(query.checkpoint()?, before);
    Ok(())
}

#[tokio::test]
async fn integer_numeric_statistics_preserve_exact_weighted_state() -> Result<()> {
    let native = contract();
    let compiled = super::raw_statistics_compiled(compile(
        "SELECT b.auction,AVG(b.price),SUM(b.price) FROM source.bid b GROUP BY b.auction",
        &native,
    )?);
    assert!(compiled.validate_execution().is_ok());
    let ir = compiled.relational();
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let large =
        row(&[("id", Some("1")), ("auction", None), ("price", Some("9223372036854775807"))]);
    let negative = row(&[("id", Some("2")), ("auction", None), ("price", Some("-1"))]);
    let null = row(&[("id", Some("3")), ("auction", None), ("price", None)]);
    let changes = batch(vec![
        ("bid", large.clone(), 2),
        ("bid", negative.clone(), 3),
        ("bid", null.clone(), 1),
    ]);
    let candidate = query.prepare(relational::inputs(&changes, ir, 1)?).await?;
    let total = num_bigint::BigInt::from(i64::MAX) * 2_i32 - 3_i32;
    let expected: Row = [
        ("0:auction".into(), None),
        ("@aggregate_0".into(), Some(format!("{total}/5"))),
        ("@aggregate_1".into(), Some(total.to_string())),
    ]
    .into();
    assert_eq!(
        candidate.output().batch,
        Batch::from_updates([(
            (crate::worker::program::relational::Key::new(), expected.clone()),
            1
        )])?
    );
    relational::deltas(&query, &candidate.output().batch, &ir.output).await?;
    query.commit(candidate)?;
    super::restart_relational(&mut query, &native, &compiled, &options).await?;
    let changes = batch(vec![("bid", large, -2), ("bid", negative, -3)]);
    let candidate = query.prepare(relational::inputs(&changes, ir, 2)?).await?;
    let empty: Row =
        [("0:auction".into(), None), ("@aggregate_0".into(), None), ("@aggregate_1".into(), None)]
            .into();
    assert_eq!(
        candidate.output().batch,
        Batch::from_updates([
            ((crate::worker::program::relational::Key::new(), expected), -1),
            ((crate::worker::program::relational::Key::new(), empty), 1)
        ])?
    );
    query.commit(candidate)?;
    let before = serde_json::to_vec(&query.checkpoint()?)?;
    let changes = batch(vec![("bid", null, -2)]);
    assert!(query.prepare(relational::inputs(&changes, ir, 3)?).await.is_err());
    assert_eq!(serde_json::to_vec(&query.checkpoint()?)?, before);
    Ok(())
}

#[test]
fn native_timestamp_resolution_normalizes_offsets_and_utc_cross_type_comparisons() -> Result<()> {
    let mut native = contract();
    for (index, name, oid) in [(4, "local", 1114), (5, "instant", 1184), (6, "amount", 1700)] {
        native.relations[1].columns.push(crate::source::tests::column(index, name, oid));
    }
    let sql = "SELECT b.local,b.instant,b.amount FROM source.bid b WHERE b.instant<'2000-01-01 00:00:00+00'";
    let original = compile(sql, &native)?;
    let equivalent =
        compile(&sql.replace("2000-01-01 00:00:00+00", "1999-12-31 16:00:00-08"), &native)?;
    assert_eq!(serde_json::to_vec(&original)?, serde_json::to_vec(&equivalent)?);
    assert!(original.validate_execution().is_ok());

    compile("SELECT b.local FROM source.bid b WHERE b.local=b.instant", &native)?;
    let implicit_utc = compile(&sql.replace("00:00:00+00", "00:00:00"), &native)?;
    assert_eq!(serde_json::to_vec(&original)?, serde_json::to_vec(&implicit_utc)?);
    assert!(
        compile("SELECT b.local FROM source.bid b WHERE b.local<'1900-02-29 00:00:00'", &native)
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn compiled_date_bins_retract_groups_and_preserve_nulls_across_restart() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns.push(crate::source::tests::column(4, "event_time", 1184));
    let bin = "date_bin('10 seconds',b.event_time,TIMESTAMPTZ '2000-01-01 00:00:00+00')";
    let sql = format!("SELECT {bin} AS bucket,COUNT(*) AS n FROM source.bid b GROUP BY {bin}");
    let compiled = compile(&sql, &native)?;
    let equivalent =
        compile(&sql.replace("'10 seconds'", "INTERVAL '0.000001 milliseconds'"), &native);
    assert!(equivalent.is_err()); // fractional interval has sub-microsecond precision
    let equivalent =
        compile(&sql.replace("'10 seconds'", "INTERVAL '10000 milliseconds'"), &native)?;
    assert_eq!(serde_json::to_vec(&compiled)?, serde_json::to_vec(&equivalent)?);
    let ir = compiled.relational();
    let field = ir.output.columns[0].column.name.clone();
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[
        ("id", Some("1")),
        ("auction", Some("1")),
        ("price", Some("1")),
        ("event_time", Some("1999-12-31 23:59:59.999999+00")),
    ]);
    let b =
        row(&[("id", Some("2")), ("auction", Some("1")), ("price", None), ("event_time", None)]);
    let moved = row(&[
        ("id", Some("1")),
        ("auction", Some("1")),
        ("price", Some("1")),
        ("event_time", Some("2000-01-01 00:00:10+00")),
    ]);
    let output = |key: Option<&str>, count: i64| -> Row {
        [(field.clone(), key.map(str::to_owned)), ("@aggregate_0".into(), Some(count.to_string()))]
            .into()
    };
    let changes = batch(vec![("bid", a.clone(), 1), ("bid", b.clone(), 1)]);
    let candidate = query.prepare(relational::inputs(&changes, ir, 1)?).await?;
    let before = output(Some("1999-12-31 23:59:50+00"), 1);
    let null = output(None, 1);
    assert_eq!(
        candidate.output().batch,
        Batch::from_updates([
            ((crate::worker::program::relational::Key::new(), before.clone()), 1),
            ((crate::worker::program::relational::Key::new(), null.clone()), 1)
        ])?
    );
    query.commit(candidate)?;
    super::restart_relational(&mut query, &native, &compiled, &options).await?;
    let changes = batch(vec![("bid", a, -1), ("bid", moved, 1), ("bid", b, -1)]);
    let candidate = query.prepare(relational::inputs(&changes, ir, 2)?).await?;
    assert_eq!(
        candidate.output().batch,
        Batch::from_updates([
            ((crate::worker::program::relational::Key::new(), before), -1),
            ((crate::worker::program::relational::Key::new(), null), -1),
            (
                (
                    crate::worker::program::relational::Key::new(),
                    output(Some("2000-01-01 00:00:10+00"), 1)
                ),
                1
            )
        ])?
    );
    Ok(())
}

#[tokio::test]
async fn date_bin_rejects_unqualified_semantics_and_filters_before_errors() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns.push(crate::source::tests::column(4, "event_time", 1184));
    let bin = "date_bin('10 seconds',b.event_time,TIMESTAMPTZ '2000-01-01 00:00:00+00')";
    let grouped = format!("SELECT {bin} AS bucket,COUNT(*) FROM source.bid b GROUP BY {bin}");
    for sql in [
        grouped.replacen("10 seconds", "20 seconds", 1),
        grouped.replace("10 seconds", "0 seconds"),
        grouped.replace("10 seconds", "-1 seconds"),
        grouped.replace("10 seconds", "1 month"),
        grouped.replace("10 seconds", "1 secondss"),
        grouped.replace("10 seconds", "infinity"),
        grouped.replace("10 seconds", "9223372036854775808 microseconds"),
        grouped.replace("TIMESTAMPTZ", "TIMESTAMP").replace("00:00:00+00", "00:00:00"),
        grouped.replace("TIMESTAMPTZ '2000-01-01 00:00:00+00'", "'2000-01-01 00:00:00+00'"),
        grouped.replace("2000-01-01 00:00:00+00", "infinity"),
        grouped.replace("2000-01-01 00:00:00+00", "now"),
        grouped.replace("b.event_time", "NULL"),
    ] {
        assert!(compile(&sql, &native).is_err(), "{sql}");
    }
    let sql = format!("SELECT {bin} FROM source.bid b WHERE b.id<0")
        .replace("2000-01-01 00:00:00+00", "4714-11-24 00:00:00+00 BC");
    let compiled = compile(&sql, &native)?;
    let ir = compiled.relational();
    let mut query = relational::build(&native, &compiled, &settings())?;
    let input = row(&[
        ("id", Some("1")),
        ("auction", Some("1")),
        ("price", Some("1")),
        ("event_time", Some("294276-12-31 23:59:59.999999+00")),
    ]);
    let delta = batch(vec![("bid", input, 1)]);
    let candidate = query.prepare(relational::inputs(&delta, ir, 1)?).await?;
    assert_eq!(candidate.output().batch.iter().count(), 0);
    query.commit(candidate)?;
    assert_eq!(query.time(), 1);
    Ok(())
}

#[test]
fn named_windows_lower_to_the_inline_plan() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 23;
    let expected = serde_json::to_vec(&compile(WINDOW, &native)?)?;
    let frame = "PARTITION BY b.auction ORDER BY b.id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING";
    let direct = WINDOW.replace(&format!("OVER ({frame})"), r#"OVER "W""#);
    let named = format!(r#"{direct} WINDOW "W" AS ({frame})"#);
    assert_eq!(serde_json::to_vec(&compile(&named, &native)?)?, expected);

    let inherited = direct
        .replacen(r#"OVER "W""#, "OVER (ordered ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)", 1)
        .replace(r#"OVER "W""#, "OVER framed");
    let named = format!(
        "{inherited} WINDOW base AS (PARTITION BY b.auction), ordered AS (base ORDER BY b.id), framed AS (ordered ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
    );
    assert_eq!(serde_json::to_vec(&compile(&named, &native)?)?, expected);

    for (named, inline) in [
        (
            "SELECT b.id,rank() OVER w AS r,dense_rank() OVER (w) AS d FROM source.bid b WINDOW w AS (PARTITION BY b.auction ORDER BY b.price)",
            "SELECT b.id,rank() OVER (PARTITION BY b.auction ORDER BY b.price) AS r,dense_rank() OVER (PARTITION BY b.auction ORDER BY b.price) AS d FROM source.bid b",
        ),
        (
            "SELECT b.id,lag(b.price) OVER w FROM source.bid b WINDOW w AS (ORDER BY b.id)",
            "SELECT b.id,lag(b.price) OVER (ORDER BY b.id) FROM source.bid b",
        ),
        (
            "SELECT b.id FROM source.bid b WINDOW w AS (ORDER BY b.id) ORDER BY COUNT(*) OVER w,b.id LIMIT 3",
            "SELECT b.id FROM source.bid b ORDER BY COUNT(*) OVER (ORDER BY b.id),b.id LIMIT 3",
        ),
        (
            "WITH q AS (SELECT b.id,rank() OVER w AS r FROM source.bid b WINDOW w AS (ORDER BY b.price)) SELECT q.id,q.r FROM q",
            "WITH q AS (SELECT b.id,rank() OVER (ORDER BY b.price) AS r FROM source.bid b) SELECT q.id,q.r FROM q",
        ),
    ] {
        assert_eq!(
            serde_json::to_vec(&compile(named, &native)?)?,
            serde_json::to_vec(&compile(inline, &native)?)?,
            "{named}"
        );
    }
    Ok(())
}

#[test]
fn named_windows_reject_invalid_references() -> Result<()> {
    let native = contract();
    for (sql, message) in [
        (
            "SELECT id FROM source.bid WINDOW unused AS (ORDER BY missing)",
            "unused WINDOW definition",
        ),
        (
            "SELECT 1 FROM source.bid WINDOW unused AS (ORDER BY sum(price))",
            "unused WINDOW definition",
        ),
        ("SELECT count(*) OVER missing FROM source.bid", "does not exist"),
        (
            "SELECT count(*) OVER w FROM source.bid WINDOW w AS (later), later AS ()",
            "does not exist",
        ),
        ("SELECT count(*) OVER w FROM source.bid WINDOW w AS (w)", "does not exist"),
        ("SELECT count(*) OVER w FROM source.bid WINDOW w AS (), w AS ()", "duplicate WINDOW"),
        (
            "SELECT count(*) OVER (w PARTITION BY auction) FROM source.bid WINDOW w AS ()",
            "override a window's PARTITION BY",
        ),
        (
            "SELECT count(*) OVER w FROM source.bid WINDOW base AS (ORDER BY id), w AS (base ORDER BY price)",
            "override a window's ORDER BY",
        ),
        (
            "SELECT count(*) OVER (w) FROM source.bid WINDOW w AS (ORDER BY id ROWS UNBOUNDED PRECEDING)",
            "cannot copy a window",
        ),
        (
            "SELECT count(*) OVER w FROM source.bid WINDOW base AS (ORDER BY id ROWS UNBOUNDED PRECEDING), w AS (base)",
            "cannot copy a window",
        ),
        (
            "SELECT count(*) OVER w FROM (SELECT id, rank() OVER w FROM source.bid WINDOW w AS (ORDER BY id)) q",
            "does not exist",
        ),
    ] {
        let Err(error) = compile(sql, &native) else {
            anyhow::bail!("unexpectedly compiled {sql}");
        };
        assert!(format!("{error:#}").contains(message), "{sql}: {error:#}");
    }
    Ok(())
}

#[tokio::test]
async fn boolean_aggregates_preserve_filter_and_empty_input_semantics() -> Result<()> {
    let native = contract();
    let options = settings();
    for (sql, expected) in [
        (
            "SELECT bool_and(v),bool_or(v),every(v) FROM (VALUES (true),(false),(NULL)) t(v)",
            [Some("f"), Some("t"), Some("f")],
        ),
        (
            "SELECT bool_and(v),bool_or(v),every(v) FROM (VALUES (NULL::boolean),(NULL)) t(v)",
            [None, None, None],
        ),
        (
            "SELECT bool_and(v),bool_or(v),every(v) FROM (VALUES (true),(false)) t(v) WHERE false",
            [None, None, None],
        ),
        (
            r#"SELECT bool_and(NULL),bool_or('yes' COLLATE "C"),pg_catalog.every('off')"#,
            [None, Some("t"), Some("f")],
        ),
        (
            "SELECT bool_and(v) FILTER (WHERE v),bool_or(v) FILTER (WHERE NULL),every(v) FILTER (WHERE NOT v) FROM (VALUES (true),(false),(NULL)) t(v)",
            [Some("t"), None, Some("f")],
        ),
        (
            "SELECT bool_and(x > 0),bool_or(x > 0),every(x > 0) FROM (VALUES (1),(2)) t(x) HAVING every(x > 0)",
            [Some("t"), Some("t"), Some("t")],
        ),
    ] {
        let compiled = compile(sql, &native)?;
        let columns = &compiled.output().columns;
        assert_eq!(
            columns.iter().map(|value| value.label.as_str()).collect::<Vec<_>>(),
            ["bool_and", "bool_or", "every"]
        );
        assert!(columns.iter().all(|value| value.column.oid == 16 && value.column.nullable));
        let worker = relational::build(&native, &compiled, &options)?;
        let prepared =
            worker.prepare(relational::inputs(&batch(vec![]), compiled.relational(), 1)?).await?;
        let values = columns
            .iter()
            .zip(expected)
            .map(|(column, value)| (column.column.name.clone(), value.map(str::to_owned)))
            .collect::<Row>();
        let expected = Batch::from_updates([((relational::Key::new(), values), 1)])?;
        assert_eq!(prepared.output().batch, expected, "{sql}");
    }
    Ok(())
}

#[test]
fn boolean_aggregates_reject_non_boolean_overloads_and_bad_modifiers() {
    let native = contract();
    for sql in [
        "SELECT bool_and(1)",
        "SELECT bool_or('true'::text)",
        "SELECT every('not a Boolean') FILTER (WHERE false)",
        "SELECT every((1 / 0) > 0) FILTER (WHERE false)",
        "SELECT bool_or(*)",
        "SELECT bool_and(true,false)",
        "SELECT bool_or(DISTINCT true) OVER()",
        "SELECT public.every(true)",
    ] {
        assert!(compile(sql, &native).is_err(), "{sql}");
    }
}

#[test]
fn group_by_output_references_bind_like_explicit_expressions() -> Result<()> {
    let native = contract();
    for (prefix, explicit) in [
        ("SELECT auction AS g,COUNT(*) AS n FROM source.bid", "auction"),
        ("SELECT b.auction+1 AS g,COUNT(*) AS n FROM source.bid b", "b.auction+1"),
        (
            "SELECT a.id AS g,COUNT(*) AS n FROM source.auction a JOIN source.bid b ON a.id=b.auction",
            "a.id",
        ),
        (
            "SELECT q.auction AS g,COUNT(*) AS n FROM (SELECT auction FROM source.bid) q",
            "q.auction",
        ),
        ("SELECT 40+2 AS g,COUNT(*) AS n", "40+2"),
    ] {
        let alias = compile(&format!("{prefix} GROUP BY g"), &native)?;
        let ordinal = compile(&format!("{prefix} GROUP BY 1"), &native)?;
        assert_eq!(serde_json::to_vec(&alias)?, serde_json::to_vec(&ordinal)?);
        let expression = compile(&format!("{prefix} GROUP BY {explicit}"), &native)?;
        assert_eq!(serde_json::to_vec(&alias)?, serde_json::to_vec(&expression)?);
    }
    compile("SELECT b.* FROM source.bid b GROUP BY 1,2,3 ORDER BY 2", &native)?;
    compile(
        "SELECT a.id+1 AS g,COUNT(*) AS n FROM source.auction a
         GROUP BY g HAVING a.id+1 > 1 ORDER BY a.id+1",
        &native,
    )?;
    compile(
        "SELECT q.n FROM (
            SELECT b.auction AS g,auction AS g,b.auction::integer AS g,COUNT(*) AS n FROM source.bid b GROUP BY g
        ) q",
        &native,
    )?;
    compile(
        "SELECT q.n FROM (
            SELECT 1.00 AS g,'01.00'::numeric AS g,COUNT(*) AS n FROM source.bid GROUP BY g
        ) q",
        &native,
    )?;
    compile(
        "SELECT q.n FROM (
            SELECT a.id AS g,id AS g,COUNT(*) AS n
            FROM source.auction a JOIN source.bid b USING (id) GROUP BY g
        ) q",
        &native,
    )?;
    let mut varchar = native;
    let category = &mut varchar.relations[0].columns[1];
    category.oid = 1043;
    category.modifier = 11;
    category.collation = 950;
    compile(
        "SELECT q.n FROM (
            SELECT category AS g,category::varchar(7) AS g,COUNT(*) AS n
            FROM source.auction GROUP BY g
        ) q",
        &varchar,
    )?;
    Ok(())
}

#[test]
fn group_by_respects_input_precedence_and_visible_target_bounds() -> Result<()> {
    let native = contract();
    let prefix = "SELECT auction AS g,COUNT(*) AS auction FROM source.bid b";
    let unqualified = compile(&format!("{prefix} GROUP BY auction"), &native)?;
    let qualified = compile(&format!("{prefix} GROUP BY b.auction"), &native)?;
    assert_eq!(serde_json::to_vec(&unqualified)?, serde_json::to_vec(&qualified)?);
    for sql in [
        "SELECT price AS auction,COUNT(*) FROM source.bid GROUP BY auction",
        "SELECT a.id AS id,COUNT(*) FROM source.auction a JOIN source.bid b ON a.id=b.auction GROUP BY id",
        "SELECT q.auction AS k,COUNT(*) FROM (SELECT auction,id AS k,price AS k FROM source.bid) q GROUP BY k",
        "SELECT auction AS g,price AS g,COUNT(*) FROM source.bid GROUP BY g",
        "SELECT 1.0 AS g,1.00 AS g,COUNT(*) FROM source.bid GROUP BY g",
        "SELECT auction AS g,COUNT(*) FROM source.bid GROUP BY g+1",
        "SELECT auction AS g,COUNT(*) FROM source.bid b GROUP BY b.g",
        "SELECT COUNT(*) AS n FROM source.bid GROUP BY n",
        "SELECT COUNT(*) FROM source.bid GROUP BY 1",
        "SELECT ROW_NUMBER() OVER(ORDER BY id) AS w FROM source.bid GROUP BY w",
        "SELECT ROW_NUMBER() OVER(ORDER BY id) FROM source.bid GROUP BY 1",
        "SELECT COUNT(*) AS n FROM source.bid GROUP BY 2 ORDER BY price",
        "SELECT COUNT(*) AS n FROM source.bid GROUP BY abs ORDER BY abs(price)",
        "SELECT price FROM source.bid GROUP BY 0",
        "SELECT price FROM source.bid GROUP BY -1",
        "SELECT COUNT(*) FROM source.bid GROUP BY NULL",
        "SELECT COUNT(*) FROM source.bid GROUP BY 'price'",
        "SELECT COUNT(*) FROM source.bid GROUP BY 1.0",
    ] {
        assert!(compile(sql, &native).is_err(), "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn different_frames_keep_one_peer_order_for_the_same_sort_keys() -> Result<()> {
    let native = contract();
    let compiled = compile(
        "SELECT id,row_number() OVER (ORDER BY id DESC),
           first_value(id) OVER (ORDER BY auction RANGE CURRENT ROW),
           last_value(id) OVER (ORDER BY auction RANGE UNBOUNDED PRECEDING)
         FROM source.bid",
        &native,
    )?;
    let options = settings();
    let rows = [("1", "2"), ("2", "1")];
    let inputs = rows
        .map(|(id, _)| ("bid", row(&[("id", Some(id)), ("auction", None), ("price", None)]), 1));
    let expected = Batch::from_updates(rows.map(|(id, rank)| {
        let output = row(&[
            ("0:id", Some(id)),
            ("@aggregate_0", Some(rank)),
            ("@aggregate_1", Some("1")),
            ("@aggregate_2", Some("2")),
        ]);
        ((relational::Key::new(), output), 1)
    }))?;
    super::check_row_history(
        (&native, &compiled, &options),
        [batch(inputs.into())],
        |_| Ok(expected.clone()),
        |_| false,
    )
    .await?;
    Ok(())
}

#[test]
fn multiple_windows_share_specs_and_keep_hidden_ordering() -> Result<()> {
    use crate::compiler::relational::Node;
    let compiled = compile(
        "SELECT id,rank() OVER w, dense_rank() OVER w FROM source.bid
         WINDOW w AS (PARTITION BY auction ORDER BY price)
         ORDER BY row_number() OVER (ORDER BY id DESC) LIMIT 3",
        &contract(),
    )?;
    let sizes = compiled
        .relational
        .nodes
        .iter()
        .filter_map(|node| match node {
            Node::Partition { spec, .. } => Some(spec.aggregates.len()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(sizes, [2, 1]);
    assert_eq!(compiled.relational.output.columns.len(), 3);
    Ok(())
}

#[tokio::test]
async fn unread_prefix_window_payload_is_pruned_but_its_order_is_retained() -> Result<()> {
    use crate::compiler::relational::Node;
    let native = contract();
    let compiled = compile(
        "SELECT id,n FROM (
           SELECT id,count(*) OVER () AS n,count(1/(id-id)) OVER (ORDER BY id) AS unused
           FROM source.bid
         ) q",
        &native,
    )?;
    let mut prefix_nodes = 0;
    for node in &compiled.relational.nodes {
        if let Node::PrefixInput { spec, .. }
        | Node::PrefixStatistics { spec, .. }
        | Node::PrefixWindow { spec, .. } = node
        {
            assert_eq!(spec.aggregates.len(), 1);
            assert!(spec.aggregates[0].expression.is_none());
            assert!(matches!(&spec.mode, crate::compiler::partition::Mode::Peers { order, .. }
                if order[0].column.name == "0:id"));
            prefix_nodes += 1;
        }
    }
    assert_eq!(prefix_nodes, 3);
    let input = row(&[("id", Some("1")), ("auction", None), ("price", None)]);
    let output = row(&[("0:id", Some("1")), ("@aggregate_0", Some("1"))]);
    let expected = Batch::from_updates([((relational::Key::new(), output), 1)])?;
    super::check_row_history(
        (&native, &compiled, &settings()),
        [batch(vec![("bid", input, 1)])],
        |_| Ok(expected.clone()),
        |_| false,
    )
    .await?;
    Ok(())
}

#[test]
fn outer_filter_moves_only_on_partition_keys_shared_by_all_windows() -> Result<()> {
    use crate::compiler::relational::Node;
    for (first, crosses_both) in [("PARTITION BY auction", true), ("PARTITION BY price", false)] {
        let query = format!(
            "SELECT n,m FROM (
               SELECT auction,count(*) OVER ({first}) AS n,
               count(*) OVER (PARTITION BY auction ORDER BY id ROWS CURRENT ROW) AS m
               FROM source.bid
             ) q WHERE auction=1"
        );
        let compiled = compile(&query, &contract())?;
        let nodes = &compiled.relational.nodes;
        let filter = nodes
            .iter()
            .position(|node| matches!(node, Node::Filter { .. }))
            .ok_or_else(|| anyhow::anyhow!("missing filter"))?;
        for (index, _) in
            nodes.iter().enumerate().filter(|(_, node)| matches!(node, Node::Partition { .. }))
        {
            assert_eq!(filter < index, crosses_both, "{query}");
        }
    }
    Ok(())
}
