use super::{batch, contract, row, settings};
use crate::{
    compiler::{compile, relational::Node},
    engine::{Batch, execution::Limits},
    worker::program::relational,
};
use anyhow::{Context, Result};
fn native() -> crate::source::Contract {
    let mut native = contract();
    native.relations[1].columns.push(crate::source::Column {
        position: 4,
        name: "event_time".into(),
        oid: 1184,
        modifier: -1,
        nullable: true,
        primary: false,
        collation: 0,
    });
    native
}
const BIN: &str = "date_bin('2 seconds',b.event_time,TIMESTAMPTZ '2000-01-01 00:00:00+00') - w.n * INTERVAL '2 seconds'";
fn sql() -> String {
    format!(
        "SELECT {BIN} AS bucket,COUNT(*) AS n,SUM(b.price) AS total FROM source.bid b CROSS JOIN generate_series(0,4) w(n) GROUP BY {BIN}"
    )
}
#[tokio::test]
async fn hopping_preserves_weighted_memberships_nulls_and_restart() -> Result<()> {
    let native = native();
    let compiled = compile(&sql(), &native)?;
    let equivalent = compile(
        &sql()
            .replace("w.n", "\"W\".\"N\"")
            .replace("w(n)", "AS \"W\"(\"N\")")
            .replace("generate_series", "pg_catalog.generate_series")
            .replace("INTERVAL '2 seconds'", "INTERVAL '2000 milliseconds'"),
        &native,
    )?;
    assert_eq!(serde_json::to_vec(&compiled)?, serde_json::to_vec(&equivalent)?);
    let ir = compiled.relational().context("missing expansion IR")?;
    let computed = ir
        .nodes
        .iter()
        .find_map(|node| match node {
            Node::Map { computed, .. } => computed.first().map(|value| value.column.name.clone()),
            _ => None,
        })
        .context("missing scalar")?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let input = row(&[
        ("id", Some("1")),
        ("auction", None),
        ("price", Some("7")),
        ("event_time", Some("2000-01-01 00:00:00+00")),
    ]);
    let null = row(&[("id", Some("2")), ("auction", None), ("price", None), ("event_time", None)]);
    let output = |bucket: Option<&str>, count: i64, sum: Option<&str>| {
        [
            (computed.clone(), bucket.map(str::to_owned)),
            ("@aggregate_0".into(), Some(count.to_string())),
            ("@aggregate_1".into(), sum.map(str::to_owned)),
        ]
        .into()
    };
    let buckets = [
        "2000-01-01 00:00:00+00",
        "1999-12-31 23:59:58+00",
        "1999-12-31 23:59:56+00",
        "1999-12-31 23:59:54+00",
        "1999-12-31 23:59:52+00",
    ];
    let initial = batch(vec![("bid", input.clone(), 2), ("bid", null.clone(), 3)]);
    let prepared = query.prepare(relational::inputs(&initial, ir, 1)?).await?;
    assert_eq!(
        prepared.output().batch,
        Batch::from_updates(
            buckets
                .iter()
                .map(|bucket| ((Vec::new(), output(Some(bucket), 2, Some("14"))), 1))
                .chain([((Vec::new(), output(None, 15, None)), 1)])
        )?
    );
    query.commit(prepared)?;
    let checkpoint = query.checkpoint()?;
    query = relational::build(&native, &compiled, &options)?;
    query.restore_checkpoint(checkpoint).await?;
    let mut moved = input.clone();
    moved.insert("event_time".into(), Some("2000-01-01 00:00:02+00".into()));
    let delta = batch(vec![("bid", input, -2), ("bid", moved, 2), ("bid", null, -3)]);
    let prepared = query.prepare(relational::inputs(&delta, ir, 2)?).await?;
    assert_eq!(
        prepared.output().batch,
        Batch::from_updates([
            ((Vec::new(), output(Some(buckets[4]), 2, Some("14"))), -1),
            ((Vec::new(), output(Some("2000-01-01 00:00:02+00"), 2, Some("14"))), 1),
            ((Vec::new(), output(None, 15, None)), -1)
        ])?
    );
    query.commit(prepared)?;
    assert_eq!(query.time(), 2);
    Ok(())
}
#[test]
fn series_binding_rejects_ambiguous_and_unqualified_semantics() -> Result<()> {
    let mut native = native();
    for sql in [
        sql().replace("0,4", "0,1024"),
        sql().replace("0,4", "0,4,1"),
        sql().replace("0,4", "0.0,4"),
        sql().replace("generate_series", "other.generate_series"),
        sql().replace("CROSS JOIN", "LEFT JOIN").replace(" GROUP BY", " ON true GROUP BY"),
        sql().replace("w(n)", "b(n)"),
        sql().replace("w(n)", "w(n,x)"),
        sql().replace("w(n)", "WITH ORDINALITY w(n)"),
        sql().replace("CROSS JOIN", "CROSS JOIN LATERAL"),
        sql().replace("w.n *", "b.id *"),
        sql().replace("INTERVAL '2 seconds'", "INTERVAL '1 day'"),
        sql().replace("INTERVAL '2 seconds'", "INTERVAL '9007199254740992 microseconds'"),
        sql().replace("GROUP BY", "WHERE w.n>0 GROUP BY"),
    ] {
        assert!(compile(&sql, &native).is_err(), "{sql}");
    }
    native.relations[1].columns.push(crate::source::Column {
        position: 5,
        name: "n".into(),
        oid: 23,
        modifier: -1,
        nullable: true,
        primary: false,
        collation: 0,
    });
    assert!(compile(&sql().replace("w.n *", "n *"), &native).is_err());
    assert!(compile(&sql().replace("GROUP BY", "WHERE n>0 GROUP BY"), &native).is_err());
    assert_ne!(
        serde_json::to_vec(&compile(&sql(), &native)?)?,
        serde_json::to_vec(&compile(&sql().replace("0,4", "0,3"), &native)?)?
    );
    Ok(())
}
#[tokio::test]
async fn expansion_limits_fail_before_visibility_and_empty_series_is_empty() -> Result<()> {
    let native = native();
    let compiled = compile(&sql(), &native)?;
    let ir = compiled.relational().context("missing expansion IR")?;
    let mut options = settings();
    options.limits = Limits { contributions: 4, ..options.limits };
    let query = relational::build(&native, &compiled, &options)?;
    let before = serde_json::to_vec(&query.checkpoint()?)?;
    let input = row(&[
        ("id", Some("1")),
        ("auction", Some("1")),
        ("price", Some("1")),
        ("event_time", Some("2000-01-01 00:00:00+00")),
    ]);
    let delta = batch(vec![("bid", input, 1)]);
    assert!(query.prepare(relational::inputs(&delta, ir, 1)?).await.is_err());
    assert_eq!(serde_json::to_vec(&query.checkpoint()?)?, before);
    let empty = compile(&sql().replace("0,4", "4,0"), &native)?;
    let ir = empty.relational().context("missing empty IR")?;
    let query = relational::build(&native, &empty, &settings())?;
    let prepared = query.prepare(relational::inputs(&delta, ir, 1)?).await?;
    assert_eq!(prepared.output().batch.iter().count(), 0);
    Ok(())
}

#[tokio::test]
async fn expanded_rows_require_explicit_composite_order_and_match_neighbors() -> Result<()> {
    let native = contract();
    let sql = "SELECT b.id,w.n,COUNT(*) OVER(PARTITION BY b.auction ORDER BY b.id,w.n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS neighbors FROM source.bid b CROSS JOIN generate_series(0,2) w(n)";
    assert!(compile(&sql.replace("ORDER BY b.id,w.n", "ORDER BY b.id"), &native).is_err());
    let compiled = compile(sql, &native)?;
    let ir = compiled.relational().context("missing expanded ROWS IR")?;
    let query = relational::build(&native, &compiled, &settings())?;
    let input = row(&[("id", Some("1")), ("auction", None), ("price", None)]);
    let delta = batch(vec![("bid", input, 1)]);
    let prepared = query.prepare(relational::inputs(&delta, ir, 1)?).await?;
    let expected = (0..3).map(|ordinal| {
        (
            (
                Vec::new(),
                [
                    ("0:id".into(), Some("1".into())),
                    ("@series_0".into(), Some(ordinal.to_string())),
                    ("@aggregate_0".into(), Some(if ordinal == 1 { "3" } else { "2" }.into())),
                ]
                .into(),
            ),
            1,
        )
    });
    assert_eq!(prepared.output().batch, Batch::from_updates(expected)?);
    Ok(())
}

#[test]
fn reusable_expansion_preserves_complete_tick_cancellation_and_spill_limits() -> Result<()> {
    use crate::engine::dataflow::{Expand, TimedBatch};
    let input = TimedBatch {
        time: 7,
        batch: Batch::from_updates([(((), 0_i32), i64::MAX), (((), 1), 1), (((), 2), -1)])?,
    };
    let expand = Expand::new(|(): &(), _: &i32, emit| {
        for ordinal in 0..2 {
            emit(((), ordinal))?;
        }
        Ok(())
    });
    let limits = Limits { resident_entries: 1, ..Limits::default() };
    let output = expand.evaluate(&input, limits)?;
    assert_eq!(output.time, 7);
    assert_eq!(output.batch, Batch::from_updates([(((), 0), i64::MAX), (((), 1), i64::MAX)])?);
    assert!(expand.evaluate(&input, Limits { contributions: 5, ..limits }).is_err());
    assert_eq!(input.time, 7);
    Ok(())
}
