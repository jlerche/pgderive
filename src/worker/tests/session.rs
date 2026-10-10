//! Direct connected-component oracle, independent of the compiled window stages.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
fn sql() -> String {
    let previous = "SELECT b.id,b.auction,b.price,lag(b.price) OVER(PARTITION BY b.auction ORDER BY b.price,b.id) AS previous FROM source.bid b WHERE b.price IS NOT NULL";
    let flags = format!(
        "SELECT q.id,q.auction,q.price,CASE WHEN q.previous IS NULL THEN 1 WHEN q.price=q.previous THEN 0 WHEN q.price-q.previous>=INTERVAL '10 seconds' THEN 1 ELSE 0 END AS flag FROM({previous}) q"
    );
    let numbered = format!(
        "SELECT z.auction,z.price,SUM(z.flag) OVER(PARTITION BY z.auction ORDER BY z.price,z.id ROWS UNBOUNDED PRECEDING) AS session FROM({flags}) z"
    );
    let grouped = format!(
        "SELECT s.auction,MIN(s.price) AS start,MAX(s.price) AS last,COUNT(*) AS n FROM({numbered}) s GROUP BY s.auction,s.session"
    );
    format!("SELECT g.auction,g.start,g.last+INTERVAL '10 seconds' AS finish,g.n FROM({grouped}) g")
}
fn source(id: i32, group: Option<&str>, seconds: Option<i32>) -> Row {
    let time = seconds.map(|value| format!("2000-01-01 00:00:{value:02}"));
    row(&[("id", Some(&id.to_string())), ("auction", group), ("price", time.as_deref())])
}
type Points = Vec<(i32, i64)>;
fn oracle(state: &BTreeMap<Row, i64>, fields: &[String]) -> Result<Batch<relational::Key, Row>> {
    let mut groups: BTreeMap<Option<String>, Points> = BTreeMap::new();
    for (row, weight) in state {
        if *weight == 0 {
            continue;
        }
        if let Some(time) = &row["price"] {
            let seconds = time.get(17..).context("bad oracle time")?.parse::<i32>()?;
            groups.entry(row["auction"].clone()).or_default().push((seconds, *weight));
        }
    }
    let mut outputs = Vec::new();
    for (group, mut points) in groups {
        points.sort_unstable();
        let mut sessions: Vec<(i32, i32, i64)> = Vec::new();
        for (point, weight) in points {
            if let Some((_, last, count)) =
                sessions.last_mut().filter(|(_, last, _)| point - *last < 10)
            {
                *last = point;
                *count += weight;
            } else {
                sessions.push((point, point, weight));
            }
        }
        for (start, last, count) in sessions {
            let output = row(&[
                (&fields[0], group.as_deref()),
                (&fields[1], Some(&format!("2000-01-01 00:00:{start:02}"))),
                (&fields[2], Some(&format!("2000-01-01 00:00:{:02}", last + 10))),
                (&fields[3], Some(&count.to_string())),
            ]);
            outputs.push(((Vec::new(), output), 1));
        }
    }
    Batch::from_updates(outputs)
}
#[tokio::test]
async fn complete_sessions_merge_split_move_and_restore_signed_bags() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 1114;
    let compiled = compile(&sql(), &native)?;
    let ir = compiled.relational().context("missing session IR")?;
    let fields =
        ir.output.columns.iter().map(|value| value.column.name.clone()).collect::<Vec<_>>();
    let revision = compiled.revision.as_deref().context("missing revision")?;
    assert!(revision.contains("pg-fixed-time-offset-v1"));
    assert!(revision.contains("pg-native-temporal-extrema-v1"));
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let first = source(1, None, Some(10));
    let last = source(2, None, Some(20));
    let bridge = source(3, None, Some(15));
    let moved = source(3, Some("7"), Some(15));
    let null = source(4, None, None);
    let mut state = BTreeMap::new();
    for changes in [
        batch(vec![("bid", first.clone(), 2), ("bid", last.clone(), 1), ("bid", null.clone(), 1)]),
        batch(vec![("bid", bridge.clone(), 1)]),
        batch(vec![("bid", bridge.clone(), -1), ("bid", moved.clone(), 1)]),
        batch(vec![
            ("bid", moved.clone(), -1),
            ("bid", bridge.clone(), 1),
            ("bid", first.clone(), -1),
        ]),
        batch(vec![("bid", bridge, -1)]),
        batch(vec![("bid", first, -1), ("bid", last, -1), ("bid", null, -1)]),
    ] {
        let before = oracle(&state, &fields)?;
        for update in &changes.updates {
            *state.entry(update.tuple.row.clone()).or_default() += update.weight;
        }
        let after = oracle(&state, &fields)?;
        let delta = Batch::from_updates(
            before
                .iter()
                .map(|(tuple, weight)| (tuple.clone(), -*weight))
                .chain(after.iter().map(|(tuple, weight)| (tuple.clone(), *weight))),
        )?;
        let prepared = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(prepared.output().batch, delta);
        query.commit(prepared)?;
        let checkpoint = query.checkpoint()?;
        query = relational::build(&native, &compiled, &options)?;
        query.restore_checkpoint(checkpoint).await?;
    }
    Ok(())
}
#[test]
fn offsets_reject_calendar_and_untyped_arithmetic_before_registration() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 1114;
    for expression in [
        "b.price+INTERVAL '1 day'",
        "b.price+INTERVAL '1 month'",
        "b.price+'10 seconds'",
        "b.id+INTERVAL '10 seconds'",
        "b.price*INTERVAL '10 seconds'",
        "b.price+INTERVAL '0 seconds'",
    ] {
        assert!(compile(&format!("SELECT {expression} FROM source.bid b"), &native).is_err());
    }
    assert_ne!(
        serde_json::to_vec(&compile(&sql(), &native)?)?,
        serde_json::to_vec(&compile(&sql().replace("g.last+", "g.last-"), &native)?)?
    );
    Ok(())
}
#[tokio::test]
async fn offset_range_error_leaves_checkpoint_unchanged() -> Result<()> {
    let mut native = contract();
    native.relations[1].columns[2].oid = 1114;
    for (operator, time) in [("+", "294276-12-31 23:59:59.999999"), ("-", "4714-11-24 00:00:00 BC")]
    {
        let compiled = compile(
            &format!("SELECT b.price{operator}INTERVAL '1 microsecond' FROM source.bid b"),
            &native,
        )?;
        let ir = compiled.relational().context("missing offset IR")?;
        let mut query = relational::build(&native, &compiled, &settings())?;
        let prior = serde_json::to_vec(&query.checkpoint()?)?;
        let bad = row(&[("id", Some("1")), ("auction", None), ("price", Some(time))]);
        let changes = batch(vec![("bid", bad, 1)]);
        let error = query
            .prepare(relational::inputs(&changes, ir, 1)?)
            .await
            .err()
            .context("offset must fail")?;
        assert!(format!("{error:#}").contains("out of range"));
        assert_eq!(serde_json::to_vec(&query.checkpoint()?)?, prior);
        let changes = batch(vec![("bid", source(1, None, Some(10)), 1)]);
        let prepared = query.prepare(relational::inputs(&changes, ir, 1)?).await?;
        query.commit(prepared)?;
    }
    Ok(())
}
