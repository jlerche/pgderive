use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
type Bag = Batch<relational::Key, Row>;
fn native() -> crate::source::Contract {
    let mut native = contract();
    native.relations[1].columns.push(crate::source::Column {
        position: 4,
        name: "step".into(),
        oid: 23,
        modifier: -1,
        nullable: true,
        primary: false,
        collation: 0,
    });
    native
}
fn sql(derived: bool) -> String {
    let window = "OVER(PARTITION BY b.auction ORDER BY b.id)";
    let functions = [
        "lag(b.price)",
        "lead(b.price)",
        "lag(b.price,b.step,b.price)",
        "lead(b.price,-1,99)",
        "lag(b.price,0,99)",
        "lag(b.price,NULL,99)",
        "lead(b.price,-2147483648,b.price)",
        "row_number()",
    ];
    let functions = functions
        .iter()
        .enumerate()
        .map(|(index, function)| format!("{function} {window} AS f{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let predicate = if derived { " WHERE b.id<>1" } else { "" };
    let query = format!("SELECT b.id,{functions} FROM source.bid b{predicate}");
    if derived { format!("SELECT q.id,q.f0 FROM({query}) q WHERE q.f7<=2") } else { query }
}
fn oracle(state: &BTreeMap<Row, i64>, derived: bool) -> Result<Bag> {
    let mut groups: BTreeMap<Option<String>, Vec<Row>> = BTreeMap::new();
    for (row, weight) in state {
        if derived && row["id"].as_deref() == Some("1") {
            continue;
        }
        for _ in 0..*weight {
            groups.entry(row["auction"].clone()).or_default().push(row.clone());
        }
    }
    let mut output = Vec::new();
    for mut rows in groups.into_values() {
        rows.sort_by_key(|row| row["id"].as_ref().and_then(|value| value.parse::<i64>().ok()));
        for (index, row) in rows.iter().enumerate().take(if derived { 2 } else { usize::MAX }) {
            let value = |position: Option<usize>, default: Option<String>| {
                position
                    .and_then(|position| rows.get(position))
                    .map_or(default, |row| row["price"].clone())
            };
            let mut result: Row = [
                ("0:id".into(), row["id"].clone()),
                ("@aggregate_0".into(), value(index.checked_sub(1), None)),
            ]
            .into();
            if !derived {
                let shifted = row["step"]
                    .as_ref()
                    .map(|step| -> Result<_> {
                        let target = i64::try_from(index)? - step.parse::<i64>()?;
                        Ok(value(usize::try_from(target).ok(), row["price"].clone()))
                    })
                    .transpose()?
                    .flatten();
                for (field, value) in [
                    value(index.checked_add(1), None),
                    shifted,
                    value(index.checked_sub(1), Some("99".into())),
                    row["price"].clone(),
                    None,
                    row["price"].clone(),
                    Some((index + 1).to_string()),
                ]
                .into_iter()
                .enumerate()
                {
                    result.insert(format!("@aggregate_{}", field + 1), value);
                }
            }
            output.push(((Vec::new(), result), 1));
        }
    }
    Batch::from_updates(output)
}
async fn history(derived: bool) -> Result<()> {
    let native = native();
    let compiled = compile(&sql(derived), &native)?;
    let ir = compiled.relational().context("missing navigation IR")?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let input = |id, price, step| {
        row(&[("id", Some(id)), ("auction", None), ("price", price), ("step", step)])
    };
    let a = input("1", None, Some("1"));
    let b = input("2", Some("7"), Some("0"));
    let c = input("3", None, Some("-1"));
    let d = input("4", Some("-2"), None);
    let extreme = input("5", Some("9"), Some("-2147483648"));
    let mut moved = input("2", Some("-7"), Some("2"));
    moved.insert("auction".into(), Some("1".into()));
    let mut state = BTreeMap::new();
    for (index, changes) in [
        batch(vec![
            ("bid", a.clone(), 2),
            ("bid", b.clone(), 3),
            ("bid", c.clone(), 1),
            ("bid", d.clone(), 1),
            ("bid", extreme.clone(), 1),
        ]),
        batch(vec![("bid", b, -3), ("bid", moved.clone(), 3)]),
        batch(vec![("bid", a.clone(), -1), ("bid", d, -1)]),
        batch(vec![("bid", a, -1), ("bid", c, -1), ("bid", extreme, -1), ("bid", moved, -3)]),
    ]
    .into_iter()
    .enumerate()
    {
        let before = oracle(&state, derived)?;
        for change in &changes.updates {
            *state.entry(change.tuple.row.clone()).or_default() += change.weight;
        }
        let after = oracle(&state, derived)?;
        let expected = Batch::from_updates(
            before
                .iter()
                .map(|(tuple, weight)| (tuple.clone(), -*weight))
                .chain(after.iter().map(|(tuple, weight)| (tuple.clone(), *weight))),
        )?;
        let prepared = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(prepared.output().batch, expected);
        query.commit(prepared)?;
        if index == 1 {
            let checkpoint = query.checkpoint()?;
            query = relational::build(&native, &compiled, &options)?;
            query.restore_checkpoint(checkpoint).await?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn navigation_respects_nulls_signed_offsets_current_defaults_and_weights() -> Result<()> {
    history(false).await
}
#[tokio::test]
async fn source_filter_precedes_navigation_and_outer_rank_filter() -> Result<()> {
    history(true).await
}
#[test]
fn navigation_normalizes_defaults_frames_and_rejects_unbound_forms() -> Result<()> {
    let native = native();
    let simple = "SELECT lag(b.price) OVER(PARTITION BY b.auction ORDER BY b.id) FROM source.bid b";
    let compiled = compile(simple, &native)?;
    let explicit = simple
        .replace("lag(b.price)", "pg_catalog.lag(b.price,1,NULL)")
        .replace("b.id)", "b.id ROWS BETWEEN CURRENT ROW AND CURRENT ROW)");
    assert_eq!(serde_json::to_vec(&compiled)?, serde_json::to_vec(&compile(&explicit, &native)?)?);
    for query in [
        simple.replace("ORDER BY b.id", "ORDER BY b.price"),
        simple.replace("lag(b.price)", "lag(b.price,2147483648)"),
        simple.replace("lag(b.price)", "lag(b.price,b.price)"),
        simple.replace("lag(b.price)", "lag(b.price,1,true)"),
        simple.replace("lag(b.price)", "lag(b.price,1,'bad')"),
        simple.replace("lag(b.price)", "lag(b.price,1,mod(b.price,2))"),
        simple.replace("lag(b.price)", "lag(b.price) FILTER(WHERE b.price>0)"),
        simple.replace("lag(b.price)", "lag(*)"),
        simple.replace("lag(b.price)", "lag()"),
        simple.replace("lag(b.price)", "lag(b.price,1,NULL,1)"),
        simple.replace("lag(b.price)", "lag(b.price,true)"),
        simple.replace("lag(b.price)", "lag(b.price,mod(b.step,2))"),
    ] {
        assert!(compile(&query, &native).is_err(), "{query}");
    }
    Ok(())
}
#[test]
fn navigation_binds_postgres_common_types_and_native_defaults() -> Result<()> {
    let mut native = native();
    native.relations[1].columns[2].oid = 21;
    let mut label = native.relations[1].columns[2].clone();
    label.position = 5;
    label.name = "label".into();
    label.oid = 25;
    label.collation = 100;
    native.relations[1].columns.push(label);
    for (argument, default, expected) in [
        (21, "0", 23),
        (23, "9223372036854775807", 20),
        (20, "b.price", 20),
        (1700, "0", 1700),
        (25, "'first'", 25),
        (1043, "'first'", 1043),
        (1043, "b.label", 1043),
        (25, "b.label", 25),
        (16, "true", 16),
        (2950, "b.price", 2950),
        (1114, "b.price", 1114),
        (1184, "b.price", 1184),
    ] {
        native.relations[1].columns[2].oid = argument;
        let sql = format!("SELECT lag(b.price,1,{default}) OVER(ORDER BY b.id) FROM source.bid b");
        let compiled = compile(&sql, &native)?;
        let ir = compiled.relational().context("missing native navigation")?;
        assert_eq!(ir.output.columns[0].column.oid, expected);
        assert_eq!(ir.output.terminal.is_some(), expected == 1700);
    }
    Ok(())
}
#[tokio::test]
async fn navigation_copies_native_values_and_never_replaces_present_nulls() -> Result<()> {
    for (oid, value) in [
        (16, "t"),
        (25, "hello"),
        (1043, "world"),
        (2950, "00000000-0000-0000-0000-000000000001"),
        (1114, "1999-12-31 23:59:59.999999"),
        (1184, "2000-01-01 00:00:00+00"),
        (1700, "12345678901234567890.1234567890"),
    ] {
        let mut native = native();
        native.relations[1].columns[2].oid = oid;
        let compiled = compile(
            "SELECT b.id,lag(b.price,1,b.price) OVER(ORDER BY b.id) AS previous,lead(b.price,1,b.price) OVER(ORDER BY b.id) AS next FROM source.bid b",
            &native,
        )?;
        let ir = compiled.relational().context("missing native navigation")?;
        let mut query = relational::build(&native, &compiled, &settings())?;
        let changes = batch(vec![
            (
                "bid",
                row(&[("id", Some("1")), ("auction", None), ("price", None), ("step", None)]),
                1,
            ),
            (
                "bid",
                row(&[
                    ("id", Some("2")),
                    ("auction", None),
                    ("price", Some(value)),
                    ("step", None),
                ]),
                1,
            ),
        ]);
        let prepared = query.prepare(relational::inputs(&changes, ir, 1)?).await?;
        let expected = [
            [
                ("0:id".into(), Some("1".into())),
                ("@aggregate_0".into(), None),
                ("@aggregate_1".into(), Some(value.into())),
            ]
            .into(),
            [
                ("0:id".into(), Some("2".into())),
                ("@aggregate_0".into(), None),
                ("@aggregate_1".into(), Some(value.into())),
            ]
            .into(),
        ];
        assert_eq!(
            prepared.output().batch,
            Batch::from_updates(expected.map(|row| ((Vec::new(), row), 1)))?
        );
        query.commit(prepared)?;
    }
    Ok(())
}
#[test]
fn expanded_navigation_requires_ordinal_and_revision_binds_offsets() -> Result<()> {
    let native = native();
    let sql = "SELECT lag(b.price) OVER(ORDER BY b.id,w.n) FROM source.bid b CROSS JOIN generate_series(0,2) w(n)";
    assert!(compile(&sql.replace(",w.n", ""), &native).is_err());
    let compiled = compile(sql, &native)?;
    assert_ne!(
        serde_json::to_vec(&compiled)?,
        serde_json::to_vec(&compile(&sql.replace("lag(b.price)", "lag(b.price,2)"), &native)?)?
    );
    let json = serde_json::to_string(&compile(
        "SELECT rank() OVER(ORDER BY b.price) FROM source.bid b",
        &native,
    )?)?;
    assert!(!json.contains("navigation"));
    Ok(())
}
#[tokio::test]
async fn expanded_navigation_counts_occurrences_before_output_collisions() -> Result<()> {
    let native = native();
    let sql = "SELECT lag(b.price) OVER(ORDER BY b.id,w.n) FROM source.bid b CROSS JOIN generate_series(0,2) w(n)";
    let compiled = compile(sql, &native)?;
    let ir = compiled.relational().context("missing expanded navigation")?;
    let mut query = relational::build(&native, &compiled, &settings())?;
    let input = row(&[("id", Some("1")), ("auction", None), ("price", Some("7")), ("step", None)]);
    let changes = batch(vec![("bid", input.clone(), 2)]);
    let prepared = query.prepare(relational::inputs(&changes, ir, 1)?).await?;
    let output = |value: Option<&str>| [("@aggregate_0".into(), value.map(Into::into))].into();
    assert_eq!(
        prepared.output().batch,
        Batch::from_updates([
            ((Vec::new(), output(None)), 1),
            ((Vec::new(), output(Some("7"))), 5)
        ])?
    );
    query.commit(prepared)?;
    let changes = batch(vec![("bid", input, -1)]);
    let prepared = query.prepare(relational::inputs(&changes, ir, 2)?).await?;
    assert_eq!(
        prepared.output().batch,
        Batch::from_updates([((Vec::new(), output(Some("7"))), -3)])?
    );
    query.commit(prepared)?;
    Ok(())
}
