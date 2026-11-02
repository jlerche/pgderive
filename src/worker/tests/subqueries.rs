//! Subqueries share aggregate lowering and retain SQL NULL membership marks.
use super::{History, JoinedState, batch, contract, row, settings};
use crate::{
    compiler::compile, engine::Batch, source::Contract, transaction::Row,
    worker::program::relational,
};
use anyhow::{Context, Result};
fn native() -> Contract {
    let mut native = contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    native
}
fn member(probe: Option<i64>, values: &[Option<i64>]) -> Option<bool> {
    if values.is_empty() {
        return Some(false);
    }
    let probe = probe?;
    if values.contains(&Some(probe)) {
        return Some(true);
    }
    (!values.contains(&None)).then_some(false)
}
fn changes() -> Vec<crate::weighted::Batch> {
    let left_match = row(&[("id", Some("1")), ("category", Some("same")), ("other", Some("1"))]);
    let left_miss = row(&[("id", Some("2")), ("category", Some("same")), ("other", Some("2"))]);
    let left_null = row(&[("id", Some("3")), ("category", None), ("other", None)]);
    let left_empty = row(&[("id", Some("4")), ("category", Some("empty")), ("other", Some("4"))]);
    let right_match = row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("1"))]);
    let right_null = row(&[("id", Some("2")), ("auction", Some("1")), ("price", None)]);
    let right_other = row(&[("id", Some("3")), ("auction", Some("2")), ("price", Some("7"))]);
    vec![
        batch(vec![
            ("auction", left_match.clone(), 2),
            ("auction", left_miss.clone(), 1),
            ("auction", left_null.clone(), 1),
            ("auction", left_empty.clone(), 1),
            ("bid", right_match.clone(), 3),
            ("bid", right_null.clone(), 2),
            ("bid", right_other.clone(), 1),
        ]),
        batch(vec![("bid", right_match.clone(), -3), ("bid", right_null.clone(), -1)]),
        batch(vec![("bid", right_null, -1), ("auction", left_miss.clone(), -1)]),
        batch(vec![("bid", right_match.clone(), 1), ("auction", left_miss.clone(), 1)]),
        batch(vec![
            ("auction", left_match.clone(), -2),
            ("auction", left_miss, -1),
            ("auction", left_null, -1),
            ("auction", left_empty, -1),
            ("bid", right_match, -1),
            ("bid", right_other, -1),
        ]),
        batch(vec![("auction", left_match, 1)]),
    ]
}
fn outputs(
    state: &JoinedState,
    names: &[String],
    values: impl Fn(&Row, &[(Row, i64)]) -> Result<Vec<Option<String>>>,
) -> Result<super::RowBatch> {
    let right = state
        .1
        .iter()
        .filter(|(_, weight)| **weight > 0)
        .map(|(row, weight)| (row.clone(), *weight))
        .collect::<Vec<_>>();
    Batch::from_updates(
        state
            .0
            .iter()
            .filter(|(_, weight)| **weight > 0)
            .map(|(row, weight)| {
                Ok((
                    (
                        relational::Key::new(),
                        names.iter().cloned().zip(values(row, &right)?).collect::<Row>(),
                    ),
                    *weight,
                ))
            })
            .collect::<Result<Vec<_>>>()?,
    )
}
#[tokio::test]
async fn correlated_membership_preserves_marks_duplicates_null_poison_and_empty_sets() -> Result<()>
{
    let native = native();
    let compiled = compile(
        "SELECT a.category AS c,a.other IN(SELECT b.price FROM source.bid b WHERE b.auction=a.other) AS yes,a.other NOT IN(SELECT b.price FROM source.bid b WHERE b.auction=a.other) AS no FROM source.auction a",
        &native,
    )?;
    let names = compiled
        .output()
        .columns
        .iter()
        .map(|column| column.column.name.clone())
        .collect::<Vec<_>>();
    let options = settings();
    let mut history = History::new((&native, &compiled, &options));
    history.compact = true;
    history
        .run(
            changes(),
            |state| {
                outputs(state, &names, |left, right| {
                    let values = right
                        .iter()
                        .filter(|(right, _)| {
                            left["other"].is_some() && left["other"] == right["auction"]
                        })
                        .map(|(row, _)| row["price"].as_deref().map(str::parse::<i64>).transpose())
                        .collect::<Result<Vec<_>, _>>()?;
                    let result =
                        member(left["other"].as_deref().map(str::parse).transpose()?, &values);
                    Ok(vec![
                        left["category"].clone(),
                        result.map(|value| if value { "t" } else { "f" }.into()),
                        result.map(|value| if value { "f" } else { "t" }.into()),
                    ])
                })
            },
            super::apply_joined,
            |_| true,
        )
        .await?;
    Ok(())
}
#[tokio::test]
async fn correlated_scalar_count_having_distinguishes_missing_and_suppressed_groups() -> Result<()>
{
    let native = native();
    for condition in ["", " HAVING COUNT(*)=0", " HAVING COUNT(*)>2", " LIMIT 0"] {
        let compiled = compile(&format!("SELECT a.category AS c,(SELECT COUNT(*) FROM source.bid b WHERE b.auction=a.other{condition}) AS n FROM source.auction a"), &native).with_context(|| condition.to_owned())?;
        let names = compiled
            .output()
            .columns
            .iter()
            .map(|column| column.column.name.clone())
            .collect::<Vec<_>>();
        let options = settings();
        let mut history = History::new((&native, &compiled, &options));
        history.compact = true;
        history
            .run(
                changes(),
                |state| {
                    outputs(state, &names, |left, right| {
                        let count = right
                            .iter()
                            .filter(|(right, _)| correlated(left, right))
                            .map(|(_, weight)| weight)
                            .sum::<i64>();
                        let visible = match condition {
                            "" => true,
                            " HAVING COUNT(*)=0" => count == 0,
                            " HAVING COUNT(*)>2" => count > 2,
                            _ => false,
                        };
                        Ok(vec![left["category"].clone(), visible.then(|| count.to_string())])
                    })
                },
                super::apply_joined,
                |_| true,
            )
            .await?;
    }
    Ok(())
}
#[test]
fn subquery_admission_scopes_contextual_literals_and_lazy_boundaries() -> Result<()> {
    let native = native();
    for sql in [
        "SELECT a.id,(SELECT SUM(price) FROM source.bid) FROM source.auction a",
        "SELECT a.id,(SELECT AVG(price) FROM source.bid) FROM source.auction a",
        "SELECT a.id,COALESCE((SELECT SUM(price) FROM source.bid),0)+a.id FROM source.auction a",
        "SELECT a.id FROM source.auction a WHERE a.id>(SELECT AVG(price) FROM source.bid)",
        "SELECT a.id IN(SELECT NULL::bigint),a.id IN(SELECT '1'::bigint) FROM source.auction a",
        "SELECT a.id IN(SELECT price FROM source.bid UNION ALL SELECT price FROM source.bid) FROM source.auction a",
        "SELECT a.id IN(SELECT price AS x FROM source.bid ORDER BY x LIMIT 1) FROM source.auction a",
        "SELECT a.id IN(SELECT price AS x FROM source.bid GROUP BY price ORDER BY x LIMIT 1) FROM source.auction a",
        "SELECT a.id,(SELECT SUM(price) AS x FROM source.bid ORDER BY x LIMIT 1) FROM source.auction a",
        "SELECT (SELECT SUM(price) FROM source.bid) FROM source.auction ORDER BY sum LIMIT 1",
        "SELECT COALESCE((SELECT SUM(price) FROM source.bid),0) FROM source.auction ORDER BY coalesce LIMIT 1",
        "SELECT a.id IN(SELECT MAX(b.price) FROM source.bid b WHERE b.auction=a.other) FROM source.auction a",
        "SELECT a.id IN(SELECT a.price FROM source.bid a WHERE a.auction=1) FROM source.auction a",
        "SELECT a.id IN(1,NULL,3),a.id NOT IN(1,NULL,3),NOT(a.id IN(1,2)) FROM source.auction a",
        "SELECT CASE WHEN a.id IN(SELECT price FROM source.bid) THEN true ELSE false END FROM source.auction a",
        "SELECT 1 IN(SELECT 1),1 NOT IN(SELECT NULL::bigint)",
        "SELECT 1 IN(1,NULL,2),NULL NOT IN(1,2),'1' IN(1,2)",
        "SELECT a.id::smallint IN(32768::integer,'32768',10),a.id IN(1::numeric,'1.2',NULL) FROM source.auction a",
        "SELECT '1' IN(SELECT price FROM source.bid)",
        "SELECT a.id FROM source.auction a WHERE a.id NOT IN(SELECT auction FROM source.bid)",
        r#"SELECT q."?column?" FROM (SELECT a.id IN(SELECT price FROM source.bid) FROM source.auction a) q"#,
        "SELECT q.sum FROM (SELECT (SELECT SUM(price) FROM source.bid) FROM source.auction) q",
        "SELECT q.coalesce FROM (SELECT COALESCE((SELECT SUM(price) FROM source.bid),0) FROM source.auction) q",
    ] {
        compile(sql, &native).with_context(|| sql.to_owned())?.validate_execution()?;
    }
    for sql in [
        "SELECT CASE WHEN id>0 THEN 'true' ELSE false END FROM source.auction",
        "SELECT COALESCE(true,'false') FROM source.auction",
        "SELECT a.id IN(SELECT NULL) FROM source.auction a",
        "SELECT a.id IN(SELECT '1') FROM source.auction a",
        "SELECT NULL+(SELECT SUM(10/price) FROM source.bid) FROM source.auction",
        "SELECT (SELECT SUM((SELECT SUM(price) FROM source.bid)) FROM source.bid) FROM source.auction",
        "SELECT NULL=(SELECT SUM(10/price) FROM source.bid) FROM source.auction",
        "SELECT false AND (SELECT COUNT(*) FROM source.bid)>0 FROM source.auction",
        "SELECT (SELECT price FROM source.bid) FROM source.auction",
        "SELECT (SELECT MAX(price) FROM source.bid GROUP BY auction) FROM source.auction",
        "SELECT (SELECT b.auction+COUNT(*) FROM source.bid b WHERE b.auction=a.other) FROM source.auction a",
        "SELECT (SELECT COUNT(*) FROM source.bid b WHERE b.auction=a.other HAVING b.auction=1 AND COUNT(*)>0) FROM source.auction a",
        "SELECT (SELECT COUNT(*) FROM source.bid b WHERE b.auction=a.other ORDER BY b.auction LIMIT 0) FROM source.auction a",
        "SELECT (SELECT SUM(price) FROM source.bid ORDER BY 2) FROM source.auction",
        "SELECT (SELECT SUM(price) FROM source.bid ORDER BY 2 LIMIT 0) FROM source.auction",
        "SELECT (SELECT SUM(b.price) FROM source.bid b WHERE b.auction>a.id) FROM source.auction a",
        "SELECT (SELECT SUM(b.price+a.id) FROM source.bid b) FROM source.auction a",
        "SELECT CASE WHEN false THEN (SELECT SUM(10/price) FROM source.bid) ELSE 0 END FROM source.auction",
        "SELECT COALESCE(0,(SELECT SUM(10/price) FROM source.bid)) FROM source.auction",
        "SELECT SUM((SELECT SUM(10/price) FROM source.bid)) FILTER(WHERE false) FROM source.auction",
    ] {
        assert!(compile(sql, &native).is_err(), "{sql}");
    }
    Ok(())
}

fn correlated(left: &Row, right: &Row) -> bool {
    left["other"].is_some() && left["other"] == right["auction"]
}

#[tokio::test]
async fn uncorrelated_membership_ticks_are_linear_in_the_affected_group() -> Result<()> {
    let native = native();
    let compiled = compile(
        "SELECT a.id FROM source.auction a WHERE a.other NOT IN (SELECT b.auction FROM source.bid b)",
        &native,
    )?;
    let ir = compiled.relational();
    let size = 256_usize;
    let left = |id: usize| {
        row(&[("id", Some(&id.to_string())), ("category", None), ("other", Some(&id.to_string()))])
    };
    let right = |id: usize, value: Option<&str>| {
        row(&[("id", Some(&id.to_string())), ("auction", value), ("price", None)])
    };
    let initial = batch(
        (0..size)
            .map(|id| ("auction", left(id), 1))
            .chain((0..size).map(|id| ("bid", right(id, Some("-1")), 1)))
            .collect(),
    );
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let prepared = query.prepare(relational::inputs(&initial, ir, 1)?).await?;
    query.commit(prepared)?;
    let mut limited = options.clone();
    limited.limits.contributions = 20 * size;
    super::restart_relational(&mut query, &native, &compiled, &limited).await?;
    let added = batch(vec![("auction", left(size), 1)]);
    let prepared = query.prepare(relational::inputs(&added, ir, 2)?).await?;
    assert_eq!(prepared.output().batch.iter().count(), 1);
    query.commit(prepared)?;
    let poison = batch(vec![("bid", right(size, None), 1)]);
    let prepared = query.prepare(relational::inputs(&poison, ir, 3)?).await?;
    assert_eq!(prepared.output().batch.iter().count(), size + 1);
    assert!(prepared.output().batch.iter().all(|(_, weight)| *weight == -1));
    Ok(())
}
