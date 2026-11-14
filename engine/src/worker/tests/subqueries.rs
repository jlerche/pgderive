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
        "SELECT CASE WHEN id>0 THEN 'true'::text ELSE false END FROM source.auction",
        "SELECT COALESCE(true,'false'::text) FROM source.auction",
        "SELECT a.id IN(SELECT NULL) FROM source.auction a",
        "SELECT a.id IN(SELECT '1') FROM source.auction a",
        "SELECT NULL+(SELECT SUM(10/price) FROM source.bid) FROM source.auction",
        "SELECT (SELECT SUM((SELECT SUM(price) FROM source.bid)) FROM source.bid) FROM source.auction",
        "SELECT NULL=(SELECT SUM(10/price) FROM source.bid) FROM source.auction",
        r#"SELECT NULL COLLATE "C" LIKE (SELECT SUM(10/price)::text FROM source.bid) FROM source.auction"#,
        "SELECT false AND (SELECT COUNT(*) FROM source.bid)>0 FROM source.auction",
        "SELECT (SELECT MAX(price) FROM source.bid GROUP BY auction) FROM source.auction",
        "SELECT (SELECT 1/price FROM source.bid) FROM source.auction",
        "SELECT (SELECT b.price FROM source.bid b WHERE b.auction=a.other AND 1/b.price>0)
         FROM source.auction a",
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

#[tokio::test]
async fn nonaggregate_scalar_checks_occurrences_at_the_read_site() -> Result<()> {
    use crate::engine::execution::EvaluationFailed;
    let native = native();
    let compiled = compile(
        "SELECT a.id,(SELECT b.price FROM source.bid b WHERE b.auction=a.other) AS value
         FROM source.auction a WHERE a.id>1",
        &native,
    )?;
    let ir = compiled.relational();
    let mut query = relational::build(&native, &compiled, &settings())?;
    let left = |id, key| row(&[("id", Some(id)), ("category", None), ("other", key)]);
    let right = |id, key, value| row(&[("id", Some(id)), ("auction", Some(key)), ("price", value)]);
    let ignored = right("1", "1", Some("9"));
    let single = right("2", "2", Some("7"));
    let matched = left("2", Some("2"));
    let initial = batch(vec![
        ("auction", left("1", Some("1")), 1),
        ("auction", matched, 2),
        ("auction", left("3", Some("3")), 1),
        ("auction", left("4", None), 1),
        ("bid", ignored, 2),
        ("bid", single.clone(), 1),
    ]);
    let prepared = query.prepare(relational::inputs(&initial, ir, 1)?).await?;
    assert_eq!(prepared.output().batch.iter().count(), 3);
    query.commit(prepared)?;
    let too_many = batch(vec![("bid", single.clone(), 1)]);
    match query.prepare(relational::inputs(&too_many, ir, 2)?).await {
        Err(error) if error.is::<EvaluationFailed>() => {
            assert_eq!(
                error.root_cause().to_string(),
                "more than one row returned by a subquery used as an expression"
            );
        }
        Err(error) => return Err(error),
        Ok(_) => anyhow::bail!("scalar accepted two identical occurrences"),
    }
    assert_eq!(query.time(), 1);
    let replacement = batch(vec![("bid", single, -1), ("bid", right("3", "2", None), 1)]);
    let prepared = query.prepare(relational::inputs(&replacement, ir, 2)?).await?;
    assert_eq!(prepared.output().batch.iter().map(|(_, weight)| weight.abs()).sum::<i64>(), 4);
    query.commit(prepared)?;
    Ok(())
}

#[tokio::test]
async fn uncorrelated_scalar_counts_projected_rows_and_respects_limit() -> Result<()> {
    let native = contract();
    let cases = [
        ("SELECT (SELECT price FROM source.bid WHERE false)", Some(None)),
        ("SELECT (SELECT price FROM source.bid ORDER BY id LIMIT 1)", Some(Some("7"))),
        ("SELECT (SELECT price FROM source.bid)", None),
        ("SELECT (VALUES (NULL::integer),(NULL::integer))", None),
        ("SELECT 1 WHERE (SELECT price FROM source.bid) IS NULL", None),
        ("SELECT q.column1 FROM (SELECT (VALUES (7))) q", Some(Some("7"))),
        ("SELECT q.x FROM (SELECT (SELECT 7 AS x UNION ALL SELECT 7 LIMIT 1)) q", Some(Some("7"))),
    ];
    for (sql, expected) in cases {
        let compiled = compile(sql, &native)?;
        let ir = compiled.relational();
        let query = relational::build(&native, &compiled, &settings())?;
        let source = batch(vec![
            ("bid", row(&[("id", Some("1")), ("auction", None), ("price", Some("7"))]), 1),
            ("bid", row(&[("id", Some("2")), ("auction", None), ("price", Some("7"))]), 1),
        ]);
        match (query.prepare(relational::inputs(&source, ir, 1)?).await, expected) {
            (Ok(prepared), Some(expected)) => {
                let ((_, row), weight) =
                    prepared.output().batch.iter().next().context("missing scalar output")?;
                assert_eq!(*weight, 1);
                assert_eq!(
                    row.values().next().context("missing scalar value")?.as_deref(),
                    expected
                );
            }
            (Err(error), None) if error.is::<crate::engine::execution::EvaluationFailed>() => {}
            (Err(error), _) => return Err(error),
            _ => anyhow::bail!("wrong scalar cardinality outcome for {sql}"),
        }
    }
    Ok(())
}

#[test]
fn scalar_set_input_rejects_fallible_implicit_row_conversion() {
    let mut native = contract();
    native.relations[1].columns[2].oid = 1082;
    let sql = "SELECT (SELECT price FROM source.bid UNION ALL SELECT timestamp '2000-01-01')
               FROM source.auction WHERE false";
    assert!(
        matches!(compile(sql, &native), Err(error) if format!("{error:#}").contains("scalar RHS row conversions must be total"))
    );
}

fn boolean_text(value: bool) -> String {
    if value { "t" } else { "f" }.into()
}

fn less_than_quantified(probe: Option<i64>, values: &[Option<i64>], all: bool) -> Option<bool> {
    let outcomes = values.iter().map(|value| Some(probe? < (*value)?));
    if outcomes.clone().any(|result| result == Some(!all)) {
        Some(!all)
    } else if outcomes.clone().any(|result| result.is_none()) {
        None
    } else {
        Some(all)
    }
}

#[tokio::test]
async fn quantified_comparison_preserves_weighted_rows_and_group_truth() -> Result<()> {
    let native = native();
    let compiled = compile(
        "SELECT a.category AS c,a.other < ANY(SELECT b.price FROM source.bid b WHERE b.auction=a.other) AS some,a.other < ALL(SELECT b.price FROM source.bid b WHERE b.auction=a.other) AS all_less FROM source.auction a",
        &native,
    )?;
    let names = compiled.output().columns.iter().map(|c| c.column.name.clone()).collect::<Vec<_>>();
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
                    let probe = left["other"].as_deref().map(str::parse).transpose()?;
                    let result = |all| less_than_quantified(probe, &values, all).map(boolean_text);
                    Ok(vec![left["category"].clone(), result(false), result(true)])
                })
            },
            super::apply_joined,
            |_| true,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn quantified_where_keeps_inequality_and_preserves_literal_width() -> Result<()> {
    let native = native();
    let compiled = compile(
        "SELECT a.category AS c, 100000 < ALL (VALUES (1::smallint)) AS wide, 100000 = ANY (VALUES (1::smallint)) AS wide_eq, NULL < ANY (SELECT 1 WHERE false) AS empty_any, NULL < ALL (SELECT 1 WHERE false) AS empty_all,
            '9007199254740993.000' = ANY (VALUES (9007199254740993.00)) AS exact_eq,
            9007199254740993 < ALL (VALUES (9007199254740993.000000000000000001)) AS exact_order,
            'Infinity'::numeric < ANY (VALUES ('NaN'::numeric)) AS special_order
         FROM source.auction a WHERE a.other=2 AND a.other < SOME(SELECT b.price FROM source.bid b WHERE b.auction=a.other)",
        &native,
    )?;
    let names = compiled.output().columns.iter().map(|c| c.column.name.clone()).collect::<Vec<_>>();
    let options = settings();
    History::new((&native, &compiled, &options))
        .run(
            changes(),
            |state| {
                let matching = (
                    state
                        .0
                        .iter()
                        .filter(|(row, _)| row["other"].as_deref() == Some("2"))
                        .map(|(row, weight)| (row.clone(), *weight))
                        .collect(),
                    state.1.clone(),
                );
                outputs(&matching, &names, |left, _| {
                    Ok(vec![
                        left["category"].clone(),
                        Some("f".into()),
                        Some("f".into()),
                        Some("f".into()),
                        Some("t".into()),
                        Some("t".into()),
                        Some("t".into()),
                        Some("t".into()),
                    ])
                })
            },
            super::apply_joined,
            |_| true,
        )
        .await?;
    Ok(())
}

#[test]
fn explicit_quantified_equality_rejects_unsupported_operands() {
    let native = native();
    for quantifier in ["= ANY", "<> ALL"] {
        for (probe, rhs) in [
            ("a.category", "SELECT 'x'::text"),
            ("'invalid'", "SELECT '00000000-0000-0000-0000-000000000001'::uuid"),
            (
                "'00000000-0000-0000-0000-000000000001'::text",
                "SELECT '00000000-0000-0000-0000-000000000001'::uuid",
            ),
            ("1::double precision", "SELECT 1.0"),
            ("a.other", "SELECT b.price + 1 FROM source.bid b"),
            ("a.other", "SELECT b.price FROM source.bid b WHERE b.auction=a.other + 1"),
        ] {
            let sql = format!("SELECT {probe} {quantifier} ({rhs}) FROM source.auction a");
            assert!(compile(&sql, &native).is_err(), "{sql}");
        }
    }
}

#[test]
fn membership_with_native_join_rhs_retains_correlation() -> Result<()> {
    let contract = native();
    let rhs = "SELECT c.price FROM source.bid b LEFT JOIN source.bid c ON b.id=c.auction
               WHERE b.auction=a.id AND c.price>a.other";
    for expression in [
        format!("EXISTS ({rhs})"),
        format!("a.other IN ({rhs})"),
        format!("a.other < ANY ({rhs})"),
        format!("a.other <= ALL ({rhs})"),
    ] {
        let sql = format!("SELECT {expression} FROM source.auction a");
        compile(&sql, &contract).map_err(|error| error.context(sql))?;
    }
    let rhs = "SELECT c.price FROM source.bid b JOIN source.bid c
               ON b.id=10/c.auction WHERE b.auction=a.id";
    for expression in [format!("a.other IN ({rhs})"), format!("a.other < ANY ({rhs})")] {
        let sql = format!("SELECT {expression} FROM source.auction a");
        assert!(compile(&sql, &contract).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn residual_membership_admits_inequality_only_correlation() -> Result<()> {
    let native = native();
    for predicate in [
        "b.price>a.id",
        "b.price::numeric > a.id::bigint",
        "b.auction=a.other AND NOT (b.id::numeric < a.id)",
        "b.auction=a.other OR b.price>a.id",
        "NOT (b.auction=a.other AND b.price>a.id)",
        "b.auction=a.other AND NOT (b.id=a.id)",
        "b.auction=a.other AND (b.price>a.id OR b.id=a.id)",
        "b.auction=a.other AND b.price>a.id",
        "b.auction=a.other AND a.id<=b.price AND b.id>a.id",
        "b.auction=a.other AND (b.price>a.id OR (b.id<a.id AND b.price<>a.id))",
        "b.auction=a.other AND NOT (b.price>a.id OR NOT (b.id<a.id))",
    ] {
        for (projection, filter) in [
            (
                format!("a.other IN (SELECT b.price FROM source.bid b WHERE {predicate})"),
                "true".into(),
            ),
            ("a.id".into(), format!("EXISTS (SELECT 1 FROM source.bid b WHERE {predicate})")),
        ] {
            compile(&format!("SELECT {projection} FROM source.auction a WHERE {filter}"), &native)?;
        }
    }
    for predicate in [
        "b.auction=a.other AND (b.price>a.id OR b.id>1)",
        "b.auction=a.other AND b.price+1>a.id",
        "b.auction=a.other AND b.price>10/a.other",
        "b.auction=a.other+1 AND b.price>a.id",
    ] {
        let in_query = format!(
            "SELECT a.other IN (SELECT b.price FROM source.bid b WHERE {predicate}) FROM source.auction a"
        );
        let exists_query = format!(
            "SELECT a.id FROM source.auction a WHERE EXISTS (SELECT 1 FROM source.bid b WHERE {predicate})"
        );
        assert!(compile(&in_query, &native).is_err(), "{in_query}");
        assert!(compile(&exists_query, &native).is_err(), "{exists_query}");
    }
    for sql in [
        "SELECT a.id FROM source.auction a WHERE EXISTS (SELECT 1 FROM source.bid b WHERE b.auction=1)",
        "SELECT a.id FROM source.auction a WHERE EXISTS (SELECT 1 FROM source.bid b WHERE b.auction=other)",
        "SELECT a.id FROM (SELECT id FROM source.auction) a WHERE EXISTS (SELECT 1 FROM source.bid b WHERE b.auction=a.id)",
    ] {
        compile(sql, &native)?;
    }
    let fallible_probe = "SELECT 10 / a.other IN (SELECT b.price FROM source.bid b
        WHERE b.auction=a.other AND b.price>a.id) FROM source.auction a";
    assert!(matches!(compile(fallible_probe, &native), Err(error)
        if format!("{error:#}").contains("requires a total membership probe")));
    Ok(())
}

#[test]
fn correlated_membership_elides_only_plain_distinct() -> Result<()> {
    let native = native();
    for comparison in ["IN", "NOT IN", "< ANY", ">= ALL"] {
        let query = format!(
            "SELECT a.other {comparison} (SELECT DISTINCT b.price FROM source.bid b
             WHERE b.auction = a.id AND b.id > a.id) FROM source.auction a"
        );
        compile(&query, &native)?;
        assert!(
            compile(&query.replace("DISTINCT b.price", "DISTINCT ON (b.price) b.price"), &native)
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn exists_discards_row_preserving_targets() -> Result<()> {
    let native = native();
    for (projection, predicate) in [
        ("b.*", "b.auction=a.id"),
        ("DISTINCT b.price", "b.auction=a.id"),
        ("DISTINCT b.price", "b.price>a.id"),
        ("b.price, NULL, 1 OPERATOR(pg_catalog./) 0", "b.price>a.id"),
        ("", "b.price>a.id"),
    ] {
        let exists = format!("EXISTS (SELECT {projection} FROM source.bid b WHERE {predicate})");
        compile(&format!("SELECT a.id FROM source.auction a WHERE {exists}"), &native)?;
        compile(&format!("SELECT {exists} FROM source.auction a"), &native)?;
    }
    for suffix in ["ORDER BY b.price LIMIT 1", "ORDER BY 1 LIMIT ALL", "LIMIT NULL"] {
        compile(
            &format!(
                "SELECT EXISTS (SELECT DISTINCT ON (b.price) b.price
            FROM source.bid b WHERE b.id > a.id {suffix}) FROM source.auction a"
            ),
            &native,
        )?;
    }
    for suffix in [
        "LIMIT 0",
        "OFFSET 1",
        "ORDER BY b.price FETCH FIRST 1 ROW WITH TIES",
        "ORDER BY generate_series(1,0)",
        "ORDER BY count(*)",
    ] {
        assert!(
            compile(
                &format!(
                    "SELECT EXISTS (SELECT b.price FROM source.bid b {suffix})
            FROM source.auction a"
                ),
                &native
            )
            .is_err()
        );
    }
    for projection in ["count(*)", "generate_series(1, 0)", "1 OPERATOR(public.##) 2"] {
        let sql =
            format!("SELECT EXISTS (SELECT {projection} FROM source.bid) FROM source.auction");
        assert!(compile(&sql, &native).is_err(), "{sql}");
    }
    Ok(())
}
#[test]
fn quantified_inequality_rejects_unsupported_operands() -> Result<()> {
    let native = native();
    for quantifier in ["< ANY", "<> ALL"] {
        let query = |probe, output, predicate| {
            format!(
                "SELECT {probe} {quantifier} (SELECT {output} FROM source.bid b WHERE {predicate}) FROM source.auction a"
            )
        };
        let keyed = "b.auction=a.other AND NOT (b.price>a.id OR b.id<a.id)";
        compile(&query("a.other", "b.price", keyed), &native)?;
        compile(&query("a.other", "b.price", "b.price>a.id AND b.id<a.id"), &native)?;
        let over_budget =
            format!("b.auction=a.other AND (b.price>a.id{})", " OR b.price>a.id".repeat(8));
        assert!(compile(&query("a.other", "b.price", &over_budget), &native).is_err());
        for rejected in [
            query("a.other", "b.price", "b.price>a.id LIMIT 1"),
            query("a.other", "b.price", "b.price>a.id AND b.id<a.id AND b.price=a.category"),
            query("a.other", "b.price", "b.auction=a.other AND b.price+1>a.id"),
            query("a.category", "'text'::text", keyed),
        ] {
            assert!(compile(&rejected, &native).is_err(), "{rejected}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn row_quantifiers_reduce_nulls_after_comparing_fields() -> Result<()> {
    let native = native();
    let compiled = compile(
        "SELECT (1, a.other::bigint) IN (SELECT b.auction, b.price FROM source.bid b) AS member,
                (1, a.other::bigint) < ANY (SELECT b.auction, b.price FROM source.bid b) AS earlier,
                (1, a.other::bigint) < ALL (SELECT b.auction, b.price FROM source.bid b) AS before_all
         FROM source.auction a",
        &native,
    )?;
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let outer = row(&[("id", Some("1")), ("category", None), ("other", None)]);
    let different = row(&[("id", Some("1")), ("auction", Some("2")), ("price", None)]);
    let equal = row(&[("id", Some("2")), ("auction", Some("1")), ("price", None)]);
    let columns = &compiled.output().columns;
    for (changes, expected) in [
        (
            batch(vec![("auction", outer, 1), ("bid", different.clone(), 1)]),
            [Some("f"), Some("t"), Some("t")],
        ),
        (batch(vec![("bid", different, -1), ("bid", equal.clone(), 1)]), [None, None, None]),
        (batch(vec![("bid", equal, -1)]), [Some("f"), Some("f"), Some("t")]),
    ] {
        let prepared = query
            .prepare(relational::inputs(&changes, compiled.relational(), query.time() + 1)?)
            .await?;
        let (_, output) = prepared
            .output()
            .batch
            .iter()
            .find_map(|(tuple, weight)| (*weight == 1).then_some(tuple))
            .context("missing row quantifier output")?;
        let actual: Vec<_> = columns
            .iter()
            .map(|column| output.get(&column.column.name).and_then(Option::as_deref))
            .collect();
        assert_eq!(actual, expected);
        query.commit(prepared)?;
    }
    Ok(())
}

#[test]
fn row_quantifiers_bind_matching_widths() -> Result<()> {
    let native = native();
    for operator in ["IN", "NOT IN", "= ANY", "< ALL", ">= ANY"] {
        let query = format!(
            "SELECT (a.id, a.other) {operator} (SELECT b.auction, b.price
             FROM source.bid b WHERE b.id > a.id) FROM source.auction a"
        );
        compile(&query, &native)?;
        assert!(compile(&query.replace("b.auction, b.price", "b.price"), &native).is_err());
    }
    Ok(())
}

#[test]
fn quantified_accepts_total_conversions_and_catalog_operators() -> Result<()> {
    let native = native();
    let sql = "SELECT a.id::numeric OPERATOR(pg_catalog.<) ANY
        (SELECT b.auction::bigint FROM source.bid b WHERE b.id > a.id)
        FROM source.auction a";
    compile(sql, &native)?;
    for rejected in [
        sql.replace("pg_catalog.<", "public.<"),
        sql.replace("a.id::numeric", "(10 / a.id)::numeric"),
        sql.replace("b.auction::bigint", "10 / b.auction"),
    ] {
        assert!(compile(&rejected, &native).is_err(), "{rejected}");
    }
    let aggregate = "SELECT a.id = ANY (SELECT count(*) FILTER (WHERE 1/(b.id-b.id)>0)
        FROM source.bid b) FROM source.auction a";
    assert!(compile(aggregate, &native).is_err());
    Ok(())
}

#[tokio::test]
async fn inequality_only_scan_obeys_contribution_limit() -> Result<()> {
    let native = native();
    let compiled = compile(
        "SELECT a.id, a.other < ANY (SELECT b.price FROM source.bid b WHERE b.auction < a.id) FROM source.auction a",
        &native,
    )?;
    let ir = compiled.relational();
    let size = 16_usize;
    let initial = batch(
        (0..size)
            .map(|id| {
                (
                    "auction",
                    row(&[("id", Some(&id.to_string())), ("category", None), ("other", None)]),
                    1,
                )
            })
            .collect(),
    );
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let prepared = query.prepare(relational::inputs(&initial, ir, 1)?).await?;
    query.commit(prepared)?;

    let mut limited = options;
    limited.limits.contributions = 2048;
    super::restart_relational(&mut query, &native, &compiled, &limited).await?;
    let delta = |count| {
        batch(
            (0..count)
                .map(|id: usize| {
                    (
                        "bid",
                        row(&[
                            ("id", Some(&id.to_string())),
                            ("auction", Some(&(100 + id).to_string())),
                            ("price", None),
                        ]),
                        1,
                    )
                })
                .collect(),
        )
    };
    let error = query
        .prepare(relational::inputs(&delta(256), ir, 2)?)
        .await
        .err()
        .context("global residual scan exceeded its work budget")?;
    assert!(
        matches!(
            error.downcast_ref::<crate::engine::execution::CapacityExceeded>(),
            Some(crate::engine::execution::CapacityExceeded("partition tick work limit exceeded"))
        ),
        "{error:#}"
    );

    let prepared = query.prepare(relational::inputs(&delta(1), ir, 2)?).await?;
    assert_eq!(prepared.output().batch.iter().count(), 0);
    Ok(())
}

#[test]
fn quantified_text_resolves_c_and_posix_collations() -> Result<()> {
    let native = native();
    for expression in [
        "'Z' < ANY (SELECT 'a' COLLATE \"C\")",
        "'Z' COLLATE \"POSIX\" < ALL (SELECT 'a'::text)",
        "NULL = ANY (SELECT 'a'::varchar COLLATE \"C\")",
        "'Z' COLLATE \"C\" < ANY (SELECT 'a' COLLATE \"POSIX\")",
        "'1' COLLATE \"C\" < ANY (SELECT 2)",
        "'Z' < ANY (VALUES ('a' COLLATE \"C\"))",
    ] {
        compile(&format!("SELECT {expression}"), &native)?;
    }
    for expression in ["'Z' < ANY (SELECT 'a'::text)", "1 < ALL (SELECT 'a' COLLATE \"C\")"] {
        assert!(compile(&format!("SELECT {expression}"), &native).is_err(), "{expression}");
    }
    Ok(())
}

#[test]
fn quantified_boolean_resolves_unknown_probes_and_rejects_other_types() -> Result<()> {
    let native = native();
    for expression in
        ["'of' < ANY (VALUES (TRUE), (NULL))", "NULL = ALL (SELECT FALSE WHERE FALSE)"]
    {
        compile(&format!("SELECT {expression}"), &native)?;
    }
    for expression in [
        "1 = ANY (SELECT TRUE)",
        "TRUE = ALL (SELECT 'true'::text)",
        "'invalid' <> ANY (SELECT FALSE)",
    ] {
        assert!(compile(&format!("SELECT {expression}"), &native).is_err(), "{expression}");
    }
    Ok(())
}

#[test]
fn quantified_uuid_resolves_unknown_probes() -> Result<()> {
    let native = native();
    compile(
        "SELECT '{80000000000000000000000000000000}' = ANY
            (VALUES ('80000000-0000-0000-0000-000000000000'::uuid), (NULL)),
            NULL < ALL (SELECT 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid WHERE FALSE)",
        &native,
    )?;
    Ok(())
}

#[tokio::test]
async fn quantified_temporal_preserves_mixed_types_and_edges() -> Result<()> {
    let native = native();
    let compiled = compile(
        "SELECT date '5874897-12-31' > ANY (VALUES (timestamp '294276-12-31 00:00:00')) AS far_date,
            date '5874897-12-31' <> ALL (VALUES (timestamptz 'infinity')) AS before_infinity,
            timestamp '2000-01-01' = ANY (VALUES (timestamptz '1999-12-31 19:00:00-05')) AS same_instant,
            '2000-01-01' = ANY (VALUES (date '2000-01-01')) AS unknown_date,
            NULL >= ALL (SELECT timestamptz 'infinity' WHERE false) AS empty_all,
            date '294277-01-01' > ANY (VALUES ('294276-12-31 23:59:59.999999'::timestamp(0))) AS rounded_date,
            '294276-12-31 23:59:59.999999'::timestamp(0) > ALL
                (VALUES ('294276-12-31 23:59:59.999999+00'::timestamptz(0))) AS rounded_timestamp
         FROM source.auction a",
        &native,
    )?;
    let names = compiled.output().columns.iter().map(|c| c.column.name.clone()).collect::<Vec<_>>();
    let options = settings();
    History::new((&native, &compiled, &options))
        .run(
            changes(),
            |state| outputs(state, &names, |_, _| Ok(vec![Some("t".into()); 7])),
            super::apply_joined,
            |_| true,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn quantified_interval_compares_duration_and_preserves_unknown() -> Result<()> {
    let native = native();
    let compiled = compile(
        "SELECT interval '1 month' = ANY (VALUES (interval '30 days'), (NULL)) AS equal_duration,
            interval '1 month' <> ALL (VALUES (interval '30 days'), (NULL)) AS unequal_duration,
            interval '1 month' < ANY (VALUES (interval '30 days'), (NULL)) AS unknown_order,
            interval '1 day' < ALL (VALUES (interval '25 hours')) AS increasing,
            NULL < ALL (SELECT interval '1 day' WHERE FALSE) AS empty_all
         FROM source.auction a",
        &native,
    )?;
    let names = compiled.output().columns.iter().map(|c| c.column.name.clone()).collect::<Vec<_>>();
    let options = settings();
    History::new((&native, &compiled, &options))
        .run(
            changes(),
            |state| {
                outputs(state, &names, |_, _| {
                    Ok(vec![
                        Some("t".into()),
                        Some("f".into()),
                        None,
                        Some("t".into()),
                        Some("t".into()),
                    ])
                })
            },
            super::apply_joined,
            |_| true,
        )
        .await?;
    Ok(())
}
