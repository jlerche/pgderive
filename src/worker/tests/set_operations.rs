//! Full-tuple SQL set multiplicities across synchronized sources and cold roots.
use super::{History, JoinedState, batch, contract, row, settings};
use crate::{
    compiler::{Compiled, compile},
    engine::Batch,
    transaction::Row,
    worker::program::relational,
};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
fn expected(compiled: &Compiled, state: &JoinedState, operation: &str) -> Result<super::RowBatch> {
    let mut left = BTreeMap::<Option<String>, i64>::new();
    let mut right = BTreeMap::<Option<String>, i64>::new();
    for (row, weight) in &state.0 {
        *left.entry(row["other"].clone()).or_default() += weight;
    }
    for (row, weight) in &state.1 {
        *right.entry(row["auction"].clone()).or_default() += weight;
    }
    let values =
        left.keys().chain(right.keys()).cloned().collect::<std::collections::BTreeSet<_>>();
    Batch::from_updates(values.into_iter().filter_map(|value| {
        let (a, b) = (*left.get(&value).unwrap_or(&0), *right.get(&value).unwrap_or(&0));
        let weight = match operation {
            "UNION ALL" => a + b,
            "UNION" | "UNION DISTINCT" => i64::from(a + b > 0),
            "INTERSECT" | "INTERSECT DISTINCT" => i64::from(a > 0 && b > 0),
            "INTERSECT ALL" => a.min(b),
            "EXCEPT" | "EXCEPT DISTINCT" => i64::from(a > 0 && b == 0),
            "EXCEPT ALL" => (a - b).max(0),
            _ => return None,
        };
        (weight > 0).then(|| {
            (
                (
                    relational::Key::new(),
                    Row::from([(compiled.output().columns[0].column.name.clone(), value)]),
                ),
                weight,
            )
        })
    }))
}
fn changes() -> Vec<crate::weighted::Batch> {
    let left = |id, value| row(&[("id", Some(id)), ("category", Some("same")), ("other", value)]);
    let right = |id, value| row(&[("id", Some(id)), ("auction", value), ("price", None)]);
    let (left_first, left_duplicate, left_null) =
        (left("1", Some("7")), left("2", Some("7")), left("3", None));
    let (right_match, right_null, right_other) =
        (right("1", Some("7")), right("2", None), right("3", Some("9")));
    vec![
        batch(vec![]),
        batch(vec![
            ("auction", left_first.clone(), 3),
            ("auction", left_duplicate.clone(), 2),
            ("auction", left_null.clone(), 2),
            ("bid", right_match.clone(), 3),
            ("bid", right_null.clone(), 1),
            ("bid", right_other.clone(), 2),
        ]),
        batch(vec![
            ("auction", left_first.clone(), -2),
            ("bid", right_match.clone(), -1),
            ("bid", right_null.clone(), 2),
        ]),
        batch(vec![
            ("auction", left_duplicate, -2),
            ("bid", right_match.clone(), -2),
            ("bid", right_other, -2),
        ]),
        batch(vec![("auction", left_first.clone(), -1), ("bid", right_match.clone(), 5)]),
        batch(vec![
            ("auction", left_null, -2),
            ("bid", right_null, -3),
            ("bid", right_match.clone(), -5),
        ]),
        batch(vec![("auction", left_first, 2), ("bid", right_match, 1)]),
    ]
}
#[tokio::test]
async fn set_algebra_signed_duplicates_nulls_empty_states_restart_and_compaction() -> Result<()> {
    let mut native = contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    for operation in [
        "UNION ALL",
        "UNION",
        "UNION DISTINCT",
        "INTERSECT",
        "INTERSECT DISTINCT",
        "INTERSECT ALL",
        "EXCEPT",
        "EXCEPT DISTINCT",
        "EXCEPT ALL",
    ] {
        let sql = format!(
            "SELECT other AS value FROM source.auction {operation} SELECT auction FROM source.bid"
        );
        let compiled = compile(&sql, &native)?;
        let options = settings();
        let mut history = History::new((&native, &compiled, &options));
        history.compact = true;
        history
            .run(
                changes(),
                |state| expected(&compiled, state, operation),
                super::apply_joined,
                |_| true,
            )
            .await?;
    }
    Ok(())
}
#[test]
fn set_scopes_coercions_order_and_numeric_representation_domains_are_explicit() -> Result<()> {
    let native = contract();
    for operation in ["UNION", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"] {
        for (left, right) in [
            ("1", "NULL"),
            ("NULL", "1"),
            ("1::smallint", "2::bigint"),
            ("1::numeric(5,2)", "1::numeric(8,2)"),
            ("1::numeric(5,-2)", "0::numeric(8,0)"),
            ("NULL", "1::numeric(5,2)"),
            ("1::numeric(5,2)", "NULL"),
            ("1", "1::numeric(5,0)"),
            ("'NaN'::numeric(5,2)", "'NaN'::numeric(8,2)"),
        ] {
            compile(
                &format!(
                    "SELECT {left} AS x {operation} SELECT {right} ORDER BY x LIMIT 3 OFFSET 1"
                ),
                &native,
            )?
            .validate_execution()?;
        }
        for (left, right) in [
            ("1::numeric", "1::numeric"),
            ("1.20", "1.200"),
            ("1::numeric(5,2)", "1::numeric(5,3)"),
            ("'1.20'", "1::numeric(5,2)"),
        ] {
            assert!(
                compile(&format!("SELECT {left} {operation} SELECT {right}"), &native).is_err()
            );
        }
    }
    compile("SELECT 1::numeric UNION ALL SELECT 1.200", &native)?.validate_execution()?;
    for sql in [
        "WITH c AS (SELECT auction AS k FROM source.bid INTERSECT ALL SELECT id FROM source.auction) SELECT k,COUNT(*) FROM c GROUP BY k HAVING COUNT(*)>0 ORDER BY k LIMIT 2",
        "SELECT 1 UNION SELECT 2 INTERSECT SELECT 2 EXCEPT SELECT 3",
        "(SELECT 1 AS v UNION ALL SELECT 2 ORDER BY v LIMIT 1) EXCEPT ALL (SELECT 3 UNION SELECT 4)",
        "SELECT d.v,COUNT(*) OVER() FROM (SELECT 1 AS v UNION SELECT 2) d",
    ] {
        compile(sql, &native)?.validate_execution()?;
    }
    Ok(())
}

#[tokio::test]
async fn numeric_grouped_branch_materializes_declared_scale_before_set_alignment() -> Result<()> {
    let mut native = contract();
    native.relations[0].columns[2].oid = 1700;
    native.relations[0].columns[2].modifier = crate::numeric::Typmod::new(12, 2)?.encoded();
    native.relations[0].columns[2].collation = 0;
    native.relations[1].columns[1].oid = 1700;
    native.relations[1].columns[1].modifier = crate::numeric::Typmod::new(10, 2)?.encoded();
    let left = row(&[("id", Some("1")), ("category", None), ("other", Some("1.20"))]);
    let ten = row(&[("id", Some("2")), ("category", None), ("other", Some("10.00"))]);
    let right = row(&[("id", Some("1")), ("auction", Some("1.20")), ("price", None)]);
    let changes = vec![
        batch(vec![
            ("auction", left.clone(), 1),
            ("auction", ten.clone(), 1),
            ("bid", right.clone(), 1),
        ]),
        batch(vec![("auction", ten, -1), ("bid", right.clone(), -1)]),
        batch(vec![("bid", right, 1)]),
        batch(vec![("auction", left, -1)]),
    ];
    for operation in ["UNION ALL", "UNION", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"] {
        let compiled = compile(
            &format!(
                "SELECT other AS value FROM source.auction GROUP BY other HAVING COUNT(*)>0 {operation} SELECT auction FROM source.bid"
            ),
            &native,
        )?;
        let options = settings();
        let mut history = History::new((&native, &compiled, &options));
        history.compact = true;
        history
            .run(
                changes.clone(),
                |state| expected(&compiled, state, operation),
                super::apply_joined,
                |_| true,
            )
            .await?;
    }
    Ok(())
}

fn union_all_native() -> crate::source::Contract {
    let mut native = contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    native
}
const UNION_ALL: &str =
    "SELECT other AS v FROM source.auction UNION ALL SELECT auction FROM source.bid";
fn union_left(id: &str, value: &str) -> Row {
    row(&[("id", Some(id)), ("category", None), ("other", Some(value))])
}
fn union_right(id: &str, value: &str) -> Row {
    row(&[("id", Some(id)), ("auction", Some(value)), ("price", None)])
}
#[tokio::test]
async fn union_all_rejects_only_final_overflow_and_excess_contributions() -> Result<()> {
    let native = union_all_native();
    let compiled = compile(UNION_ALL, &native)?;
    let ir = compiled.relational();
    let (left, right) = (union_left("1", "7"), union_right("2", "7"));
    let query = relational::build(&native, &compiled, &settings())?;
    let overflow = batch(vec![("auction", left.clone(), i64::MAX), ("bid", right.clone(), 1)]);
    let error = query
        .prepare(relational::inputs(&overflow, ir, 1)?)
        .await
        .err()
        .context("overflowing union accepted")?;
    assert!(format!("{error:#}").contains("exceeds i64"), "{error:#}");
    let mut limited = settings();
    limited.limits.contributions = 1;
    let query = relational::build(&native, &compiled, &limited)?;
    let wide = batch(vec![("auction", left, 1), ("bid", right, 1)]);
    let error =
        query.prepare(relational::inputs(&wide, ir, 1)?).await.err().context("work accepted")?;
    assert!(error.is::<crate::engine::execution::CapacityExceeded>(), "{error:#}");
    Ok(())
}
#[tokio::test]
async fn union_all_cancels_extreme_signed_branches_through_spill() -> Result<()> {
    let native = union_all_native();
    let compiled = compile(UNION_ALL, &native)?;
    let ir = compiled.relational();
    let mut options = settings();
    options.limits.resident_entries = 1;
    let mut query = relational::build(&native, &compiled, &options)?;
    let (cancelled, partial) = (union_left("1", "7"), union_left("2", "8"));
    let initial =
        batch(vec![("auction", cancelled.clone(), i64::MAX), ("auction", partial.clone(), 3)]);
    let prepared = query.prepare(relational::inputs(&initial, ir, 1)?).await?;
    query.commit(prepared)?;
    let change = batch(vec![
        ("auction", cancelled, -i64::MAX),
        ("bid", union_right("3", "7"), i64::MAX),
        ("auction", partial, -2),
        ("bid", union_right("4", "8"), 1),
    ]);
    let prepared = query.prepare(relational::inputs(&change, ir, 2)?).await?;
    let output = prepared
        .output()
        .batch
        .iter()
        .map(|((_, row), weight)| (row.values().cloned().collect::<Vec<_>>(), *weight))
        .collect::<Vec<_>>();
    assert_eq!(output, [(vec![Some("8".to_owned())], -1)]);
    Ok(())
}

#[test]
fn boolean_set_operands_resolve_unknowns_pairwise() -> Result<()> {
    let native = contract();
    for operation in ["UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"] {
        for (left, right) in [
            ("true", "'yes'"),
            ("'off' COLLATE \"C\"", "false"),
            ("true", "NULL COLLATE \"POSIX\""),
            ("NULL", "NULL::boolean"),
            ("NULL", "NULL"),
        ] {
            let compiled =
                compile(&format!("SELECT {left} AS flag {operation} SELECT {right}"), &native)?;
            let column = &compiled.output().columns[0].column;
            if left == "NULL" && right == "NULL" {
                assert_eq!(column.oid, 25);
            } else {
                assert_eq!((column.oid, column.collation), (16, 0));
            }
        }
    }
    for sql in [
        "SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT true",
        "SELECT true UNION ALL SELECT NULL UNION ALL SELECT 'yes'",
        "SELECT true UNION ALL (SELECT NULL UNION ALL SELECT 'yes')",
        "SELECT 'yes' UNION ALL (SELECT NULL UNION ALL SELECT true)",
        "(SELECT 'yes' ORDER BY 1) UNION ALL SELECT true",
        "SELECT DISTINCT 'yes' UNION ALL SELECT true",
        "(VALUES (NULL)) UNION ALL SELECT true",
        "SELECT true UNION ALL SELECT COALESCE(NULL,NULL)",
        "SELECT true INTERSECT SELECT 'yes'::text",
        "SELECT true EXCEPT SELECT 'bad' WHERE false",
    ] {
        let valid = sql == "SELECT true UNION ALL SELECT NULL UNION ALL SELECT 'yes'"
            || sql == "SELECT 'yes' UNION ALL (SELECT NULL UNION ALL SELECT true)";
        assert_eq!(compile(sql, &native).is_ok(), valid, "{sql}");
    }
    Ok(())
}

#[test]
fn uuid_set_operands_resolve_unknowns_pairwise() -> Result<()> {
    let native = contract();
    let literal = "'{A0EEBC999C0B4EF8BB6D6BB9BD380A11}' COLLATE \"C\"";
    for operation in ["UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"] {
        for (left, right) in [(literal, "NULL::uuid"), ("NULL", "NULL::uuid")] {
            let sql = format!("SELECT {left} AS v {operation} SELECT {right}");
            let compiled = compile(&sql, &native)?;
            let column = &compiled.output().columns[0].column;
            assert_eq!((column.oid, column.collation), (2950, 0), "{sql}");
        }
    }
    for sql in [
        "SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT NULL::uuid",
        "SELECT NULL::uuid UNION ALL SELECT 'bad' WHERE false",
        "SELECT NULL::uuid UNION ALL SELECT NULL::text",
        "SELECT NULL::uuid UNION ALL SELECT true",
    ] {
        assert!(compile(sql, &native).is_err(), "{sql}");
    }
    Ok(())
}
