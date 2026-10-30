//! Full-tuple SQL set multiplicities across synchronized sources and cold roots.
use super::{History, JoinedState, batch, contract, row, settings};
use crate::{
    compiler::{Compiled, compile},
    engine::Batch,
    transaction::Row,
    worker::program::relational,
};
use anyhow::Result;
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
