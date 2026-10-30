//! Outer joins retain both unmatched sides and full weighted identities.
use super::{History, JoinedState, batch, contract, row, settings};
use crate::{
    compiler::{Compiled, compile},
    engine::Batch,
    source::Contract,
    transaction::Row,
    worker::program::relational,
};
use anyhow::Result;
fn native() -> Contract {
    let mut native = contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    native
}
fn values(left: Option<&Row>, right: Option<&Row>) -> Vec<Option<String>> {
    vec![
        left.and_then(|row| row["id"].clone()),
        right.and_then(|row| row["id"].clone()),
        left.and_then(|row| row["other"].clone()),
        right.and_then(|row| row["auction"].clone()),
        right.and_then(|row| row["price"].clone()),
    ]
}
fn matches(left: &Row, right: &Row, condition: &str) -> Result<bool> {
    let key = left["other"].is_some() && left["other"] == right["auction"];
    Ok(match condition {
        "TRUE" => true,
        "FALSE" | "NULL" => false,
        "a.id<b.price" => right["price"]
            .as_deref()
            .map(str::parse::<i64>)
            .transpose()?
            .zip(left["id"].as_deref().map(str::parse::<i64>).transpose()?)
            .is_some_and(|(price, id)| id < price),
        "a.other=b.auction" => key,
        _ => {
            key && right["price"]
                .as_deref()
                .map(str::parse::<i64>)
                .transpose()?
                .zip(left["id"].as_deref().map(str::parse::<i64>).transpose()?)
                .is_some_and(|(price, id)| price > id)
        }
    })
}
fn oracle(
    compiled: &Compiled,
    state: &JoinedState,
    kind: &str,
    condition: &str,
) -> Result<super::RowBatch> {
    let mut rows = Vec::new();
    for (left, weight) in state.0.iter().filter(|(_, weight)| **weight > 0) {
        let mut present = false;
        for (right, count) in state.1.iter().filter(|(_, weight)| **weight > 0) {
            if matches(left, right, condition)? {
                present = true;
                rows.push((values(Some(left), Some(right)), weight * count));
            }
        }
        if matches!(kind, "LEFT" | "FULL") && !present {
            rows.push((values(Some(left), None), *weight));
        }
    }
    for (right, weight) in state.1.iter().filter(|(_, weight)| **weight > 0) {
        let mut present = false;
        for (left, _) in state.0.iter().filter(|(_, weight)| **weight > 0) {
            present |= matches(left, right, condition)?;
        }
        if matches!(kind, "RIGHT" | "FULL") && !present {
            rows.push((values(None, Some(right)), *weight));
        }
    }
    Batch::from_updates(rows.into_iter().map(|(mut values, weight)| {
        if compiled.output().columns.len() == 6 {
            values.insert(0, values[2].clone().or_else(|| values[3].clone()));
        }
        (
            (
                relational::Key::new(),
                compiled
                    .output()
                    .columns
                    .iter()
                    .map(|column| column.column.name.clone())
                    .zip(values)
                    .collect::<Row>(),
            ),
            weight,
        )
    }))
}
fn changes() -> Vec<crate::weighted::Batch> {
    let left = |id, key| row(&[("id", Some(id)), ("category", None), ("other", key)]);
    let right = |id, key, price| row(&[("id", Some(id)), ("auction", key), ("price", price)]);
    let first = left("1", Some("7"));
    let second = left("5", Some("7"));
    let null_left = left("8", None);
    let matched = right("1", Some("7"), Some("3"));
    let changed = right("1", Some("7"), Some("9"));
    let null_right = right("2", None, None);
    let unmatched = right("3", Some("9"), Some("1"));
    vec![
        batch(vec![
            ("auction", first.clone(), 2),
            ("auction", second.clone(), 1),
            ("auction", null_left.clone(), 1),
            ("bid", matched.clone(), 3),
            ("bid", null_right.clone(), 2),
            ("bid", unmatched.clone(), 1),
        ]),
        batch(vec![
            ("auction", second.clone(), -1),
            ("bid", matched.clone(), -3),
            ("bid", changed.clone(), 2),
        ]),
        batch(vec![("bid", changed, -2)]),
        batch(vec![("bid", matched.clone(), 1)]),
        batch(vec![("auction", first, -2), ("auction", null_left, -1)]),
        batch(vec![("bid", matched, -1), ("bid", null_right, -2), ("bid", unmatched, -1)]),
        batch(vec![("auction", second, 1)]),
    ]
}
#[tokio::test]
async fn right_and_full_signed_histories_match_oracle_and_scanned_transform() -> Result<()> {
    let native = native();
    for kind in ["RIGHT", "FULL"] {
        for condition in [
            "a.other=b.auction",
            "a.other=b.auction AND b.price>a.id",
            "TRUE",
            "FALSE",
            "NULL",
            "a.id<b.price",
        ] {
            if kind == "FULL" && condition == "a.id<b.price" {
                continue;
            }
            let compiled = compile(
                &format!(
                    "SELECT a.id AS aid,b.id AS bid,a.other AS ak,b.auction AS bk,b.price AS p FROM source.auction a {kind} JOIN source.bid b ON {condition}"
                ),
                &native,
            )?;
            let options = settings();
            let mut history = History::new((&native, &compiled, &options));
            history.compact = true;
            history.scanned = Some(relational::build_scanned(&native, &compiled, &options)?);
            history
                .run(
                    changes(),
                    |state| oracle(&compiled, state, kind, condition),
                    super::apply_joined,
                    |_| true,
                )
                .await?;
        }
    }
    Ok(())
}
#[test]
fn using_scope_hides_only_unqualified_originals_and_keeps_nested_physical_fields() -> Result<()> {
    let native = native();
    for kind in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"] {
        compile(&format!("SELECT k,l.k,r.k FROM(SELECT other AS k FROM source.auction)l {kind}(SELECT auction AS k FROM source.bid)r USING(k)"), &native)?.validate_execution()?;
    }
    for sql in [
        "SELECT k,a.k,b.k,c.k FROM(SELECT other AS k FROM source.auction)a FULL JOIN(SELECT auction AS k FROM source.bid)b USING(k) FULL JOIN(SELECT auction AS k FROM source.bid)c USING(k)",
        "SELECT k,a.k,b.k,c.k,d.k FROM((SELECT other AS k FROM source.auction)a JOIN(SELECT auction AS k FROM source.bid)b USING(k)) FULL JOIN((SELECT auction AS k FROM source.bid)c JOIN(SELECT other AS k FROM source.auction)d USING(k)) USING(k)",
    ] {
        compile(sql, &native)?.validate_execution()?;
    }
    for sql in [
        "SELECT id FROM source.auction a FULL JOIN source.bid b ON a.id=b.id",
        "SELECT id FROM source.auction a FULL JOIN source.bid b USING(missing)",
        "SELECT id FROM source.auction a FULL JOIN source.bid b USING(id,id)",
        "SELECT id FROM source.auction a NATURAL FULL JOIN source.bid b",
    ] {
        assert!(compile(sql, &native).is_err(), "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn using_merged_and_qualified_values_match_all_join_kinds_through_restart() -> Result<()> {
    let native = native();
    for kind in ["INNER", "LEFT", "RIGHT", "FULL"] {
        let compiled = compile(
            &format!(
                "SELECT k AS merged,l.aid AS aid,r.bid AS bid,l.k AS ak,r.k AS bk,r.p AS p FROM(SELECT id AS aid,other AS k FROM source.auction)l {kind} JOIN(SELECT id AS bid,auction AS k,price AS p FROM source.bid)r USING(k)"
            ),
            &native,
        )?;
        let options = settings();
        let mut history = History::new((&native, &compiled, &options));
        history.compact = true;
        history
            .run(
                changes(),
                |state| oracle(&compiled, state, kind, "a.other=b.auction"),
                super::apply_joined,
                |_| true,
            )
            .await?;
    }
    Ok(())
}

#[test]
fn full_join_admission_rejects_keyless_nonconstant_numeric_conditions() -> Result<()> {
    let native = native();
    for condition in ["a.id<b.id", "a.id>0", "a.id=b.id OR a.other=b.auction"] {
        assert!(
            compile(
                &format!(
                    "SELECT a.id,b.id FROM source.auction a FULL JOIN source.bid b ON {condition}"
                ),
                &native
            )
            .is_err(),
            "{condition}"
        );
    }
    for condition in ["TRUE", "FALSE", "NULL", "1=1", "a.id=b.id AND a.other<b.auction"] {
        compile(
            &format!(
                "SELECT a.id,b.id FROM source.auction a FULL JOIN source.bid b ON {condition}"
            ),
            &native,
        )?
        .validate_execution()?;
    }
    compile(
        "SELECT a.id,b.id FROM source.auction a RIGHT JOIN source.bid b ON a.id<b.id",
        &native,
    )?
    .validate_execution()?;
    Ok(())
}
