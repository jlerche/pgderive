//! SQL ON residuals determine matches independently for every full left tuple.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::Result;
fn oracle(state: &super::JoinedState, outer: bool) -> Result<Batch<relational::Key, Row>> {
    let mut updates = Vec::new();
    for (a, aw) in state.0.iter().filter(|(_, weight)| **weight > 0) {
        let mut matched = false;
        for (b, bw) in state.1.iter().filter(|(_, weight)| **weight > 0) {
            let passes = a["other"].is_some()
                && a["other"] == b["auction"]
                && b["price"].as_deref().map(str::parse::<i64>).transpose()?.is_some_and(|price| {
                    price
                        > a["id"]
                            .as_deref()
                            .and_then(|id| id.parse::<i64>().ok())
                            .unwrap_or(i64::MAX)
                });
            if passes {
                matched = true;
                updates.push((
                    (
                        relational::Key::new(),
                        row(&[
                            ("0:category", a["category"].as_deref()),
                            ("1:price", b["price"].as_deref()),
                        ]),
                    ),
                    aw * bw,
                ));
            }
        }
        if outer && !matched {
            updates.push((
                (
                    relational::Key::new(),
                    row(&[("0:category", a["category"].as_deref()), ("1:price", None)]),
                ),
                *aw,
            ));
        }
    }
    Batch::from_updates(updates)
}
#[tokio::test]
async fn residual_matches_retract_null_extensions_per_left_identity_and_restart() -> Result<()> {
    let mut native = contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    let a = row(&[("id", Some("1")), ("category", Some("same")), ("other", Some("7"))]);
    let other = row(&[("id", Some("5")), ("category", Some("same")), ("other", Some("7"))]);
    let null = row(&[("id", Some("2")), ("category", Some("null")), ("other", None)]);
    let b = row(&[("id", Some("1")), ("auction", Some("7")), ("price", Some("3"))]);
    let changed = row(&[("id", Some("1")), ("auction", Some("7")), ("price", Some("9"))]);
    let moved = row(&[("id", Some("10")), ("category", Some("same")), ("other", Some("7"))]);
    let history = vec![
        batch(vec![
            ("auction", a.clone(), 2),
            ("auction", other.clone(), 1),
            ("auction", null.clone(), 1),
            ("bid", b.clone(), 3),
        ]),
        batch(vec![
            ("bid", b.clone(), -3),
            ("bid", changed.clone(), 2),
            ("auction", other.clone(), -1),
            ("auction", moved.clone(), 1),
        ]),
        batch(vec![("bid", changed.clone(), -2)]),
        batch(vec![("bid", b.clone(), 1)]),
        batch(vec![
            ("auction", a.clone(), -2),
            ("auction", moved.clone(), -1),
            ("auction", null.clone(), -1),
            ("bid", b.clone(), -1),
        ]),
    ];
    for outer in [false, true] {
        let kind = if outer { "LEFT JOIN" } else { "JOIN" };
        let compiled = compile(
            &format!(
                "SELECT a.category,b.price FROM source.auction a {kind} source.bid b ON a.other=b.auction AND b.price>a.id"
            ),
            &native,
        )?;
        let options = settings();
        let mut runner = super::History::new((&native, &compiled, &options));
        runner.scanned = Some(relational::build_scanned(&native, &compiled, &options)?);
        runner.compact = true;
        runner
            .run(history.clone(), |state| oracle(state, outer), super::apply_joined, |_| true)
            .await?;
    }
    Ok(())
}
#[test]
fn native_join_collations_and_computed_derived_text_are_proven() -> Result<()> {
    let mut native = contract();
    native.relations[0].columns[1].collation = 950;
    native.relations[0].columns[2].collation = 950;
    for condition in [
        "a.category=a.other",
        "NULL=1",
        "NULL=NULL",
        "1=1",
        "chr(a.id)=chr(b.id)",
        "to_hex(a.id)=to_hex(b.id)",
        "replace(a.category,'x','y')=a.other",
        "a.category>a.other",
        "a.category<>a.other OR a.category IS NULL",
    ] {
        compile(
            &format!("SELECT a.id FROM source.auction a JOIN source.bid b ON {condition}"),
            &native,
        )?
        .validate_execution()?;
    }
    for condition in ["a.k=z.other", "a.k>z.other"] {
        compile(&format!("SELECT a.id FROM (SELECT id,replace(category,'x','y') AS k FROM source.auction) a LEFT JOIN source.auction z ON {condition}"), &native)?.validate_execution()?;
    }
    native.relations[0].columns[2].collation = 951;
    assert!(
        compile(
            "SELECT a.id FROM source.auction a JOIN source.auction z ON a.category=z.other",
            &native
        )
        .is_err()
    );
    assert!(compile("SELECT d.id FROM (SELECT id,CASE WHEN id>0 THEN category ELSE other END AS k FROM source.auction) d LEFT JOIN source.auction z ON d.k=z.category OR d.id=z.id", &native).is_err());
    for wrapped in
        ["replace(d.k,'x','y')", "COALESCE(d.k,'x')", "CASE WHEN d.id>0 THEN d.k ELSE 'x' END"]
    {
        assert!(compile(&format!("SELECT d.id FROM (SELECT id,CASE WHEN id>0 THEN category ELSE other END AS k FROM source.auction) d LEFT JOIN source.auction z ON {wrapped}=z.category OR d.id=z.id"), &native).is_err());
    }
    native.relations[0].columns[2].collation = 100;
    native.relations[0].columns[1].collation = 100;
    assert!(
        compile(
            "SELECT a.id FROM source.auction a LEFT JOIN source.auction z ON a.category>z.other",
            &native
        )
        .is_err()
    );
    compile(
        "SELECT a.id FROM source.auction a JOIN source.auction z ON a.category=z.other",
        &native,
    )?
    .validate_execution()?;
    Ok(())
}
