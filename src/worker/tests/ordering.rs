//! SQL ordering uses the retained occurrence selector and ordinary SQL boundaries.
use super::{History, RowState, batch, contract, row, settings};
use crate::{
    compiler::compile, compiler::relational::Node, engine::Batch, worker::program::relational,
};
use anyhow::Result;

#[test]
fn ordering_binds_aliases_ordinals_and_hidden_expressions() -> Result<()> {
    let native = contract();
    let base = "SELECT price AS p,id FROM source.bid";
    let first = compile(&format!("{base} ORDER BY p DESC NULLS LAST,2 LIMIT 3"), &native)?;
    for order in ["price DESC NULLS LAST,id", "p DESC NULLS LAST,2", "1 DESC NULLS LAST,2"] {
        let other = compile(&format!("{base} ORDER BY {order} LIMIT 3"), &native)?;
        assert_eq!(serde_json::to_vec(&first)?, serde_json::to_vec(&other)?);
    }
    for sql in [
        "SELECT auction FROM source.bid ORDER BY abs(price) DESC,id LIMIT 3",
        "SELECT auction,COUNT(*) AS n FROM source.bid GROUP BY auction ORDER BY n DESC,auction LIMIT 3",
        "SELECT auction FROM source.bid GROUP BY auction ORDER BY COUNT(*) DESC,auction LIMIT 3",
        "SELECT DISTINCT auction FROM source.bid ORDER BY auction DESC LIMIT 3",
        "SELECT DISTINCT abs(auction) AS a FROM source.bid ORDER BY abs(auction) LIMIT 3",
        "SELECT price FROM source.bid ORDER BY price,id LIMIT 1025",
        "SELECT price FROM source.bid ORDER BY price,id FETCH FIRST 3 ROWS ONLY",
    ] {
        let compiled = compile(sql, &native)?;
        assert!(
            compiled.relational().nodes.iter().any(|n| matches!(n, Node::TopK { .. })),
            "{sql}"
        );
        relational::build(&native, &compiled, &settings())?;
    }
    let plain = compile("SELECT price FROM source.bid ORDER BY id DESC", &native)?;
    assert!(!plain.relational().nodes.iter().any(|n| matches!(n, Node::TopK { .. })));
    for suffix in [
        "ORDER BY 0",
        "ORDER BY 3",
        "ORDER BY 'price'",
        "ORDER BY missing",
        "ORDER BY price LIMIT -1",
        "ORDER BY price LIMIT 1000001",
        "ORDER BY price LIMIT NULL",
        "ORDER BY price LIMIT 1+1",
        "ORDER BY price LIMIT 1 OFFSET 1",
        "ORDER BY price USING >",
        "ORDER BY price FETCH FIRST 1 ROW WITH TIES",
        "LIMIT 1",
    ] {
        assert!(compile(&format!("{base} {suffix}"), &native).is_err(), "{suffix}");
    }
    for sql in [
        "SELECT id AS p,price AS p FROM source.bid ORDER BY p LIMIT 3",
        "SELECT price AS p FROM source.bid ORDER BY p+1 LIMIT 3",
        "SELECT DISTINCT auction FROM source.bid ORDER BY price LIMIT 3",
        "SELECT category FROM source.auction ORDER BY category LIMIT 3",
        "SELECT price FROM source.bid ORDER BY price LIMIT 3",
    ] {
        assert!(compile(sql, &native).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn ordering_preserves_derived_admission_boundaries() {
    for query in [
        "SELECT DISTINCT id FROM source.bid",
        "SELECT b.id FROM source.bid b WHERE EXISTS(SELECT 1 FROM source.auction a WHERE a.id=b.auction)",
    ] {
        for suffix in ["", " ORDER BY id", " ORDER BY id LIMIT 3"] {
            let sql = format!("SELECT q.id FROM ({query}{suffix}) q");
            assert!(compile(&sql, &contract()).is_err(), "{sql}");
        }
    }
}

fn oracle(state: &RowState, limit: Option<usize>) -> Result<super::RowBatch> {
    let mut rows = Vec::new();
    for (row, weight) in state {
        for _ in 0..*weight {
            let price = row["price"].as_deref().map(str::parse::<i64>).transpose()?;
            let id = row["id"].as_deref().map(str::parse::<i64>).transpose()?;
            rows.push((price, id, row));
        }
    }
    rows.sort_by_key(|(price, id, _)| (price.is_none(), price.map(std::cmp::Reverse), *id));
    Batch::from_updates(rows.into_iter().take(limit.unwrap_or(usize::MAX)).map(|(_, _, row)| {
        ((relational::Key::new(), [("0:auction".into(), row["auction"].clone())].into()), 1)
    }))
}

#[tokio::test]
async fn global_ordering_clips_weighted_bags_and_refills_after_cold_restart() -> Result<()> {
    let native = contract();
    let options = settings();
    for limit in [Some(0), Some(3), Some(100), None] {
        let suffix = limit.map_or_else(String::new, |limit| format!(" LIMIT {limit}"));
        let compiled = compile(
            &format!("SELECT auction FROM source.bid ORDER BY price DESC NULLS LAST,id{suffix}"),
            &native,
        )?;
        let mut history = History::new((&native, &compiled, &options));
        history.snapshot = true;
        history.compact = true;
        history
            .run(super::peer_changes(), |state| oracle(state, limit), super::apply_rows, |_| true)
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn nested_limits_preserve_scope_and_filter_placement() -> Result<()> {
    let native = contract();
    let options = settings();
    let compiled = compile(
        "SELECT q.auction FROM (SELECT auction,price,id FROM source.bid ORDER BY price DESC NULLS LAST,id LIMIT 3) q WHERE q.price IS NOT NULL ORDER BY q.id LIMIT 1",
        &native,
    )?;
    let a = row(&[("id", Some("1")), ("auction", Some("10")), ("price", Some("1"))]);
    let b = row(&[("id", Some("2")), ("auction", Some("20")), ("price", Some("8"))]);
    let c = row(&[("id", Some("3")), ("auction", Some("30")), ("price", Some("9"))]);
    let d = row(&[("id", Some("4")), ("auction", Some("40")), ("price", Some("10"))]);
    let mut query = relational::build(&native, &compiled, &options)?;
    for (time, changes, expected) in [
        (
            1,
            batch(vec![
                ("bid", a.clone(), 1),
                ("bid", b.clone(), 1),
                ("bid", c, 1),
                ("bid", d.clone(), 1),
            ]),
            vec![("20", 1)],
        ),
        (2, batch(vec![("bid", d, -1)]), vec![("20", -1), ("10", 1)]),
        (3, batch(vec![("bid", a, -1)]), vec![("10", -1), ("20", 1)]),
    ] {
        let prepared =
            query.prepare(relational::inputs(&changes, compiled.relational(), time)?).await?;
        assert_eq!(
            prepared.output().batch,
            Batch::from_updates(expected.into_iter().map(|(value, weight)| {
                (
                    (relational::Key::new(), [("0:auction".into(), Some(value.into()))].into()),
                    weight,
                )
            }))?
        );
        query.commit(prepared)?;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
    }
    Ok(())
}
