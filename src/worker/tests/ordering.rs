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
        "SELECT price FROM source.bid ORDER BY price LIMIT 3",
        "SELECT price FROM source.bid ORDER BY price LIMIT 3 OFFSET 2",
        "SELECT price FROM source.bid ORDER BY price OFFSET 2",
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
        "ORDER BY price LIMIT 1 OFFSET -1",
        "ORDER BY price LIMIT 1000000 OFFSET 1",
        "ORDER BY price OFFSET 1000001",
        "ORDER BY price OFFSET NULL",
        "ORDER BY price COLLATE \"C\"",
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

#[test]
fn native_order_domains_and_window_proofs_remain_explicit() -> Result<()> {
    for oid in [16, 1700, 2950, 25, 1043] {
        let mut native = contract();
        native.relations[1].columns[2].oid = oid;
        native.relations[1].columns[2].collation = if matches!(oid, 25 | 1043) { 950 } else { 0 };
        let compiled =
            compile("SELECT price FROM source.bid ORDER BY price DESC LIMIT 3 OFFSET 2", &native)?;
        relational::build(&native, &compiled, &settings())?;
    }
    let native = contract();
    for sql in [
        "SELECT category FROM source.auction ORDER BY category COLLATE pg_catalog.\"C\" LIMIT 3",
        "SELECT category FROM source.auction ORDER BY category COLLATE \"POSIX\" OFFSET 1",
        "SELECT reverse(category) FROM source.auction ORDER BY reverse(category) COLLATE \"C\" LIMIT 3",
    ] {
        compile(sql, &native)?;
    }
    assert!(
        compile("SELECT id,ROW_NUMBER() OVER(ORDER BY price) FROM source.bid", &native).is_err()
    );
    assert!(
        compile(
            "SELECT category FROM source.auction ORDER BY category COLLATE \"en_US\" LIMIT 3",
            &native
        )
        .is_err()
    );
    assert!(
        compile(
            "SELECT DISTINCT category FROM source.auction ORDER BY category COLLATE \"C\" LIMIT 3",
            &native
        )
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn plain_order_ties_and_offsets_count_weighted_occurrences() -> Result<()> {
    let native = contract();
    let options = settings();
    for (offset, limit) in [(0, Some(3)), (2, Some(3)), (3, None), (1_000_000, None), (0, None)] {
        let suffix = limit.map_or_else(String::new, |n| format!(" LIMIT {n}"));
        let compiled = compile(
            &format!(
                "SELECT auction,price FROM source.bid ORDER BY price DESC NULLS LAST{suffix} OFFSET {offset}"
            ),
            &native,
        )?;
        let mut history = History::new((&native, &compiled, &options));
        history.compact = true;
        history
            .run(
                super::peer_changes(),
                |state| offset_oracle(state, offset, limit),
                super::apply_rows,
                |_| true,
            )
            .await?;
    }
    Ok(())
}

fn offset_oracle(state: &RowState, offset: usize, limit: Option<usize>) -> Result<super::RowBatch> {
    let mut rows = Vec::new();
    for (row, weight) in state {
        let price = row["price"].as_deref().map(str::parse::<i64>).transpose()?;
        let auction = row["auction"].as_deref().map(str::parse::<i64>).transpose()?;
        for _ in 0..*weight {
            rows.push((price, auction, row));
        }
    }
    rows.sort_by_key(|(p, a, _)| (p.is_none(), p.map(std::cmp::Reverse), *a));
    Batch::from_updates(rows.into_iter().skip(offset).take(limit.unwrap_or(usize::MAX)).map(
        |(_, _, row)| {
            (
                (
                    relational::Key::new(),
                    [
                        ("0:auction".into(), row["auction"].clone()),
                        ("0:price".into(), row["price"].clone()),
                    ]
                    .into(),
                ),
                1,
            )
        },
    ))
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

#[tokio::test]
async fn zero_limits_prune_row_errors_and_state_after_binding() -> Result<()> {
    let native = contract();
    let options = settings();
    for sql in [
        "SELECT abs(price) AS p FROM source.bid ORDER BY id LIMIT 0",
        "SELECT auction FROM source.bid ORDER BY abs(price),id LIMIT 0",
        "SELECT q.p FROM (SELECT abs(price) AS p FROM source.bid ORDER BY id LIMIT 0) q ORDER BY q.p LIMIT 1",
        "SELECT abs(b.price) AS p FROM source.auction a JOIN source.bid b ON a.id=b.auction ORDER BY a.id,b.id LIMIT 0",
        "SELECT id,COUNT(abs(price)) OVER(ORDER BY id ROWS UNBOUNDED PRECEDING) AS p FROM source.bid ORDER BY id LIMIT 0",
    ] {
        let compiled = compile(sql, &native)?;
        assert!(
            !compiled.relational().nodes.iter().any(|n| matches!(
                n,
                Node::Map { .. }
                    | Node::Join { .. }
                    | Node::Partition { .. }
                    | Node::Statistics { .. }
            )),
            "{sql}"
        );
        let a = row(&[("id", Some("1")), ("category", None), ("other", None)]);
        let b = row(&[
            ("id", Some("1")),
            ("auction", Some("1")),
            ("price", Some("-9223372036854775808")),
        ]);
        let query = History::new((&native, &compiled, &options))
            .run(
                [
                    batch(vec![("auction", a.clone(), 1), ("bid", b.clone(), 2)]),
                    batch(vec![("bid", b, -2), ("auction", a, -1)]),
                ],
                |_: &super::JoinedState| Batch::from_updates([]),
                super::apply_joined,
                |_| true,
            )
            .await?;
        assert_eq!(query.checkpoint()?.arrangements.len(), 1, "{sql}");
    }
    for sql in [
        "SELECT abs(missing) FROM source.bid ORDER BY id LIMIT 0",
        "SELECT category FROM source.auction ORDER BY category LIMIT 0",
        "SELECT abs(category) FROM source.auction ORDER BY id LIMIT 0",
    ] {
        assert!(compile(sql, &native).is_err(), "{sql}");
    }
    Ok(())
}
