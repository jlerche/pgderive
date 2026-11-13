//! Unordered SQL selection uses the same complete logical-tuple occurrence order.
use super::{History, RowState, contract, settings};
use crate::{compiler::compile, engine::Batch, worker::program::relational};
use anyhow::{Context, Result};
use pg_query::NodeEnum;

fn limit_node(suffix: &str) -> Result<pg_query::Node> {
    let parsed = pg_query::parse(&format!("SELECT price FROM source.bid {suffix}"))?;
    let statement = parsed.protobuf.stmts.into_iter().next().context("missing statement")?;
    let Some(NodeEnum::SelectStmt(select)) = statement.stmt.and_then(|node| node.node) else {
        anyhow::bail!("missing SELECT")
    };
    select.limit_count.map(|value| *value).context("missing count")
}

#[test]
fn postgres_all_and_null_counts_share_one_native_representation() -> Result<()> {
    let all = limit_node("LIMIT ALL")?;
    let null = limit_node("LIMIT NULL")?;
    assert_eq!(all, null);
    assert!(matches!(all.node,Some(NodeEnum::AConst(value)) if value.isnull));
    let native = contract();
    let plain = compile("SELECT auction,price FROM source.bid", &native)?;
    for suffix in ["LIMIT ALL", "LIMIT NULL", "OFFSET 0", "OFFSET NULL", "LIMIT ALL OFFSET NULL"] {
        let compiled = compile(&format!("SELECT auction,price FROM source.bid {suffix}"), &native)?;
        assert_eq!(serde_json::to_value(&compiled)?, serde_json::to_value(&plain)?);
    }
    for suffix in [
        "LIMIT -1",
        "OFFSET -1",
        "LIMIT 1000000 OFFSET 1",
        "LIMIT 1000001",
        "OFFSET 1000001",
        "LIMIT 1+1",
    ] {
        assert!(compile(&format!("SELECT price FROM source.bid {suffix}"), &native).is_err());
    }
    compile("SELECT category FROM source.auction LIMIT 3 OFFSET 1", &native)?;
    Ok(())
}

fn oracle(state: &RowState, offset: usize, limit: Option<usize>) -> Result<super::RowBatch> {
    let mut rows = Vec::new();
    for (row, weight) in state {
        let auction = row["auction"].as_deref().map(str::parse::<i64>).transpose()?;
        let price = row["price"].as_deref().map(str::parse::<i64>).transpose()?;
        rows.extend(std::iter::repeat_n((auction, price, row), usize::try_from(*weight)?));
    }
    rows.sort_by_key(|(auction, price, _)| (*auction, *price));
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

#[tokio::test]
async fn unordered_counts_clip_duplicate_weights_and_restore_exactly() -> Result<()> {
    let native = contract();
    let options = settings();
    for (suffix, offset, limit) in [
        ("LIMIT 3", 0, Some(3)),
        ("OFFSET 2", 2, None),
        ("LIMIT 3 OFFSET 2", 2, Some(3)),
        ("LIMIT 0 OFFSET 1000000", 1_000_000, Some(0)),
        ("LIMIT ALL", 0, None),
        ("LIMIT NULL", 0, None),
        ("LIMIT ALL OFFSET 2", 2, None),
        ("LIMIT NULL OFFSET 2", 2, None),
        ("OFFSET NULL", 0, None),
    ] {
        let compiled = compile(&format!("SELECT auction,price FROM source.bid {suffix}"), &native)?;
        let mut history = History::new((&native, &compiled, &options));
        history.compact = true;
        history.snapshot = true;
        history
            .run(
                super::peer_changes(),
                |state| oracle(state, offset, limit),
                super::apply_rows,
                |_| true,
            )
            .await?;
    }
    Ok(())
}
