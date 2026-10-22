use super::{contract, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::Result;
use std::collections::BTreeMap;
type Bag = Batch<relational::Key, Row>;
const PEERS: &str = "SELECT b.id,rank() OVER(PARTITION BY b.auction ORDER BY b.price DESC NULLS FIRST) AS r,dense_rank() OVER(PARTITION BY b.auction ORDER BY b.price DESC NULLS FIRST) AS d FROM source.bid b";
const NUMBER: &str = "SELECT b.id,row_number() OVER(PARTITION BY b.auction ORDER BY b.price DESC NULLS FIRST,b.id) AS r FROM source.bid b";
fn oracle(state: &BTreeMap<Row, i64>, numbered: bool) -> Result<Bag> {
    let mut groups: BTreeMap<Option<String>, Vec<Row>> = BTreeMap::new();
    for (row, weight) in state {
        for _ in 0..*weight {
            groups.entry(row["auction"].clone()).or_default().push(row.clone());
        }
    }
    let mut output = Vec::new();
    for mut rows in groups.into_values() {
        let price = |row: &Row| row["price"].as_ref().and_then(|value| value.parse::<i64>().ok());
        rows.sort_by_key(|row| {
            (
                price(row).is_some(),
                price(row).map(std::cmp::Reverse),
                row["id"].as_ref().and_then(|value| value.parse::<i64>().ok()),
            )
        });
        let mut distinct = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            let preceding = rows.iter().take_while(|prior| price(prior) != price(row)).count();
            if !distinct.contains(&price(row)) {
                distinct.push(price(row));
            }
            let mut result: Row = [
                ("0:id".into(), row["id"].clone()),
                (
                    "@aggregate_0".into(),
                    Some(if numbered { index + 1 } else { preceding + 1 }.to_string()),
                ),
            ]
            .into();
            if !numbered {
                result.insert("@aggregate_1".into(), Some(distinct.len().to_string()));
            }
            output.push(((crate::worker::program::relational::Key::new(), result), 1));
        }
    }
    Batch::from_updates(output)
}
async fn history(numbered: bool) -> Result<()> {
    let native = contract();
    let sql = if numbered { NUMBER } else { PEERS };
    let compiled = compile(sql, &native)?;
    let options = settings();
    super::check_row_history(
        (&native, &compiled, &options),
        super::peer_changes(),
        |state| oracle(state, numbered),
        |index| index == 1,
    )
    .await?;
    Ok(())
}
#[tokio::test]
async fn weighted_peer_ranks_revise_and_restart() -> Result<()> {
    history(false).await
}
#[tokio::test]
async fn weighted_row_numbers_revise_and_restart() -> Result<()> {
    history(true).await
}
#[test]
fn rejects_unsupported_ranking_and_binds_order() -> Result<()> {
    let native = contract();
    for sql in [
        NUMBER.replace(",b.id", ""),
        PEERS.replace("rank()", "rank(b.id)"),
        PEERS.replace("rank() OVER", "rank() FILTER(WHERE b.price>0) OVER"),
        PEERS.replace("NULLS FIRST)", "NULLS FIRST ROWS UNBOUNDED PRECEDING)"),
        PEERS.replace("rank()", "ntile()"),
    ] {
        assert!(compile(&sql, &native).is_err(), "{sql}");
    }
    let empty_order = compile(&PEERS.replace(" ORDER BY b.price DESC NULLS FIRST", ""), &native)?;
    assert!(!empty_order.relational().nodes.is_empty());
    assert_ne!(
        serde_json::to_vec(&compile(PEERS, &native)?)?,
        serde_json::to_vec(&compile(&PEERS.replace("DESC", "ASC"), &native)?)?
    );
    Ok(())
}
