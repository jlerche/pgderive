//! `PostgreSQL` q13 requires a typed modulo key before predecessor selection.
use super::contract;
use crate::{compiler::compile, source::Column};
use anyhow::{Context, Result};

fn native(input: u32, key: u32) -> crate::source::Contract {
    let mut native = contract();
    native.relations[0].table = "side_input".into();
    native.relations[0].columns[1].name = "value".into();
    native.relations[0].columns[2].name = "key".into();
    native.relations[0].columns[2].oid = key;
    native.relations[0].columns[2].collation = 0;
    native.relations[1].columns[1].oid = input;
    let column = |position, name: &str, oid| Column {
        position,
        name: name.into(),
        oid,
        modifier: -1,
        nullable: true,
        primary: false,
        collation: 0,
    };
    native.relations[0].columns.push(column(4, "date_time", 1184));
    native.relations[1].columns.extend([
        column(4, "bidder", 20),
        column(5, "date_time", 1184),
        column(6, "processing_time", 1184),
    ]);
    native
}
#[test]
fn q13_modulo_keys_use_postgres_integer_promotion() -> Result<()> {
    let sql = include_str!("../../../queries/nexmark/q13.sql").replace("nexmark.", "source.");
    for (input, key, divisor) in
        [(21, 23, "10000"), (23, 23, "10000"), (20, 20, "10000"), (23, 20, "2147483648")]
    {
        let native = native(input, key);
        let sql = sql.replace("10000", divisor);
        let compiled = compile(&sql, &native)?;
        assert!(!compiled.relational().nodes.is_empty());
        let equivalent = compile(
            &sql.replace(&format!("mod(b.auction,{divisor})"), &format!("b.auction%{divisor}")),
            &native,
        )?;
        assert_eq!(serde_json::to_vec(&compiled)?, serde_json::to_vec(&equivalent)?);
    }
    Ok(())
}

type State = std::collections::BTreeMap<(String, crate::transaction::Row), i64>;
fn oracle(
    state: &State,
) -> Result<crate::engine::Batch<crate::worker::program::relational::Key, crate::transaction::Row>>
{
    let number = |row: &crate::transaction::Row, name: &str| {
        row[name].as_ref().map(|value| value.parse::<i128>()).transpose()
    };
    let mut output = Vec::new();
    for ((table, left), weight) in state {
        if table != "bid" || *weight <= 0 {
            continue;
        }
        // Quotient/subtraction independently defines truncation-toward-zero remainder.
        let key = number(left, "auction")?.map(|value| value - value / 10_000 * 10_000);
        let mut chosen: Option<(&crate::transaction::Row, i128)> = None;
        for ((table, right), weight) in state {
            if table != "side_input" || *weight <= 0 {
                continue;
            }
            let eligible = key.is_some()
                && key == number(right, "key")?
                && left["processing_time"]
                    .as_ref()
                    .zip(right["date_time"].as_ref())
                    .is_some_and(|(left, right)| right <= left);
            let id = number(right, "id")?.context("missing side identity")?;
            if eligible
                && chosen.is_none_or(|(prior, prior_id)| {
                    (&right["date_time"], id) > (&prior["date_time"], prior_id)
                })
            {
                chosen = Some((right, id));
            }
        }
        let value = chosen.and_then(|(right, _)| right["value"].as_deref());
        output.push((
            (
                crate::worker::program::relational::Key::new(),
                super::row(&[("0:auction", left["auction"].as_deref()), ("1:value", value)]),
            ),
            *weight,
        ));
    }
    crate::engine::Batch::from_updates(output)
}
fn changes() -> Vec<crate::weighted::Batch> {
    use super::{batch, row};
    let bid = |id, key, time| {
        row(&[
            ("id", Some(id)),
            ("auction", key),
            ("price", None),
            ("bidder", None),
            ("date_time", None),
            ("processing_time", time),
        ])
    };
    let side = |id, key, time, value| {
        row(&[("id", Some(id)), ("key", key), ("date_time", time), ("value", value)])
    };
    let ten = Some("2000-01-01 00:00:10+00");
    let eight = Some("2000-01-01 00:00:08+00");
    let probe = bid("1", Some("10001"), ten);
    let earlier_probe = bid("1", Some("10001"), eight);
    let earlier = side("1", Some("1"), Some("2000-01-01 00:00:09+00"), Some("earlier"));
    let null = side("2", Some("1"), ten, None);
    let tie = side("3", Some("1"), ten, Some("tie"));
    vec![
        batch(vec![
            ("bid", probe.clone(), 2),
            ("bid", bid("2", Some("-10001"), ten), 1),
            ("bid", bid("3", None, ten), 1),
            ("bid", bid("4", Some("10001"), None), 1),
            ("side_input", earlier.clone(), 3),
            ("side_input", side("4", Some("-1"), ten, Some("negative")), 1),
        ]),
        batch(vec![
            ("side_input", earlier, -1),
            ("side_input", null.clone(), 1),
            ("side_input", tie.clone(), 1),
        ]),
        batch(vec![
            ("bid", probe.clone(), -2),
            ("bid", earlier_probe.clone(), 2),
            ("side_input", side("5", Some("1"), eight, Some("simultaneous")), 1),
        ]),
        batch(vec![("bid", earlier_probe, -2), ("bid", probe, 2), ("side_input", tie, -1)]),
        batch(vec![("side_input", null, -1)]),
        batch(vec![]),
    ]
}
#[tokio::test]
async fn modulo_predecessor_matches_independent_signed_bags_and_restart() -> Result<()> {
    use super::settings;
    use crate::worker::program::relational;
    let sql =
        include_str!("../../../queries/nexmark/q13.sql").replace("nexmark.", "source.").replace(
            "SELECT b.auction,b.bidder,b.price,b.date_time,p.value",
            "SELECT b.auction,p.value",
        );
    let native = native(23, 23);
    let compiled = compile(&sql, &native)?;
    let ir = compiled.relational();
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let mut state = State::new();
    for (index, changes) in changes().into_iter().enumerate() {
        let before = oracle(&state)?;
        for update in &changes.updates {
            *state.entry((update.tuple.table.clone(), update.tuple.row.clone())).or_default() +=
                update.weight;
        }
        let after = oracle(&state)?;
        let expected = super::bag_delta(&before, &after)?;
        let prepared = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(prepared.output().batch, expected);
        query.commit(prepared)?;
        if index == 2 {
            super::restart_relational(&mut query, &native, &compiled, &options).await?;
        }
    }
    Ok(())
}
