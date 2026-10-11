use super::contract;
use crate::{compiler::compile, source::Column};
use anyhow::{Context, Result};
pub(super) fn native(oid: u32) -> crate::source::Contract {
    let mut native = contract();
    native.relations[1].columns.extend([
        Column {
            position: 4,
            name: "bidder".into(),
            oid: 20,
            modifier: -1,
            nullable: true,
            primary: false,
            collation: 0,
        },
        Column {
            position: 5,
            name: "date_time".into(),
            oid,
            modifier: -1,
            nullable: true,
            primary: false,
            collation: 0,
        },
        Column {
            position: 6,
            name: "extra".into(),
            oid: 25,
            modifier: -1,
            nullable: true,
            primary: false,
            collation: 100,
        },
    ]);
    native
}
#[test]
fn q14_has_distinct_native_inputs_and_visible_typed_outputs() -> Result<()> {
    let sql = include_str!("../../../queries/nexmark/q14.sql").replace("nexmark.", "source.");
    for oid in [1114, 1184] {
        let sql = if oid == 1114 {
            sql.replace("date_time AT TIME ZONE 'UTC'", "date_time")
        } else {
            sql.clone()
        };
        let compiled = compile(&sql, &native(oid))?;
        let layout = compiled.projection().context("projection absent")?;
        assert_eq!(layout.columns.len(), 5);
        assert!(layout.terminal.is_some());
        assert!(compiled.validate_bound().is_err());
        let equivalent = compile(
            &sql.replace("SELECT auction,bidder", "SELECT b.auction,b.bidder")
                .replace("FROM source.bid", "FROM source.bid b"),
            &native(oid),
        )?;
        assert_eq!(serde_json::to_value(&compiled)?, serde_json::to_value(&equivalent)?);
        for changed in [sql.replace("50000000", "40000000"), sql.replace("'dayTime'", "'day'")] {
            assert_ne!(
                serde_json::to_value(&compiled)?,
                serde_json::to_value(compile(&changed, &native(oid))?)?
            );
        }
    }
    Ok(())
}
#[test]
fn terminal_programs_reject_non_native_or_untyped_semantics() {
    for expression in [
        "2 * price",
        "price * price",
        "1e2 * price",
        "replace(extra,1,'')",
        "extract(hour FROM date_time)",
        "timezone('Europe/London',date_time)",
        "length(DISTINCT extra)",
        "length(extra) OVER ()",
        "CASE WHEN price>1 THEN 'x' ELSE 1 END",
        "CASE WHEN price>1 THEN 'x' END",
        "random()",
        "public.length(extra)",
    ] {
        let sql = format!("SELECT {expression} AS output FROM source.bid");
        assert!(compile(&sql, &native(1184)).is_err(), "accepted {sql}");
    }
}

#[test]
fn terminal_filter_retains_hidden_operand_without_exposing_it() -> Result<()> {
    let compiled =
        compile("SELECT auction FROM source.bid WHERE 0.908 * price > 1", &native(1184))?;
    let layout = compiled.projection().context("projection absent")?;
    assert_eq!(layout.columns.len(), 2);
    let value = serde_json::to_value(layout)?;
    let outputs =
        value["terminal"]["program"]["outputs"].as_array().context("output layout absent")?;
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0]["label"], "auction");
    assert!(value["terminal"]["program"]["predicate"].is_number());
    Ok(())
}
