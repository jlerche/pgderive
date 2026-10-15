use crate::{compiler::compile, source::Column};
use anyhow::{Context, Result};
fn native() -> crate::source::Contract {
    let mut native = super::terminal_program::native(1184);
    native.relations[1].columns.extend([(7, "channel"), (8, "url")].into_iter().map(
        |(position, name)| Column {
            position,
            name: name.into(),
            oid: 25,
            modifier: -1,
            nullable: true,
            primary: false,
            collation: 100,
        },
    ));
    native
}
#[test]
fn q21_keeps_url_hidden_and_normalizes_in_to_or() -> Result<()> {
    let sql = include_str!("../../../queries/nexmark/q21.sql").replace("nexmark.", "source.");
    let compiled = compile(&sql, &native())?;
    let layout = compiled.output();
    assert_eq!(layout.columns.len(), 5);
    assert_eq!(layout.columns[4].column.name, crate::compiler::relational::field(0, "url"));
    let value = serde_json::to_value(layout)?;
    let outputs = value["terminal"]["program"]["outputs"].as_array().context("outputs absent")?;
    assert_eq!(outputs.len(), 5);
    assert_eq!(outputs[4]["label"], "channel_id");
    let explicit = sql.replace("lower(channel COLLATE pg_catalog.\"C\") IN ('apple','google','facebook','baidu')", "(lower(channel COLLATE pg_catalog.\"C\")='apple' OR lower(channel COLLATE pg_catalog.\"C\")='google' OR lower(channel COLLATE pg_catalog.\"C\")='facebook' OR lower(channel COLLATE pg_catalog.\"C\")='baidu')");
    assert_eq!(
        serde_json::to_value(&compiled)?,
        serde_json::to_value(compile(&explicit, &native())?)?
    );
    assert!(compiled.validate_bound().is_err());
    Ok(())
}
#[test]
fn q21_rejects_implicit_collation_regex_flags_and_dynamic_context() {
    for expression in [
        "lower(channel)",
        "lower(channel COLLATE pg_catalog.\"default\")",
        "regexp_matches(url COLLATE pg_catalog.\"C\",'(&|^)channel_id=([^&]*)')",
        "regexp_match(url COLLATE pg_catalog.\"C\",channel)",
        "regexp_match(url COLLATE pg_catalog.\"C\",'(&|^)channel_id=([^&]*)','i')",
        "(regexp_match(url COLLATE pg_catalog.\"C\",'(&|^)channel_id=([^&]*)'))[0]",
        "(regexp_match(url COLLATE pg_catalog.\"C\",'(&|^)channel_id=([^&]*)'))[1:2]",
        "lower(channel COLLATE pg_catalog.\"C\") NOT IN ('apple','baidu')",
    ] {
        let sql = format!("SELECT {expression} AS value FROM source.bid");
        assert!(compile(&sql, &native()).is_err(), "accepted {sql}");
    }
}
