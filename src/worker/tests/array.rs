//! Compiler boundary for sink-deferred `PostgreSQL` array extraction.
use super::contract;
use crate::compiler::compile;
use anyhow::Result;
const SQL: &str = "SELECT a.id,(pg_catalog.string_to_array(a.category COLLATE pg_catalog.\"C\",'/'))[4] AS dir FROM source.auction a";
#[test]
fn array_projection_normalizes_names_and_binds_index_delimiter_semantics() -> Result<()> {
    let native = contract();
    let compiled = compile(SQL, &native)?;

    assert!(compiled.output().terminal.is_some());
    let normalized = SQL
        .replace("a.", "\"A\".")
        .replace("auction a", "auction AS \"A\"")
        .replace("pg_catalog.string_to_array", "string_to_array");
    assert_eq!(
        serde_json::to_vec(&compiled)?,
        serde_json::to_vec(&compile(&normalized, &native)?)?
    );
    for changed in
        [SQL.replace("[4]", "[5]"), SQL.replace("'/'", "'🙂'"), SQL.replace("AS dir", "AS renamed")]
    {
        assert_ne!(
            serde_json::to_vec(&compiled)?,
            serde_json::to_vec(&compile(&changed, &native)?)?
        );
    }
    Ok(())
}
#[test]
fn array_selection_rejects_unsupported_shapes_before_runtime() {
    let native = contract();
    for invalid in [
        SQL.replace("[4]", "[0]"),
        SQL.replace("[4]", "[-1]"),
        SQL.replace("[4]", "[1025]"),
        SQL.replace("[4]", "[a.id]"),
        SQL.replace("[4]", "[1:4]"),
        SQL.replace("[4]", "[4][1]"),
        SQL.replace(" COLLATE pg_catalog.\"C\"", ""),
        SQL.replace("pg_catalog.\"C\"", "\"C\""),
        SQL.replace("pg_catalog.\"C\"", "pg_catalog.\"POSIX\""),
        SQL.replace("'/'", "NULL"),
        SQL.replace("'/'", "''"),
        SQL.replace("'/'", "a.other"),
        SQL.replace("string_to_array", "regexp_split_to_array"),
        SQL.replace("pg_catalog.string_to_array", "public.string_to_array"),
        SQL.replace("a.category COLLATE", "a.id COLLATE"),
        SQL.replace("a.category COLLATE", "length(a.category) COLLATE"),
        format!("SELECT q.dir FROM({SQL}) q"),
    ] {
        assert!(compile(&invalid, &native).is_err(), "{invalid}");
    }
}
