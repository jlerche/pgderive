//! SQL computations must be implemented by the engine, with no connected-PG escape hatch.
use super::contract;
use crate::compiler::compile;
use anyhow::{Context, Result};
#[test]
fn unsupported_functions_fail_before_registration_with_engine_reason() {
    for function in ["lower(category)", "random()", "to_char(id,'999')"] {
        let error =
            compile(&format!("SELECT {function} AS value FROM source.auction"), &contract()).err();
        assert!(error.is_some(), "{function}");
        assert!(format!("{error:?}").contains("not available in-engine"), "{error:?}");
    }
}
#[test]
fn casts_arithmetic_and_lazy_values_are_local_maps() -> Result<()> {
    for expression in
        ["id::bigint", "id+2", "abs(id)", "COALESCE(id,0)", "CASE WHEN id>0 THEN id ELSE 0 END"]
    {
        let compiled =
            compile(&format!("SELECT {expression} AS value FROM source.bid"), &contract())?;
        let computed = compiled
            .relational()
            .nodes
            .iter()
            .find_map(|node| {
                if let crate::compiler::relational::Node::Map { computed, .. } = node {
                    Some(computed)
                } else {
                    None
                }
            })
            .context("native computation missing")?;
        assert!(!computed.is_empty());
        compiled.validate_execution()?;
    }
    Ok(())
}

#[test]
fn invalid_constants_fail_in_engine_without_query_execution() {
    for expression in [
        "2147483647+1",
        "CASE WHEN id>0 THEN id ELSE 2147483647+1 END",
        "COALESCE(id,32768::smallint)",
    ] {
        let error =
            compile(&format!("SELECT {expression} AS value FROM source.bid"), &contract()).err();
        assert!(format!("{error:?}").contains("constant expression failed in-engine"), "{error:?}");
    }
}

#[test]
fn typed_text_and_boolean_functions_run_in_predicates() -> Result<()> {
    use crate::{compiler::relational::Node, transaction::Row};
    for (expression, positive, empty) in [
        ("length(category)>0", true, false),
        ("starts_with(category,'é')", true, false),
        ("reverse(category)='🙂é'", true, false),
        ("length(reverse(category))=2", true, false),
        ("reverse(category) IS NULL", false, false),
    ] {
        let compiled =
            compile(&format!("SELECT id FROM source.auction WHERE {expression}"), &contract())?;
        let predicate = compiled
            .relational()
            .nodes
            .iter()
            .find_map(|node| {
                if let Node::Filter { predicate, .. } = node { Some(predicate) } else { None }
            })
            .context("missing function predicate")?;
        for (value, expected) in
            [(Some("é🙂"), positive), (Some(""), empty), (None, expression.ends_with("IS NULL"))]
        {
            let row = Row::from([("0:category".into(), value.map(str::to_owned))]);
            assert_eq!(predicate.qualifies(&row)?, expected, "{expression}, {value:?}");
        }
    }
    Ok(())
}
