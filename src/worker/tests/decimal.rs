//! Final exact numeric arithmetic remains a `PostgreSQL` publication transform.
use super::contract;
use crate::compiler::compile;
use anyhow::Result;
#[test]
fn decimal_projection_binds_factor_and_preserves_scale() -> Result<()> {
    let native = contract();
    let sql = "SELECT 0.908 * b.price AS price FROM source.bid b";
    let compiled = compile(sql, &native)?;
    assert!(
        compiled
            .revision
            .as_deref()
            .is_some_and(|s| s.ends_with(":pg-terminal-decimal-multiply-v1"))
    );
    for normalized in
        [sql.replace("0.908 * b.price", "b.price * .908"), sql.replace("0.908", "00.908")]
    {
        assert_eq!(
            serde_json::to_vec(&compiled)?,
            serde_json::to_vec(&compile(&normalized, &native)?)?
        );
    }
    for changed in [sql.replace("0.908", "0.909"), sql.replace("0.908", "0.9080")] {
        assert_ne!(
            serde_json::to_vec(&compiled)?,
            serde_json::to_vec(&compile(&changed, &native)?)?
        );
    }
    Ok(())
}
#[test]
fn decimal_projection_rejects_unsupported_terminal_arithmetic() {
    let native = contract();
    for sql in [
        "SELECT 2 * price FROM source.bid",
        "SELECT 1e3 * price FROM source.bid",
        "SELECT -0.908 * price FROM source.bid",
        "SELECT price * price FROM source.bid",
        "SELECT 0.908 + price FROM source.bid",
        "SELECT 0.908 * category FROM source.auction",
        "SELECT NULL * price FROM source.bid",
    ] {
        assert!(compile(sql, &native).is_err(), "{sql}");
    }
}
