//! Exact decimal arithmetic executes through native scalar maps.
use super::contract;
use crate::compiler::{compile, relational::Node};
use anyhow::{Context, Result};
#[test]
fn decimal_projection_executes_locally_and_preserves_scale() -> Result<()> {
    for (expression, expected) in [
        ("0.908 * price", "6.356"),
        ("price * .908", "6.356"),
        ("0.9080 * price", "6.3560"),
        ("price * price", "49"),
        ("0.908 + price", "7.908"),
    ] {
        let compiled =
            compile(&format!("SELECT {expression} AS value FROM source.bid"), &contract())?;
        let computed = compiled
            .relational()
            .nodes
            .iter()
            .find_map(
                |node| if let Node::Map { computed, .. } = node { computed.first() } else { None },
            )
            .context("local numeric computation absent")?;
        assert_eq!(
            computed
                .expression
                .evaluate(&[("0:price".into(), Some("7".into()))].into())?
                .as_deref(),
            Some(expected)
        );
    }
    Ok(())
}
#[test]
fn unavailable_numeric_operations_reject() {
    for expression in ["price / 3", "round(price)", "0.908 * category"] {
        assert!(
            compile(&format!("SELECT {expression} AS value FROM source.auction"), &contract())
                .is_err()
        );
    }
}
