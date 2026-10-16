//! Native integral remainder for typed lookup-key expressions.
use super::{ColumnRef, relational, scalar::Computed};
use anyhow::{Result, ensure};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Clone, Serialize)]
pub struct Remainder {
    input: ColumnRef,
    divisor: i64,
}
impl Remainder {
    pub(super) const fn column(&self) -> &ColumnRef {
        &self.input
    }
    pub(super) fn evaluate(&self, row: &crate::transaction::Row) -> Result<Option<String>> {
        super::expression::integral_remainder(&self.input, self.divisor, row)
    }
}
pub(super) fn bind(
    input: ColumnRef,
    divisor: i64,
    computed: &mut Vec<Computed>,
) -> Result<ColumnRef> {
    ensure!(
        !input.right && matches!(input.oid, 20 | 21 | 23) && divisor != 0,
        "lookup MOD requires a left integral column and nonzero integral literal"
    );
    // An uncast integral literal is int4 or int8, so int2 input promotes to int4.
    let oid = if input.oid == 20 || i32::try_from(divisor).is_err() { 20 } else { 23 };
    let nullable = input.nullable;
    let mut input = input;
    input.name = relational::field(0, &input.name);
    let expression = Remainder { input, divisor };
    let name = format!("@lookup_mod_{:x}", Sha256::digest(serde_json::to_vec(&expression)?));
    let column = ColumnRef { name, right: false, oid, nullable };
    let mut target = column.clone();
    target.name = relational::field(0, &column.name);
    if !computed.iter().any(|prior| prior.column == target) {
        computed.push(Computed {
            column: target,
            expression: super::scalar::Native::Remainder { remainder: expression },
        });
    }
    Ok(column)
}
