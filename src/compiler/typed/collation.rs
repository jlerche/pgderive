//! Collation derivation for join comparisons and side-local text keys.
use super::{ColumnRef, Typed, Value};
use anyhow::{Result, ensure};
impl Typed {
    pub(in crate::compiler) fn collation(
        &self,
        resolve: &impl Fn(&ColumnRef) -> Result<u32>,
    ) -> Result<u32> {
        if !matches!(self.oid, 25 | 1043 | 705) {
            return Ok(0);
        }
        match &self.value {
            Value::Column(column) => {
                let collation = resolve(column)?;
                ensure!(collation != 0, "unresolved text expression collation");
                Ok(collation)
            }
            Value::Cast { input, .. } if input.oid == 0 => Ok(100),
            Value::Cast { input, .. } => input.collation(resolve),
            Value::Call(call) => collations(&call.arguments, resolve),
            Value::Coalesce(values) => collations(values, resolve),
            Value::Case { branches, otherwise } => {
                let mut result = common(100, otherwise.collation(resolve)?)?;
                for (_, value) in branches {
                    result = common(result, value.collation(resolve)?)?;
                }
                Ok(result)
            }
            _ => Ok(100),
        }
    }
}
fn collations(values: &[Typed], resolve: &impl Fn(&ColumnRef) -> Result<u32>) -> Result<u32> {
    let mut result = 100;
    for value in values {
        result = common(result, value.collation(resolve)?)?;
    }
    Ok(result)
}
pub(in crate::compiler) fn common(left: u32, right: u32) -> Result<u32> {
    ensure!(
        left == right || matches!(left, 0 | 100) || matches!(right, 0 | 100),
        "conflicting join text collations"
    );
    Ok(if right == 0 {
        left
    } else if matches!(left, 0 | 100) {
        right
    } else {
        left
    })
}
