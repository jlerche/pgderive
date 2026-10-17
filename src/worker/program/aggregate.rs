//! Shared scalar SQL aggregate semantics; traversal and persistence stay with callers.
use crate::{
    compiler::partition::{Aggregate, Function},
    transaction::Row,
};
use anyhow::{Context, Result};
use num_bigint::BigInt;
use std::borrow::Cow;
pub(super) enum Contribution<'a> {
    Star,
    Value(Cow<'a, str>),
}
#[derive(Default)]
pub(super) struct Linear {
    pub(super) count: BigInt,
    pub(super) sum: BigInt,
}
impl Linear {
    pub(super) fn observe(&mut self, aggregate: &Aggregate, row: &Row, weight: i64) -> Result<()> {
        let Some(value) = contribution(aggregate, row)? else { return Ok(()) };
        self.count += weight;
        if !matches!(aggregate.function, Function::Count | Function::CountDistinct) {
            let Contribution::Value(value) = value else {
                anyhow::bail!("missing aggregate measure")
            };
            let value = value.parse::<i64>()?;
            self.sum += BigInt::from(value) * weight;
        }
        Ok(())
    }
    pub(super) fn result(&self, aggregate: &Aggregate) -> Result<Option<String>> {
        match aggregate.function {
            Function::Count | Function::CountDistinct => {
                Ok(Some(i64::try_from(&self.count).context("COUNT overflow")?.to_string()))
            }
            Function::Sum | Function::Average if self.count == BigInt::from(0) => Ok(None),
            Function::Average => {
                let count = i64::try_from(&self.count).context("AVG non-NULL count overflow")?;
                if aggregate.argument.as_ref().is_some_and(|arg| arg.oid != 20) {
                    i64::try_from(&self.sum).context("AVG(int2/int4) sum overflow")?;
                }
                Ok(Some(format!("{}/{count}", self.sum)))
            }
            Function::Sum if aggregate.argument.as_ref().is_some_and(|arg| arg.oid == 20) => {
                Ok(Some(self.sum.to_string()))
            }
            Function::Sum => {
                Ok(Some(i64::try_from(&self.sum).context("SUM(int2/int4) overflow")?.to_string()))
            }
            _ => anyhow::bail!("aggregate requires specialized evaluator"),
        }
    }
}
// Absence is a filtered row or NULL argument; Star is COUNT(*).
pub(super) fn contribution<'a>(
    aggregate: &Aggregate,
    row: &'a Row,
) -> Result<Option<Contribution<'a>>> {
    if aggregate
        .filter
        .as_ref()
        .map(|filter| filter.qualifies(row))
        .transpose()?
        .is_some_and(|qualifies| !qualifies)
    {
        return Ok(None);
    }
    let value = if let Some(expression) = &aggregate.expression {
        expression.evaluate(row)?.map(Cow::Owned)
    } else {
        aggregate
            .argument
            .as_ref()
            .map(|column| -> Result<_> {
                Ok(row
                    .get(&column.name)
                    .context("missing aggregate input")?
                    .as_deref()
                    .map(Cow::Borrowed))
            })
            .transpose()?
            .flatten()
    };
    Ok(value
        .map(Contribution::Value)
        .or_else(|| aggregate.argument.is_none().then_some(Contribution::Star)))
}
