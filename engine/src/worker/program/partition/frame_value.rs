//! Select a row within the current frame without evaluating unselected values.
use super::selection::Selection;
use crate::{
    compiler::partition::{Aggregate, Function},
    engine::execution::{EvaluationFailed, PlanFault},
    transaction::Row,
    worker::program::aggregate::{Contribution, contribution},
};
use anyhow::{Context, Result};

pub(super) fn evaluate(
    value: &Aggregate,
    frame: Selection<'_>,
    current: &Row,
) -> Result<Option<String>> {
    let selected = match value.function {
        Function::FirstValue => frame.get(0),
        Function::LastValue => frame.len().checked_sub(1).and_then(|index| frame.get(index)),
        Function::NthValue => {
            let position = value.position.as_ref().context("missing NTH_VALUE position")?;
            let Some(position) = position.evaluate(current)? else {
                return Ok(None);
            };
            let position =
                position.parse::<i32>().context(PlanFault("invalid int4 window position"))?;
            if position <= 0 {
                return Err(anyhow::anyhow!("argument of nth_value must be greater than zero")
                    .context(EvaluationFailed));
            }
            frame.get(usize::try_from(position - 1)?)
        }
        _ => anyhow::bail!("expected frame value function"),
    };
    let Some((_, row)) = selected else {
        return Ok(None);
    };
    match contribution(value, row)? {
        Some(Contribution::Value(value)) => Ok(Some(value.into_owned())),
        None => Ok(None),
        Some(Contribution::Star) => anyhow::bail!("missing window value"),
    }
}
