//! Explicit closed collation and regex context, validated again for restored plans.
use super::{Expression, Function, Literal};
use anyhow::{Result, ensure};
pub(super) fn validate(expression: &Expression, prior: &[Expression]) -> Result<()> {
    match expression {
        Expression::Call { function: Function::Lower, arguments } => {
            ensure!(
                arguments.first().is_some_and(|index| collated(*index, prior)),
                "lower requires explicit C collation"
            );
        }
        Expression::Call { function: Function::TextEqual, arguments } => {
            ensure!(
                arguments.iter().any(|index| collated(*index, prior)),
                "text equality requires explicit C collation"
            );
        }
        Expression::Call { function: Function::RegexMatch, arguments } => {
            let [input, pattern] = arguments.as_slice() else {
                anyhow::bail!("regexp_match requires two operands");
            };
            ensure!(collated(*input, prior), "regexp_match requires explicit C collation");
            ensure!(
                matches!(prior.get(*pattern), Some(Expression::Literal(Literal::Text(pattern))) if pattern == "(&|^)channel_id=([^&]*)"),
                "only the closed channel_id capture pattern is supported"
            );
        }
        Expression::ArrayElement { input, .. } => {
            ensure!(
                matches!(
                    prior.get(*input),
                    Some(Expression::Call { function: Function::RegexMatch, .. })
                ),
                "terminal array selection requires regexp_match"
            );
        }
        _ => {}
    }
    Ok(())
}
fn collated(index: usize, prior: &[Expression]) -> bool {
    matches!(
        prior.get(index),
        Some(Expression::CollateC(_) | Expression::Call { function: Function::Lower, .. })
    )
}
