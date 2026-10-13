//! Remove unreachable nodes after associative boolean flattening at compilation.
use super::{Expression, Program};
use anyhow::{Context, Result};
impl Program {
    pub fn prune(&mut self) -> Result<()> {
        let mut pending = self.outputs.iter().map(|output| output.expression).collect::<Vec<_>>();
        pending.extend(self.predicate);
        let mut live = vec![false; self.expressions.len()];
        while let Some(index) = pending.pop() {
            let visited = live.get_mut(index).context("terminal root node absent")?;
            if !*visited {
                *visited = true;
                pending.extend(references(&self.expressions[index]));
            }
        }
        let mut mapping = vec![None; self.expressions.len()];
        for (index, mut expression) in std::mem::take(&mut self.expressions).into_iter().enumerate()
        {
            if live[index] {
                remap(&mut expression, &mapping)?;
                mapping[index] = Some(self.expressions.len());
                self.expressions.push(expression);
            }
        }
        for output in &mut self.outputs {
            output.expression = mapped(output.expression, &mapping)?;
        }
        self.predicate = self.predicate.map(|index| mapped(index, &mapping)).transpose()?;
        Ok(())
    }
}
fn references(expression: &Expression) -> Vec<usize> {
    match expression {
        Expression::Input(_) | Expression::Null(_) | Expression::Literal(_) => Vec::new(),
        Expression::Call { arguments, .. }
        | Expression::Coalesce(arguments)
        | Expression::And(arguments)
        | Expression::Or(arguments) => arguments.clone(),
        Expression::Not(input)
        | Expression::CollateC(input)
        | Expression::ArrayElement { input, .. }
        | Expression::IsNull { input, .. } => vec![*input],
        Expression::Case { branches, otherwise } => branches
            .iter()
            .flat_map(|(condition, value)| [*condition, *value])
            .chain([*otherwise])
            .collect(),
    }
}
fn mapped(index: usize, mapping: &[Option<usize>]) -> Result<usize> {
    mapping.get(index).copied().flatten().context("terminal node must reference a live prior node")
}
fn remap(expression: &mut Expression, mapping: &[Option<usize>]) -> Result<()> {
    match expression {
        Expression::Input(_) | Expression::Null(_) | Expression::Literal(_) => {}
        Expression::Call { arguments, .. }
        | Expression::Coalesce(arguments)
        | Expression::And(arguments)
        | Expression::Or(arguments) => {
            for index in arguments {
                *index = mapped(*index, mapping)?;
            }
        }
        Expression::Not(input)
        | Expression::CollateC(input)
        | Expression::ArrayElement { input, .. }
        | Expression::IsNull { input, .. } => {
            *input = mapped(*input, mapping)?;
        }
        Expression::Case { branches, otherwise } => {
            for (condition, value) in branches {
                *condition = mapped(*condition, mapping)?;
                *value = mapped(*value, mapping)?;
            }
            *otherwise = mapped(*otherwise, mapping)?;
        }
    }
    Ok(())
}
