use super::{Expression, Function, Literal, Program};
use anyhow::{Context, Result, ensure};
impl Program {
    pub(in crate::catalog::terminal) fn render(&self, inputs: &[u32]) -> Result<(String, String)> {
        self.validate(inputs)?;
        let mut nodes = Vec::new();
        let mut bytes = 0;
        for (index, expression) in self.expressions.iter().enumerate() {
            let rendered = render(expression, index, &nodes, inputs)?;
            bytes += rendered.len();
            ensure!(bytes <= 262_144, "terminal rendered expression budget exceeded");
            nodes.push(rendered);
        }
        let columns = self
            .outputs
            .iter()
            .map(|output| Ok(format!("pg_catalog.to_jsonb({})", get(&nodes, output.expression)?)))
            .collect::<Result<Vec<_>>>()?
            .join(",");
        let predicate =
            self.predicate.map_or(Ok::<String, anyhow::Error>("TRUE".into()), |index| {
                Ok(format!("({}) IS TRUE", get(&nodes, index)?))
            })?;
        Ok((columns, predicate))
    }
}
fn get(nodes: &[String], index: usize) -> Result<&str> {
    nodes.get(index).map(String::as_str).context("terminal rendering node absent")
}
fn render(
    expression: &Expression,
    index: usize,
    nodes: &[String],
    inputs: &[u32],
) -> Result<String> {
    match expression {
        Expression::Input(index) => {
            let oid = inputs.get(*index).context("terminal input absent")?;
            let native = match oid {
                16 => "bool",
                20 => "int8",
                21 => "int2",
                23 => "int4",
                25 | 1043 => "text",
                2950 => "uuid",
                1114 => "timestamp",
                1184 => "timestamptz",
                _ => anyhow::bail!("unsupported terminal input codec"),
            };
            Ok(format!("(tuple->1->>{index})::pg_catalog.{native}"))
        }
        Expression::Literal(literal) => {
            let (variant, native) = match literal {
                Literal::Text(_) => ("Text", "text"),
                Literal::Integer(_) => ("Integer", "int4"),
                Literal::Numeric(_) => ("Numeric", "numeric"),
            };
            Ok(format!(
                "(($2::pg_catalog.jsonb)->'expressions'->{index}->'Literal'->>'{variant}')::pg_catalog.{native}"
            ))
        }
        Expression::Call { function, arguments } => {
            let mut arguments = arguments
                .iter()
                .map(|index| Ok(get(nodes, *index)?.to_string()))
                .collect::<Result<Vec<_>>>()?;
            if *function == Function::Hour {
                arguments.insert(0, "'hour'::pg_catalog.text".into());
            }
            if *function == Function::Utc {
                arguments.insert(0, "'UTC'::pg_catalog.text".into());
            }
            Ok(format!("pg_catalog.{}({})", function.name(), arguments.join(",")))
        }
        Expression::CollateC(input) => {
            Ok(format!("({}) COLLATE pg_catalog.\"C\"", get(nodes, *input)?))
        }
        Expression::ArrayElement { input, index } => {
            Ok(format!("({})[{index}]", get(nodes, *input)?))
        }
        Expression::IsNull { input, negated } => {
            Ok(format!("({}) IS {}NULL", get(nodes, *input)?, if *negated { "NOT " } else { "" }))
        }
        Expression::And(arguments) => boolean(arguments, nodes, " AND "),
        Expression::Or(arguments) => boolean(arguments, nodes, " OR "),
        Expression::Not(index) => Ok(format!("(NOT ({}))", get(nodes, *index)?)),
        Expression::Case { branches, otherwise } => {
            let branches = branches
                .iter()
                .map(|(condition, value)| {
                    Ok(format!("WHEN {} THEN {}", get(nodes, *condition)?, get(nodes, *value)?))
                })
                .collect::<Result<Vec<_>>>()?
                .join(" ");
            Ok(format!("(CASE {branches} ELSE {} END)", get(nodes, *otherwise)?))
        }
    }
}
fn boolean(arguments: &[usize], nodes: &[String], operator: &str) -> Result<String> {
    let values = arguments
        .iter()
        .map(|index| Ok(format!("({})", get(nodes, *index)?)))
        .collect::<Result<Vec<_>>>()?;
    Ok(format!("({})", values.join(operator)))
}
