//! Case-sensitive LIKE under deterministic C/POSIX collations.
use super::{Expr, Resolve, Scalar, operand};
use crate::{
    compiler::{
        parser::expressions::{names, optional},
        value::Value,
    },
    engine::execution::EvaluationFailed,
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use pg_query::protobuf as pg;

const MAX_PATTERN_BYTES: usize = 16 * 1024 * 1024;
const MAX_MATCH_STEPS: usize = 16 * 1024 * 1024;
const MAX_MATCH_DEPTH: usize = 128;

pub(in crate::compiler) fn is_like(expression: &pg::AExpr) -> bool {
    expression.kind == i32::from(pg::AExprKind::AexprLike)
}

pub(super) fn bind(expression: &pg::AExpr, resolve: &Resolve<'_>) -> Result<Expr> {
    let operator = names(&expression.name)?;
    let negated = match operator.0.as_slice() {
        [operator] if operator == "~~" => false,
        [operator] if operator == "!~~" => true,
        _ => anyhow::bail!("unsupported LIKE operator"),
    };
    let input = operand(optional(expression.lexpr.as_deref())?, resolve)?.coerce(25)?;
    let pattern =
        super::typed::like_pattern(optional(expression.rexpr.as_deref())?, &|name| resolve(name))?;
    let collation = |column: &crate::compiler::ColumnRef| Ok(column.collation);
    let collation =
        input.derive_collation(&collation)?.merge(pattern.derive_collation(&collation)?)?.oid()?;
    ensure!(matches!(collation, 950 | 951), "LIKE requires deterministic C/POSIX collation");
    Ok(Expr::Like { input, pattern, negated })
}

pub(super) fn evaluate(
    input: &Scalar,
    pattern: &Scalar,
    negated: bool,
    rows: (&Row, &Row),
) -> Result<Option<bool>> {
    let (input, pattern) = (input.native_pair(rows)?, pattern.native_pair(rows)?);
    let (input, pattern) = match (input, pattern) {
        (Some(Value::Text(input)), Some(Value::Text(pattern))) => (input, pattern),
        (None, _) | (_, None) => return Ok(None),
        _ => return Err(crate::engine::execution::PlanFault("invalid LIKE operands").into()),
    };
    let matched = matches(&input, &pattern).context(EvaluationFailed)?;
    Ok(Some(matched != negated))
}

pub(in crate::compiler) fn escape(pattern: &str, escape: &str) -> Result<String> {
    let mut chars = escape.chars();
    let escape = chars.next();
    ensure!(chars.next().is_none(), "LIKE escape must be empty or one character");
    ensure!(pattern.len() <= MAX_PATTERN_BYTES, "LIKE pattern exceeds byte budget");
    if escape == Some('\\') {
        return Ok(pattern.into());
    }
    let mut result = String::with_capacity(pattern.len());
    let mut escaped = false;
    for ch in pattern.chars() {
        if !escaped && Some(ch) == escape {
            result.push('\\');
            escaped = true;
        } else {
            if !escaped && ch == '\\' {
                result.push('\\');
            }
            result.push(ch);
            escaped = false;
        }
        ensure!(result.len() <= MAX_PATTERN_BYTES, "LIKE pattern exceeds byte budget");
    }
    Ok(result)
}

fn matches(input: &str, pattern: &str) -> Result<bool> {
    ensure!(pattern.len() <= MAX_PATTERN_BYTES, "LIKE pattern exceeds byte budget");
    let mut steps = MAX_MATCH_STEPS;
    Ok(scan(input, pattern, &mut steps, 0)? == Match::Yes)
}

#[derive(PartialEq, Eq)]
enum Match {
    Yes,
    No,
    // No later candidate can match after the text is exhausted.
    Exhausted,
}

fn spend(steps: &mut usize) -> Result<()> {
    *steps = steps.checked_sub(1).context("LIKE match exceeds work budget")?;
    Ok(())
}

fn scan(mut input: &str, mut pattern: &str, steps: &mut usize, depth: usize) -> Result<Match> {
    ensure!(depth <= MAX_MATCH_DEPTH, "LIKE match exceeds wildcard depth budget");
    while let (Some(ch), Some(token)) = (input.chars().next(), pattern.chars().next()) {
        spend(steps)?;
        pattern = &pattern[token.len_utf8()..];
        match token {
            '%' => return search(input, pattern, steps, depth),
            '_' => {}
            '\\' => {
                let literal = escaped_literal(pattern)?;
                if ch != literal {
                    return Ok(Match::No);
                }
                pattern = &pattern[literal.len_utf8()..];
            }
            literal if literal != ch => return Ok(Match::No),
            _ => {}
        }
        input = &input[ch.len_utf8()..];
    }
    if !input.is_empty() {
        return Ok(Match::No);
    }
    for token in pattern.chars() {
        spend(steps)?;
        if token != '%' {
            return Ok(Match::Exhausted);
        }
    }
    Ok(Match::Yes)
}

fn search(mut input: &str, mut pattern: &str, steps: &mut usize, depth: usize) -> Result<Match> {
    while let Some(token) = pattern.chars().next() {
        spend(steps)?;
        match token {
            '%' => {}
            '_' => {
                let Some(ch) = input.chars().next() else { return Ok(Match::Exhausted) };
                input = &input[ch.len_utf8()..];
            }
            _ => break,
        }
        pattern = &pattern[token.len_utf8()..];
    }
    let first = match pattern.chars().next() {
        None => return Ok(Match::Yes),
        Some('\\') => escaped_literal(&pattern[1..])?,
        Some(ch) => ch,
    };
    for (offset, ch) in input.char_indices() {
        spend(steps)?;
        if ch == first {
            let matched = scan(&input[offset..], pattern, steps, depth + 1)?;
            if matched != Match::No {
                return Ok(matched);
            }
        }
    }
    Ok(Match::Exhausted)
}

fn escaped_literal(pattern: &str) -> Result<char> {
    pattern.chars().next().context("LIKE pattern must not end with escape character")
}

#[cfg(test)]
mod tests {
    use super::{MAX_MATCH_DEPTH, matches, scan};

    #[test]
    fn limits_bound_backtracking_and_nested_wildcard_search() {
        let work = scan("aaaaaaaaaaaa", "%a%a%a%ab", &mut 8, 0);
        assert!(work.is_err_and(|error| error.to_string().contains("work budget")));

        let depth = MAX_MATCH_DEPTH + 1;
        assert!(
            matches(&"a".repeat(depth), &"%a".repeat(depth))
                .is_err_and(|error| error.to_string().contains("depth budget"))
        );
    }
}
