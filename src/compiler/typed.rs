//! Checked native scalar expressions executed at their bound relational scope.
use super::capabilities::Arithmetic;
use super::{ColumnRef, parser::Name};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum};
use serde::Serialize;
use types::numeric_common;
mod bitwise;
mod calendar;
mod case;
pub(in crate::compiler) mod collation;
pub(in crate::compiler) mod control;
mod cost;
mod evaluate;
mod fold;
mod functions;
pub(super) use functions::like_pattern;
mod join;
mod minmax;
mod nullif;
mod operand;
mod predicate;
pub(super) use cost::OPERATOR_COST;
pub(super) use operand::{Operand, bind as operand, native_literal, navigation};
pub(super) use types::common as comparison_type;
mod types;
mod visit;
pub(super) use visit::Replace;

const TEXT_OUTPUT_LIMIT: usize = 16 * 1024 * 1024;
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Typed {
    oid: u32,
    value: Value,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
enum Value {
    Column(ColumnRef),
    Boolean(Box<super::expression::Expr>),
    Average(ColumnRef),
    Literal(Option<i64>),
    Numeric(String),
    Text(String),
    Encoded(Option<String>),
    Interval(crate::temporal::Interval),
    Temporal(super::scalar::Temporal),
    Call(functions::Call),
    Cast { input: Box<Typed>, typmod: Option<types::Modifier> },
    Collate { input: Box<Typed>, collation: u32 },
    Binary { operation: Arithmetic, left: Box<Typed>, right: Box<Typed> },
    Coalesce(Vec<Typed>),
    Case { branches: Vec<(super::expression::Expr, Typed)>, otherwise: Box<Typed> },
    SimpleCase(case::Simple),
    NullIf { left: Box<Typed>, right: Box<Typed> },
    Concat { left: Box<Typed>, right: Box<Typed> },
    MinMax { operation: minmax::Operation, arguments: Vec<Typed> },
}
impl Typed {
    pub(in crate::compiler) fn is_constant(&self) -> bool {
        match &self.value {
            Value::Column(_) | Value::Average(_) | Value::Temporal(_) => false,
            Value::Boolean(value) => value.is_constant(),
            Value::Literal(_)
            | Value::Numeric(_)
            | Value::Text(_)
            | Value::Encoded(_)
            | Value::Interval(_) => true,
            Value::Call(call) => call.is_constant(),
            Value::Cast { input, .. } | Value::Collate { input, .. } => input.is_constant(),
            Value::Binary { left, right, .. }
            | Value::NullIf { left, right }
            | Value::Concat { left, right } => left.is_constant() && right.is_constant(),
            Value::Coalesce(values) | Value::MinMax { arguments: values, .. } => {
                values.iter().all(Self::is_constant)
            }
            Value::SimpleCase(case) => case.is_constant(),
            Value::Case { branches, otherwise } => {
                otherwise.is_constant()
                    && branches
                        .iter()
                        .all(|(condition, value)| condition.is_constant() && value.is_constant())
            }
        }
    }

    pub(crate) fn evaluate(&self, row: &crate::transaction::Row) -> Result<Option<String>> {
        self.text_pair((row, row))
    }
}

fn check_width(value: i64, oid: u32) -> Result<()> {
    match oid {
        21 => {
            i16::try_from(value).context("smallint overflow")?;
        }
        23 => {
            i32::try_from(value).context("integer overflow")?;
        }
        20 => {}
        _ => return Err(crate::engine::execution::PlanFault("unsupported integral width").into()),
    }
    Ok(())
}
pub(super) fn bind(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    use super::parser::expressions::{column, node};
    ensure!(depth < 32, "typed scalar expression depth exceeded");
    match node(value)? {
        NodeEnum::CaseExpr(value) => case::bind(value, resolve, depth),
        NodeEnum::MinMaxExpr(value) => minmax::bind(value, resolve, depth),
        NodeEnum::ColumnRef(_) => {
            let column = resolve(&column(value)?)?;
            ensure!(
                matches!(
                    column.oid,
                    16 | 20 | 21 | 23 | 25 | 1043 | 1700 | 2950 | 1082 | 1114 | 1184 | 1186
                ),
                "column type is not available in-engine typed expressions"
            );
            Ok(Typed { oid: column.oid, value: Value::Column(column) })
        }
        NodeEnum::AConst(constant) => literal(constant),
        NodeEnum::TypeCast(cast) => types::cast(cast, resolve, depth),
        NodeEnum::CollateClause(clause) => collation::bind(clause, resolve, depth),
        NodeEnum::BoolExpr(_) | NodeEnum::NullTest(_) | NodeEnum::BooleanTest(_) => {
            boolean(value, resolve)
        }
        NodeEnum::AExpr(expression)
            if expression.kind == i32::from(pg_query::protobuf::AExprKind::AexprIn)
                || super::parser::expressions::distinct_kind(expression.kind).is_some()
                || super::parser::expressions::between_kind(expression.kind).is_some()
                || super::expression::like::is_like(expression)
                || super::expression::is_comparison(expression)? =>
        {
            boolean(value, resolve)
        }
        NodeEnum::AExpr(expression)
            if expression.kind == i32::from(pg_query::protobuf::AExprKind::AexprNullif) =>
        {
            nullif::bind(expression, resolve, depth)
        }
        NodeEnum::AExpr(expression) => operator(expression, resolve, depth),
        NodeEnum::CoalesceExpr(expression) => {
            ensure!(
                expression.xpr.is_none() && (1..=64).contains(&expression.args.len()),
                "invalid scalar COALESCE"
            );
            let arguments = expression
                .args
                .iter()
                .map(|value| Typed::bind_contextual(value, resolve, depth + 1))
                .collect::<Result<Vec<_>>>()?;
            let candidate = control::candidate(arguments.iter().map(Typed::oid));
            let arguments = arguments
                .into_iter()
                .map(|value| control::coerce(value, candidate))
                .collect::<Result<Vec<_>>>()?;
            let oid = types::common(&arguments.iter().map(Typed::oid).collect::<Vec<_>>())?;
            Ok(Typed { oid, value: Value::Coalesce(arguments) })
        }
        NodeEnum::FuncCall(function) => functions::bind(function, resolve, depth),
        _ => anyhow::bail!("expression is not available in-engine"),
    }
}
fn boolean(value: &Node, resolve: &impl Fn(&Name) -> Result<ColumnRef>) -> Result<Typed> {
    Ok(Typed { oid: 16, value: Value::Boolean(Box::new(super::expression::bind(value, resolve)?)) })
}
fn operator(
    expression: &pg_query::protobuf::AExpr,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    use super::parser::expressions::{names, optional};
    ensure!(
        expression.kind == i32::from(pg_query::protobuf::AExprKind::AexprOp),
        "unsupported scalar operator kind"
    );
    let name = names(&expression.name)?.0;
    let [operation] = name.as_slice() else {
        anyhow::bail!("qualified scalar operator unsupported")
    };
    if operation == "||" {
        let left = bind(optional(expression.lexpr.as_deref())?, resolve, depth + 1)?;
        let right = bind(optional(expression.rexpr.as_deref())?, resolve, depth + 1)?;
        return concat(left, right);
    }
    let complement = operation == "~";
    let operation = Arithmetic::resolve(operation)?;
    let left =
        expression.lexpr.as_deref().map(|value| bind(value, resolve, depth + 1)).transpose()?;
    let right = bind(optional(expression.rexpr.as_deref())?, resolve, depth + 1)?;
    if operation.is_bitwise() {
        return bitwise::bind(operation, complement, left, right);
    }
    if operation == Arithmetic::Remainder {
        return remainder(left.context("remainder requires two operands")?, right);
    }
    arithmetic(operation, left, right)
}

fn arithmetic(operation: Arithmetic, left: Option<Typed>, right: Typed) -> Result<Typed> {
    let left = if let Some(left) = left {
        left
    } else {
        ensure!(
            matches!(operation, Arithmetic::Add | Arithmetic::Subtract)
                && matches!(right.oid, 20 | 21 | 23 | 1700 | 1186),
            "unresolved unary scalar operator"
        );
        if matches!(operation, Arithmetic::Add) {
            return Ok(right);
        }
        if right.oid == 1186 {
            calendar::zero_interval()
        } else {
            Typed { oid: right.oid, value: Value::Literal(Some(0)) }
        }
    };
    if let Some(oid) = calendar::oid(left.oid, right.oid, operation)? {
        return Ok(Typed {
            oid,
            value: Value::Binary { operation, left: Box::new(left), right: Box::new(right) },
        });
    }
    ensure!(left.oid != 0 || right.oid != 0, "unresolved scalar operator types");
    ensure!(
        [left.oid, right.oid].iter().all(|oid| matches!(oid, 0 | 20 | 21 | 23 | 1700)),
        "arithmetic requires integral or numeric operands"
    );
    Ok(Typed {
        oid: numeric_common(&[left.oid, right.oid]),
        value: Value::Binary { operation, left: Box::new(left), right: Box::new(right) },
    })
}

fn remainder(left: Typed, right: Typed) -> Result<Typed> {
    ensure!(
        [left.oid, right.oid].iter().all(|oid| matches!(oid, 0 | 20 | 21 | 23 | 705 | 1700))
            && [left.oid, right.oid].iter().any(|oid| matches!(oid, 20 | 21 | 23 | 1700)),
        "remainder requires integral or numeric operands"
    );
    let oid = numeric_common(&[left.oid, right.oid]);
    let coerce = |value: Typed| {
        if matches!(value.oid, 0 | 705) { Typed::contextual_literal(value, oid) } else { Ok(value) }
    };
    Ok(Typed {
        oid,
        value: Value::Binary {
            operation: Arithmetic::Remainder,
            left: Box::new(coerce(left)?),
            right: Box::new(coerce(right)?),
        },
    })
}

fn concat(left: Typed, right: Typed) -> Result<Typed> {
    ensure!(
        matches!(left.oid, 0 | 25 | 705) && matches!(right.oid, 0 | 25 | 705),
        "concatenation requires text operands"
    );
    Ok(Typed {
        oid: 25,
        value: Value::Concat {
            left: Box::new(left.unknown_text()),
            right: Box::new(right.unknown_text()),
        },
    })
}

pub(super) fn computed(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    computed: &mut Vec<super::scalar::Computed>,
    scope: Option<usize>,
) -> Result<ColumnRef> {
    use super::parser::scalar;
    let mut typed = if scalar::is_bin(value)? {
        Typed::temporal(scalar::bin(value, resolve)?)
    } else {
        bind(value, resolve, 0)?
    };
    if matches!(typed.oid, 0 | 705) {
        typed.oid = 25;
    }
    ensure!(
        matches!(
            typed.oid,
            16 | 20 | 21 | 23 | 25 | 1043 | 1700 | 2950 | 1082 | 1114 | 1184 | 1186
        ),
        "computed scalar requires supported native type"
    );
    typed.projected_column(computed, &format!("@typed_{scope:?}_"))
}

impl Typed {
    pub(super) fn projected_column(
        self,
        computed: &mut Vec<super::scalar::Computed>,
        prefix: &str,
    ) -> Result<ColumnRef> {
        if let Value::Column(column) = self.value {
            return Ok(column);
        }
        super::scalar::register(self, computed, prefix)
    }
}

#[cfg(test)]
#[path = "typed/tests.rs"]
mod tests;

fn literal(constant: &pg_query::protobuf::AConst) -> Result<Typed> {
    if constant.isnull {
        return Ok(Typed { oid: 0, value: Value::Literal(None) });
    }
    if let Some(pg_query::protobuf::a_const::Val::Fval(value)) = &constant.val {
        if !value.fval.contains(['.', 'e', 'E'])
            && let Ok(integer) = value.fval.parse::<i64>()
        {
            return Ok(Typed {
                oid: if i32::try_from(integer).is_ok() { 23 } else { 20 },
                value: Value::Literal(Some(integer)),
            });
        }
        let numeric = crate::numeric::Numeric::parse(&value.fval)?;
        return Ok(Typed { oid: 1700, value: Value::Numeric(numeric.text()) });
    }
    if let Some(pg_query::protobuf::a_const::Val::Sval(value)) = &constant.val {
        return Ok(Typed { oid: 705, value: Value::Text(value.sval.clone()) });
    }
    if let Some(pg_query::protobuf::a_const::Val::Boolval(value)) = &constant.val {
        return Ok(Typed {
            oid: 16,
            value: Value::Encoded(Some(if value.boolval { "t" } else { "f" }.into())),
        });
    }
    let Some(pg_query::protobuf::a_const::Val::Ival(value)) = &constant.val else {
        anyhow::bail!("typed scalar requires integer literal");
    };
    Ok(Typed { oid: 23, value: Value::Literal(Some(i64::from(value.ival))) })
}

impl Typed {
    #[cfg(test)]
    pub(crate) const fn as_average(&self) -> Option<&ColumnRef> {
        if let Value::Average(column) = &self.value { Some(column) } else { None }
    }
    pub(super) fn nullable(&self) -> bool {
        match &self.value {
            Value::Column(column) | Value::Average(column) => column.nullable,
            Value::Boolean(value) => value.nullable(),
            Value::Literal(value) => value.is_none(),
            Value::NullIf { .. } => true,
            Value::Numeric(_) | Value::Text(_) | Value::Interval(_) => false,
            Value::Encoded(value) => value.is_none(),
            Value::Temporal(value) => value.input.nullable,
            Value::Call(call) => call.nullable(),
            Value::Cast { input, .. } | Value::Collate { input, .. } => input.nullable(),
            Value::Binary { left, right, .. } | Value::Concat { left, right } => {
                left.nullable() || right.nullable()
            }
            Value::Coalesce(arguments) | Value::MinMax { arguments, .. } => {
                arguments.iter().all(Self::nullable)
            }
            Value::SimpleCase(case) => case.results().any(Self::nullable),
            Value::Case { branches, otherwise } => {
                otherwise.nullable() || branches.iter().any(|(_, value)| value.nullable())
            }
        }
    }
    pub(in crate::compiler) const fn oid(&self) -> u32 {
        self.oid
    }
}

fn numeric_cast_input(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    if let NodeEnum::AConst(constant) = super::parser::expressions::node(value)?
        && let Some(pg_query::protobuf::a_const::Val::Sval(value)) = &constant.val
    {
        let numeric = crate::numeric::Numeric::parse_literal(&value.sval)?;
        return Ok(Typed { oid: 1700, value: Value::Numeric(numeric.text()) });
    }
    bind(value, resolve, depth)
}

impl Typed {
    pub(super) const fn average(column: ColumnRef) -> Self {
        Self { oid: 1700, value: Value::Average(column) }
    }
}

impl Typed {
    pub(in crate::compiler) fn set_cast(
        column: ColumnRef,
        oid: u32,
        modifier: Option<i32>,
    ) -> Result<Self> {
        ensure!(
            column.oid == oid
                || matches!(
                    (column.oid, oid),
                    (20 | 21 | 23, 20 | 21 | 23 | 1700)
                        | (25 | 1043, 25 | 1043)
                        | (1082, 1114 | 1184)
                        | (1114, 1184)
                ),
            "unsupported UNION coercion"
        );
        let mut input = Self::column(column);
        if input.oid == 1700
            && let Some(modifier) = modifier.filter(|value| *value >= 4)
        {
            input = Self {
                oid: 1700,
                value: Value::Cast {
                    input: Box::new(input),
                    typmod: Some(types::Modifier::Numeric(crate::numeric::Typmod::from_encoded(
                        modifier,
                    )?)),
                },
            };
        }
        if input.oid == oid {
            return Ok(input);
        }
        Ok(Self { oid, value: Value::Cast { input: Box::new(input), typmod: None } })
    }
    pub(in crate::compiler) fn widened_to_numeric(self) -> Self {
        Self { oid: 1700, value: Value::Cast { input: Box::new(self), typmod: None } }
    }
    pub(in crate::compiler) fn union_literal(value: &Node, oid: u32) -> Result<Self> {
        let value = Self::bind_contextual(value, &|_| anyhow::bail!("expected set literal"), 0)?;
        ensure!(matches!(value.oid, 0 | 705), "expected unknown set literal");
        value.coerce_common(oid)
    }
    pub(super) fn bind_contextual(
        value: &Node,
        resolve: &impl Fn(&Name) -> Result<ColumnRef>,
        depth: usize,
    ) -> Result<Self> {
        use super::parser::expressions::{node, optional};
        ensure!(depth < 32, "typed scalar expression depth exceeded");
        if let NodeEnum::CollateClause(clause) = node(value)? {
            let input =
                Self::bind_contextual(optional(clause.arg.as_deref())?, resolve, depth + 1)?;
            return collation::explicit(clause, input);
        }
        bind(value, resolve, depth)
    }

    pub(super) fn coerce_common(self, oid: u32) -> Result<Self> {
        match self {
            Self { oid: 0 | 705, value: Value::Collate { input, collation } } => {
                let input = input.coerce_common(oid)?;
                if matches!(oid, 25 | 1043) {
                    Ok(Self { oid, value: Value::Collate { input: Box::new(input), collation } })
                } else {
                    Ok(input)
                }
            }
            value if matches!(value.oid, 0 | 705) => Self::contextual_literal(value, oid),
            value if value.oid == oid => Ok(value),
            // The caller selected a common type from the implicit native casts.
            value => Ok(Self { oid, value: Value::Cast { input: Box::new(value), typmod: None } }),
        }
    }

    fn contextual_literal(value: Self, oid: u32) -> Result<Self> {
        let text = match value.value {
            Value::Literal(None) => None,
            Value::Text(value) => Some(value),
            _ => anyhow::bail!("expected an unknown literal"),
        };
        let text = text
            .map(|value| -> Result<String> {
                let value = if oid == 16 {
                    // PostgreSQL boolin also treats vertical tab as whitespace.
                    let lower = value
                        .trim_matches(|c: char| c.is_ascii_whitespace() || c == '\u{b}')
                        .to_ascii_lowercase();
                    let yes = lower == "1"
                        || (!lower.is_empty()
                            && ["true", "yes", "on"].iter().any(|v| v.starts_with(&lower)));
                    let no = lower == "0"
                        || (!lower.is_empty()
                            && ["false", "no", "off"].iter().any(|v| v.starts_with(&lower)));
                    ensure!(yes != no, "invalid boolean literal");
                    if yes { "t".into() } else { "f".into() }
                } else if matches!(oid, 20 | 21 | 23) {
                    value.trim_ascii().parse::<i64>()?.to_string()
                } else {
                    value
                };
                crate::compiler::value::literal_text(oid, &value)
            })
            .transpose()?;
        let result = Self { oid, value: Value::Encoded(text) };
        result.evaluate(&crate::transaction::Row::new())?;
        Ok(result)
    }
}
