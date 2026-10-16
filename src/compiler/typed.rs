//! Pure checked integral expression trees executed before relational state.
use super::{ColumnRef, parser::Name, scalar::common};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum};
use serde::Serialize;
mod case;
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Typed {
    oid: u32,
    value: Value,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
enum Value {
    Column(ColumnRef),
    Literal(Option<i64>),
    Numeric(String),
    Cast(Box<Typed>),
    Binary { operation: String, left: Box<Typed>, right: Box<Typed> },
    Coalesce(Vec<Typed>),
    Case { branches: Vec<(super::expression::Expr, Typed)>, otherwise: Box<Typed> },
}
impl Typed {
    pub(super) fn columns(&self, columns: &mut Vec<ColumnRef>) {
        match &self.value {
            Value::Column(column) => columns.push(column.clone()),
            Value::Literal(_) | Value::Numeric(_) => {}
            Value::Cast(value) => value.columns(columns),
            Value::Binary { left, right, .. } => {
                left.columns(columns);
                right.columns(columns);
            }
            Value::Coalesce(values) => {
                for value in values {
                    value.columns(columns);
                }
            }
            Value::Case { branches, otherwise } => {
                for (condition, value) in branches {
                    condition.columns(columns);
                    value.columns(columns);
                }
                otherwise.columns(columns);
            }
        }
    }
    pub(crate) fn evaluate(&self, row: &crate::transaction::Row) -> Result<Option<String>> {
        self.text_pair((row, row))
    }
    fn integer(
        &self,
        rows: (&crate::transaction::Row, &crate::transaction::Row),
    ) -> Result<Option<i64>> {
        let value = match &self.value {
            Value::Column(column) => (if column.right { rows.1 } else { rows.0 })
                .get(&column.name)
                .context("missing typed scalar input")?
                .as_deref()
                .map(str::parse)
                .transpose()?,
            Value::Literal(value) => *value,
            Value::Numeric(_) => anyhow::bail!("numeric requires numeric evaluator"),
            Value::Cast(input) => input.integer(rows)?,
            Value::Case { branches, otherwise } => {
                let mut selected = otherwise.as_ref();
                for (condition, result) in branches {
                    if condition.evaluate(rows)? == Some(true) {
                        selected = result;
                        break;
                    }
                }
                selected.integer(rows)?
            }
            Value::Coalesce(arguments) => {
                let mut result = None;
                for argument in arguments {
                    result = argument.integer(rows)?;
                    if result.is_some() {
                        break;
                    }
                }
                result
            }
            Value::Binary { operation, left, right } => {
                let left = left.integer(rows)?;
                let right = right.integer(rows)?;
                match (left, right) {
                    (Some(left), Some(right)) => Some(
                        match operation.as_str() {
                            "+" => left.checked_add(right),
                            "-" => left.checked_sub(right),
                            "*" => left.checked_mul(right),
                            _ => anyhow::bail!("invalid typed scalar operation"),
                        }
                        .context("integral arithmetic overflow")?,
                    ),
                    _ => None,
                }
            }
        };
        if let Some(value) = value {
            check_width(value, self.oid)?;
        }
        Ok(value)
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
        _ => anyhow::bail!("unsupported integral width"),
    }
    Ok(())
}
pub(super) fn bind(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    use super::parser::expressions::{column, names, node, optional};
    ensure!(depth < 32, "typed scalar expression depth exceeded");
    match node(value)? {
        NodeEnum::CaseExpr(value) => case::bind(value, resolve, depth),
        NodeEnum::ColumnRef(_) => {
            let column = resolve(&column(value)?)?;
            ensure!(
                matches!(column.oid, 20 | 21 | 23 | 1700),
                "typed scalar requires integral column"
            );
            Ok(Typed { oid: column.oid, value: Value::Column(column) })
        }
        NodeEnum::AConst(constant) => literal(constant),
        NodeEnum::TypeCast(cast) => cast_expression(cast, resolve, depth),
        NodeEnum::AExpr(expression) => {
            ensure!(
                expression.kind == i32::from(pg_query::protobuf::AExprKind::AexprOp),
                "unsupported scalar operator kind"
            );
            let name = names(&expression.name)?.0;
            let [operation] = name.as_slice() else {
                anyhow::bail!("qualified scalar operator unsupported");
            };
            ensure!(matches!(operation.as_str(), "+" | "-" | "*"), "unsupported scalar operator");
            let left = bind(optional(expression.lexpr.as_deref())?, resolve, depth + 1)?;
            let right = bind(optional(expression.rexpr.as_deref())?, resolve, depth + 1)?;
            ensure!(left.oid != 0 || right.oid != 0, "unresolved scalar operator types");
            let oid = common(&[left.oid, right.oid]);
            Ok(Typed {
                oid,
                value: Value::Binary {
                    operation: operation.clone(),
                    left: Box::new(left),
                    right: Box::new(right),
                },
            })
        }
        NodeEnum::CoalesceExpr(expression) => {
            ensure!(
                expression.xpr.is_none() && (1..=64).contains(&expression.args.len()),
                "invalid scalar COALESCE"
            );
            let arguments = expression
                .args
                .iter()
                .map(|value| bind(value, resolve, depth + 1))
                .collect::<Result<Vec<_>>>()?;
            ensure!(
                arguments.iter().any(|value| value.oid != 0),
                "all-NULL circuit COALESCE requires an explicit integral cast"
            );
            let oid = common(&arguments.iter().map(|value| value.oid).collect::<Vec<_>>());
            Ok(Typed { oid, value: Value::Coalesce(arguments) })
        }
        _ => anyhow::bail!("unsupported circuit scalar expression"),
    }
}
pub(super) fn computed(
    value: &Node,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    computed: &mut Vec<super::scalar::Computed>,
    scope: Option<usize>,
) -> Result<ColumnRef> {
    use sha2::{Digest, Sha256};
    let typed = bind(value, resolve, 0)?;
    ensure!(
        matches!(typed.oid, 20 | 21 | 23 | 1700),
        "computed scalar requires resolved integral type"
    );
    let name = format!("@typed_{scope:?}_{:x}", Sha256::digest(serde_json::to_vec(&typed)?));
    let column = ColumnRef { name, right: false, oid: typed.oid, nullable: typed.nullable() };
    if !computed.iter().any(|value| value.column == column) {
        computed.push(super::scalar::Computed {
            column: column.clone(),
            expression: super::scalar::Native::Typed { typed },
        });
    }
    Ok(column)
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
            return Ok(Typed { oid: 20, value: Value::Literal(Some(integer)) });
        }
        crate::numeric::Numeric::parse(&value.fval)?;
        return Ok(Typed { oid: 1700, value: Value::Numeric(value.fval.clone()) });
    }
    let Some(pg_query::protobuf::a_const::Val::Ival(value)) = &constant.val else {
        anyhow::bail!("typed scalar requires integer literal");
    };
    Ok(Typed { oid: 23, value: Value::Literal(Some(i64::from(value.ival))) })
}

impl Typed {
    fn nullable(&self) -> bool {
        match &self.value {
            Value::Column(column) => column.nullable,
            Value::Literal(value) => value.is_none(),
            Value::Numeric(_) => false,
            Value::Cast(input) => input.nullable(),
            Value::Binary { left, right, .. } => left.nullable() || right.nullable(),
            Value::Coalesce(arguments) => arguments.iter().all(Self::nullable),
            Value::Case { branches, otherwise } => {
                otherwise.nullable() || branches.iter().any(|(_, value)| value.nullable())
            }
        }
    }
    pub(super) const fn oid(&self) -> u32 {
        self.oid
    }
    pub(super) fn evaluate_pair(
        &self,
        rows: (&crate::transaction::Row, &crate::transaction::Row),
    ) -> Result<Option<i64>> {
        self.integer(rows)
    }
}
pub(super) fn difference(left: ColumnRef, right: ColumnRef) -> Typed {
    Typed {
        oid: common(&[left.oid, right.oid]),
        value: Value::Binary {
            operation: "-".into(),
            left: Box::new(Typed { oid: left.oid, value: Value::Column(left) }),
            right: Box::new(Typed { oid: right.oid, value: Value::Column(right) }),
        },
    }
}

impl Typed {
    pub(super) fn text_pair(
        &self,
        rows: (&crate::transaction::Row, &crate::transaction::Row),
    ) -> Result<Option<String>> {
        if self.oid != 1700 {
            return self.integer(rows).map(|v| v.map(|v| v.to_string()));
        }
        let value = match &self.value {
            Value::Column(column) => (if column.right { rows.1 } else { rows.0 })
                .get(&column.name)
                .context("missing numeric input")?
                .clone(),
            Value::Literal(value) => value.map(|v| v.to_string()),
            Value::Numeric(value) => Some(value.clone()),
            Value::Cast(input) => input.text_pair(rows)?,
            Value::Binary { operation, left, right } => {
                let (Some(left), Some(right)) = (left.text_pair(rows)?, right.text_pair(rows)?)
                else {
                    return Ok(None);
                };
                Some(
                    crate::numeric::Numeric::parse(&left)?
                        .binary(&crate::numeric::Numeric::parse(&right)?, operation)?
                        .text(),
                )
            }
            Value::Coalesce(arguments) => {
                let mut value = None;
                for argument in arguments {
                    value = argument.text_pair(rows)?;
                    if value.is_some() {
                        break;
                    }
                }
                value
            }
            Value::Case { branches, otherwise } => {
                let mut selected = otherwise.as_ref();
                for (condition, result) in branches {
                    if condition.evaluate(rows)? == Some(true) {
                        selected = result;
                        break;
                    }
                }
                selected.text_pair(rows)?
            }
        };
        value.map(|v| Ok(crate::numeric::Numeric::parse(&v)?.text())).transpose()
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
        crate::numeric::Numeric::parse(&value.sval)?;
        return Ok(Typed { oid: 1700, value: Value::Numeric(value.sval.clone()) });
    }
    bind(value, resolve, depth)
}

fn cast_expression(
    cast: &pg_query::protobuf::TypeCast,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    use super::parser::expressions::{names, optional};
    let kind = cast.type_name.as_ref().context("missing scalar cast type")?;
    ensure!(
        crate::compiler::parser::expressions::unmodified_type(kind),
        "unsupported scalar cast modifier"
    );
    let name = names(&kind.names)?.0;
    let name = crate::compiler::parser::expressions::builtin_name(&name)
        .context("unsupported scalar cast namespace")?;
    let oid = match name {
        "int2" => 21,
        "int4" => 23,
        "int8" => 20,
        "numeric" => 1700,
        _ => anyhow::bail!("unsupported scalar cast"),
    };
    let argument = optional(cast.arg.as_deref())?;
    let input = if oid == 1700 {
        numeric_cast_input(argument, resolve, depth + 1)?
    } else {
        bind(argument, resolve, depth + 1)?
    };
    ensure!(oid == 1700 || input.oid != 1700, "numeric to integral circuit cast unsupported");
    Ok(Typed { oid, value: Value::Cast(Box::new(input)) })
}
