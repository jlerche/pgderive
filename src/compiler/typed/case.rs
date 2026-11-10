//! Lazy CASE with separate result type resolution and one evaluation of a simple test.
use super::{ColumnRef, Name, Typed, Value, visit::Replace};
use crate::{
    compiler::{
        expression,
        parser::expressions::{node, optional},
        value::Value as Native,
    },
    transaction::Row,
};
use anyhow::{Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(super) struct Simple {
    pub(super) test: Box<Typed>,
    pub(super) branches: Vec<(Typed, Typed)>,
    pub(super) otherwise: Box<Typed>,
}

pub(super) fn bind(
    value: &pg::CaseExpr,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    ensure!(value.xpr.is_none(), "invalid CASE metadata");
    ensure!((1..=32).contains(&value.args.len()), "invalid CASE branch count");
    let depth = depth + 1;
    let otherwise = value
        .defresult
        .as_deref()
        .map(|value| super::bind(value, resolve, depth))
        .transpose()?
        .unwrap_or(Typed { oid: 0, value: Value::Literal(None) });
    if let Some(test) = value.arg.as_deref() {
        return simple(test, &value.args, otherwise, resolve, depth);
    }
    let branches = arms(&value.args, resolve, depth, |test| expression::bind(test, resolve))?;
    let types =
        branches.iter().map(|(_, value)| value.oid).chain([otherwise.oid]).collect::<Vec<_>>();
    ensure!(types.iter().any(|oid| *oid != 0), "circuit CASE needs resolved integral result");
    Ok(Typed {
        oid: super::types::common(&types)?,
        value: Value::Case { branches, otherwise: Box::new(otherwise) },
    })
}

fn arms<T>(
    values: &[Node],
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
    condition: impl Fn(&Node) -> Result<T>,
) -> Result<Vec<(T, Typed)>> {
    values
        .iter()
        .map(|arm| {
            let NodeEnum::CaseWhen(arm) = node(arm)? else {
                anyhow::bail!("invalid CASE arm");
            };
            ensure!(arm.xpr.is_none(), "invalid CASE arm metadata");
            Ok((
                condition(optional(arm.expr.as_deref())?)?,
                super::bind(optional(arm.result.as_deref())?, resolve, depth)?,
            ))
        })
        .collect()
}

fn simple(
    test: &Node,
    values: &[Node],
    otherwise: Typed,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    let mut test = super::operand::bind_at(test, resolve, depth)?.into_value();
    if matches!(test.oid, 0 | 705) {
        test = Typed::contextual_literal(test, 25)?;
    }
    let branches = arms(values, resolve, depth, |value| {
        expression::case_match(&test, super::operand::bind_at(value, resolve, depth)?)
    })?;
    // ELSE is the first candidate for PostgreSQL's common result type.
    let known = std::iter::once(&otherwise)
        .chain(branches.iter().map(|(_, result)| result))
        .map(|result| result.oid)
        .filter(|oid| !matches!(oid, 0 | 705))
        .collect::<Vec<_>>();
    let oid = result_type(&known)?;
    let branches = branches
        .into_iter()
        .map(|(candidate, result)| Ok((candidate, result_cast(result, oid)?)))
        .collect::<Result<_>>()?;
    Ok(Typed {
        oid,
        value: Value::SimpleCase(Simple {
            test: Box::new(test),
            branches,
            otherwise: Box::new(result_cast(otherwise, oid)?),
        }),
    })
}

fn result_type(known: &[u32]) -> Result<u32> {
    if let Some(first) = known.first()
        && (known.iter().all(|oid| oid == first)
            || known.iter().all(|oid| matches!(oid, 25 | 1043)))
    {
        return Ok(*first);
    }
    if known.iter().all(|oid| matches!(oid, 1082 | 1114 | 1184))
        && let Some(oid) = [1184, 1114, 1082].into_iter().find(|oid| known.contains(oid))
    {
        return Ok(oid);
    }
    super::types::common(known)
}

fn result_cast(input: Typed, oid: u32) -> Result<Typed> {
    if input.oid == oid {
        return Ok(input);
    }
    if matches!(input.oid, 0 | 705) {
        return Typed::contextual_literal(input, oid);
    }
    Ok(Typed { oid, value: Value::Cast { input: Box::new(input), typmod: None } })
}

impl Simple {
    pub(super) fn results(&self) -> impl Iterator<Item = &Typed> {
        std::iter::once(self.otherwise.as_ref())
            .chain(self.branches.iter().map(|(_, result)| result))
    }

    pub(super) fn is_constant(&self) -> bool {
        (self.branches.is_empty() || self.test.is_constant())
            && self
                .branches
                .iter()
                .all(|(candidate, result)| candidate.is_constant() && result.is_constant())
            && self.otherwise.is_constant()
    }

    pub(super) fn evaluate(&self, rows: (&Row, &Row)) -> Result<Option<Native>> {
        if !self.branches.is_empty() {
            let test = self.test.native_pair(rows)?;
            for (candidate, result) in &self.branches {
                let candidate = candidate.native_pair(rows)?;
                if equal(test.as_ref(), candidate.as_ref())? {
                    return result.native_pair(rows);
                }
            }
        }
        self.otherwise.native_pair(rows)
    }

    pub(super) fn fold(&self) -> Result<Self> {
        let mut test = self.test.fold()?;
        let rows = (&Row::new(), &Row::new());
        let constant = test.is_constant().then(|| test.native_pair(rows)).transpose()?;
        let mut branches = Vec::new();
        let mut otherwise = self.otherwise.as_ref();
        for (candidate, result) in &self.branches {
            let candidate = candidate.fold()?;
            let constant_candidate =
                candidate.is_constant().then(|| candidate.native_pair(rows)).transpose()?;
            if matches!(constant_candidate, Some(None)) || matches!(constant, Some(None)) {
                continue;
            }
            if let (Some(test), Some(candidate)) = (&constant, &constant_candidate) {
                if equal(test.as_ref(), candidate.as_ref())? {
                    otherwise = result;
                    break;
                }
                continue;
            }
            branches.push((candidate, result.fold()?));
        }
        if branches.is_empty() {
            test.value = Value::Literal(None);
        }
        Ok(Self { test: Box::new(test), branches, otherwise: Box::new(otherwise.fold()?) })
    }

    pub(super) fn replaced(&self, replace: &Replace<'_>) -> Option<Self> {
        Some(Self {
            test: Box::new(self.test.replaced(replace)?),
            branches: self
                .branches
                .iter()
                .map(|(candidate, result)| {
                    Some((candidate.replaced(replace)?, result.replaced(replace)?))
                })
                .collect::<Option<_>>()?,
            otherwise: Box::new(self.otherwise.replaced(replace)?),
        })
    }

    pub(super) fn cost(&self) -> u64 {
        self.branches.iter().fold(
            self.test.cost().saturating_add(self.otherwise.cost()),
            |cost, (candidate, result)| {
                let condition = if self.test.oid == 16
                    && (self.test.is_constant() || candidate.is_constant())
                {
                    candidate.cost()
                } else {
                    Typed::comparison_cost(&self.test, candidate)
                };
                cost.saturating_add(condition).saturating_add(result.cost())
            },
        )
    }
}

fn equal(left: Option<&Native>, right: Option<&Native>) -> Result<bool> {
    match (left, right) {
        (Some(left), Some(right)) => Ok(expression::compare_order(left, right)?.is_eq()),
        _ => Ok(false),
    }
}
