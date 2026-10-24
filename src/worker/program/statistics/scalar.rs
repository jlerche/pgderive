//! Canonical mixed integer/numeric scalar codec for one additive SQL operator.
use crate::{
    compiler::partition::{Aggregate, Function},
    engine::dataflow::Additive,
    numeric::sum::Sum,
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
#[derive(Clone, Serialize, Deserialize)]
pub(super) enum Scalar {
    Integer(BigInt),
    Numeric(Sum),
}
impl Default for Scalar {
    fn default() -> Self {
        Self::Integer(BigInt::default())
    }
}
impl Scalar {
    pub(super) fn integer(&self) -> Result<&BigInt> {
        match self {
            Self::Integer(value) => Ok(value),
            Self::Numeric(_) => anyhow::bail!("expected integer statistic"),
        }
    }
}
impl Additive for Scalar {
    fn add_scaled(&mut self, other: &Self, weight: i64) -> Result<()> {
        if let Self::Integer(value) = self
            && value == &BigInt::default()
            && matches!(other, Self::Numeric(_))
        {
            *self = Self::Numeric(Sum::default());
        }
        match (self, other) {
            (Self::Integer(total), Self::Integer(value)) => *total += value * weight,
            (Self::Numeric(total), Self::Numeric(value)) => total.add_scaled(value, weight),
            (Self::Numeric(_), Self::Integer(value)) if value == &BigInt::default() => {}
            _ => anyhow::bail!("incompatible statistic scalar domains"),
        }
        Ok(())
    }
    fn is_zero(&self) -> bool {
        match self {
            Self::Integer(value) => value == &BigInt::default(),
            Self::Numeric(value) => value.is_zero(),
        }
    }
}
impl super::super::native::NativeValue for Scalar {
    fn heap_bytes(&self) -> Result<usize> {
        match self {
            Self::Integer(value) => value.heap_bytes(),
            Self::Numeric(value) => value.heap_bytes(),
        }
    }
}
pub(super) fn numeric(aggregate: &Aggregate) -> bool {
    matches!(aggregate.function, Function::Sum)
        && aggregate.argument.as_ref().is_some_and(|arg| arg.oid == 1700)
}
pub(super) fn decode_fields(row: &Row, width: usize) -> Result<Vec<Scalar>> {
    let values = (0..width)
        .map(|index| {
            let text = row
                .get(&super::field(index))
                .and_then(Option::as_deref)
                .context("missing persisted statistic")?;
            if text.starts_with('{') {
                let value: Sum = serde_json::from_str(text)?;
                ensure!(serde_json::to_string(&value)? == text, "noncanonical numeric statistic");
                Ok(Scalar::Numeric(value))
            } else {
                Ok(Scalar::Integer(text.parse()?))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    validate(&values)?;
    Ok(values)
}
pub(super) fn encode_fields(values: &[Scalar]) -> Result<Row> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let text = match value {
                Scalar::Integer(value) => value.to_string(),
                Scalar::Numeric(value) => serde_json::to_string(value)?,
            };
            Ok((super::field(index), Some(text)))
        })
        .collect()
}
pub(super) fn validate(values: &[Scalar]) -> Result<bool> {
    let rows = values.first().context("missing statistics row count")?.integer()?;
    ensure!(rows >= &BigInt::default(), "negative statistics group multiplicity");
    for pair in values[1..].chunks_exact(2) {
        let count = pair[0].integer()?;
        ensure!(count >= &BigInt::default() && count <= rows, "invalid statistics non-NULL count");
        match &pair[1] {
            Scalar::Integer(sum) => ensure!(
                count != &BigInt::default() || sum == &BigInt::default(),
                "statistics sum without contributions"
            ),
            Scalar::Numeric(sum) => {
                sum.canonical(count)?;
            }
        }
    }
    Ok(rows != &BigInt::default())
}
pub(super) fn canonical(values: &[Scalar]) -> Result<Vec<Scalar>> {
    let mut result = values.to_vec();
    for pair in result[1..].chunks_exact_mut(2) {
        if let Scalar::Numeric(sum) = &pair[1] {
            pair[1] = Scalar::Numeric(sum.canonical(pair[0].integer()?)?);
        }
    }
    Ok(result)
}
