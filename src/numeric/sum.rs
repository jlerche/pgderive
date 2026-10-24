//! Reversible numeric SUM, including display scales and removable special values.
use super::{LIMIT, Numeric, power};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sum {
    coefficient: BigInt,
    scale: u32,
    scales: BTreeMap<u32, BigInt>,
    nan: BigInt,
    positive: BigInt,
    negative: BigInt,
}
impl Sum {
    pub(crate) fn measure(text: &str) -> Result<Self> {
        let mut value = Self::default();
        match Numeric::parse(text)? {
            Numeric::Finite { coefficient, scale } => {
                value.coefficient = coefficient;
                value.scale = scale;
                value.scales.insert(scale, 1.into());
            }
            Numeric::Nan => value.nan = 1.into(),
            Numeric::Infinity => value.positive = 1.into(),
            Numeric::NegativeInfinity => value.negative = 1.into(),
        }
        Ok(value)
    }
    pub(crate) fn add_scaled(&mut self, other: &Self, weight: i64) {
        let scale = self.scale.max(other.scale);
        self.coefficient = &self.coefficient * power(scale - self.scale)
            + &other.coefficient * power(scale - other.scale) * weight;
        self.scale = scale;
        for (scale, count) in &other.scales {
            let total = self.scales.entry(*scale).or_default();
            *total += count * weight;
            if *total == BigInt::default() {
                self.scales.remove(scale);
            }
        }
        self.nan += &other.nan * weight;
        self.positive += &other.positive * weight;
        self.negative += &other.negative * weight;
    }
    pub(crate) fn is_zero(&self) -> bool {
        self.coefficient == BigInt::default()
            && self.scales.is_empty()
            && self.nan == BigInt::default()
            && self.positive == BigInt::default()
            && self.negative == BigInt::default()
    }
    pub(crate) fn canonical(&self, count: &BigInt) -> Result<Self> {
        ensure!(usize::try_from(self.scale)? <= LIMIT, "numeric statistic scale exceeds budget");
        let mut total = &self.nan + &self.positive + &self.negative;
        for special in [&self.nan, &self.positive, &self.negative] {
            ensure!(special >= &BigInt::default(), "negative numeric special count");
        }
        for (scale, count) in &self.scales {
            ensure!(usize::try_from(*scale)? <= LIMIT, "numeric statistic scale exceeds budget");
            ensure!(count > &BigInt::default(), "nonpositive numeric scale count");
            total += count;
        }
        ensure!(&total == count, "numeric statistic non-NULL count mismatch");
        ensure!(
            !self.scales.is_empty() || self.coefficient == BigInt::default(),
            "numeric sum without finite inputs"
        );
        let mut result = self.clone();
        result.scale = self.scales.last_key_value().map_or(0, |(scale, _)| *scale);
        result.coefficient = if result.scale >= self.scale {
            &self.coefficient * power(result.scale - self.scale)
        } else {
            let divisor = power(self.scale - result.scale);
            ensure!(
                &self.coefficient % &divisor == BigInt::default(),
                "numeric statistic scale loses precision"
            );
            &self.coefficient / divisor
        };
        Ok(result)
    }
    pub(crate) fn result(&self, count: &BigInt) -> Result<Option<String>> {
        let value = self.canonical(count)?;
        if count == &BigInt::default() {
            return Ok(None);
        }
        if value.nan > BigInt::default()
            || (value.positive > BigInt::default() && value.negative > BigInt::default())
        {
            return Ok(Some("NaN".into()));
        }
        if value.positive > BigInt::default() {
            return Ok(Some("Infinity".into()));
        }
        if value.negative > BigInt::default() {
            return Ok(Some("-Infinity".into()));
        }
        Ok(Some(Numeric::finite(value.coefficient, value.scale)?.text()))
    }
    pub(crate) fn heap_bytes(&self) -> Result<usize> {
        // Charge a complete maximum-size BTree node per occupied entry, including
        // spare keys/values and child pointers. Conservative for sparse nodes.
        let node = std::mem::size_of::<(u32, BigInt)>()
            .checked_mul(12)
            .and_then(|bytes| bytes.checked_add(16 * std::mem::size_of::<usize>()))
            .context("numeric scale node size overflow")?;
        let tree =
            node.checked_mul(self.scales.len()).context("numeric scale tree size overflow")?;
        [&self.coefficient, &self.nan, &self.positive, &self.negative]
            .into_iter()
            .chain(self.scales.values())
            .try_fold(tree, |bytes, value| {
                bytes
                    .checked_add(
                        usize::try_from(value.bits().div_ceil(64))?
                            .checked_mul(8)
                            .context("numeric integer size overflow")?,
                    )
                    .context("numeric summary size overflow")
            })
    }
}

#[cfg(test)]
mod tests;
