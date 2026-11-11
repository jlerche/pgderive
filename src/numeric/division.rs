//! Exact numeric division with `PostgreSQL` rounding or an integral truncated quotient.
use super::{Numeric, power};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;

fn leading(coefficient: &BigInt, scale: u32) -> Result<(i32, u32)> {
    if coefficient == &BigInt::default() {
        return Ok((0, 0));
    }
    let text = coefficient.to_string();
    let digits = text.strip_prefix('-').unwrap_or(&text);
    let exponent = i32::try_from(digits.len())? - i32::try_from(scale)?;
    let weight = (exponent - 1).div_euclid(4);
    let width = usize::try_from(exponent - 4 * weight)?;
    let first = format!("{:0<width$}", &digits[..width.min(digits.len())]);
    Ok((weight, first.parse()?))
}
fn result_scale(left: (&BigInt, u32), right: (&BigInt, u32)) -> Result<u32> {
    let (lw, ld) = leading(left.0, left.1)?;
    let (rw, rd) = leading(right.0, right.1)?;
    let weight = lw - rw - i32::from(ld <= rd);
    Ok(u32::try_from(
        (16 - 4 * weight).max(i32::try_from(left.1)?).max(i32::try_from(right.1)?).clamp(0, 1000),
    )?)
}
impl Numeric {
    pub(crate) fn divide(&self, other: &Self) -> Result<Self> {
        self.divide_with(other, false)
    }

    pub(crate) fn divide_truncated(&self, other: &Self) -> Result<Self> {
        self.divide_with(other, true)
    }

    fn divide_with(&self, other: &Self, truncate: bool) -> Result<Self> {
        if matches!(self, Self::Nan) || matches!(other, Self::Nan) {
            return Ok(Self::Nan);
        }
        if matches!(other, Self::Infinity | Self::NegativeInfinity) {
            return if matches!(self, Self::Infinity | Self::NegativeInfinity) {
                Ok(Self::Nan)
            } else {
                Self::finite(BigInt::default(), 0)
            };
        }
        let Self::Finite { coefficient: right, scale: rs } = other else {
            anyhow::bail!("invalid numeric divisor")
        };
        ensure!(right != &BigInt::default(), "division by zero");
        let Self::Finite { coefficient: left, scale: ls } = self else {
            return Ok(if matches!(self, Self::NegativeInfinity) == (right < &BigInt::default()) {
                Self::Infinity
            } else {
                Self::NegativeInfinity
            });
        };
        let scale = if truncate { 0 } else { result_scale((left, *ls), (right, *rs))? };
        let exponent = i64::from(*rs) + i64::from(scale) - i64::from(*ls);
        let (numerator, denominator) = if exponent >= 0 {
            (left * power(u32::try_from(exponent)?), right.clone())
        } else {
            (left.clone(), right * power(u32::try_from(-exponent)?))
        };
        let mut quotient = &numerator / &denominator;
        if !truncate {
            let remainder = &numerator % &denominator;
            let absolute =
                |value: &BigInt| if value < &BigInt::default() { -value } else { value.clone() };
            if absolute(&remainder) * 2 >= absolute(&denominator) {
                quotient += if (numerator < BigInt::default()) == (denominator < BigInt::default())
                {
                    1
                } else {
                    -1
                };
            }
        }
        Self::finite(quotient, scale).context("numeric division result")
    }
}
