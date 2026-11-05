//! Bounded exact `PostgreSQL` unconstrained numeric values.
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use serde::Serialize;
use std::cmp::Ordering;
mod division;
mod rounding;
pub mod sum;
pub use rounding::{Rounding, Typmod};
const LIMIT: usize = 4096;
#[derive(Clone, Debug, Serialize)]
pub enum Numeric {
    NegativeInfinity,
    Finite { coefficient: BigInt, scale: u32 },
    Infinity,
    Nan,
}
impl Numeric {
    /// Canonical order shared by scalar comparisons and resident SQL sort tokens.
    pub(crate) fn order_key(&self) -> String {
        let Self::Finite { coefficient, scale } = self else {
            return match self {
                Self::NegativeInfinity => "0",
                Self::Infinity => "4",
                _ => "5",
            }
            .into();
        };
        let text = coefficient.to_string();
        if coefficient == &BigInt::from(0) {
            return "2".into();
        }
        let negative = text.starts_with('-');
        let digits = text.strip_prefix('-').unwrap_or(&text);
        let exponent = digits.bytes().map(|_| 1_i64).sum::<i64>() - i64::from(*scale);
        let biased = u64::from_be_bytes(exponent.to_be_bytes()) ^ (1_u64 << 63);
        let payload = format!("{biased:016x}{}/", digits.trim_end_matches('0'));
        if negative {
            format!("1{}", payload.bytes().map(|byte| char::from(127 - byte)).collect::<String>())
        } else {
            format!("3{payload}")
        }
    }
    pub(crate) fn parse(text: &str) -> Result<Self> {
        Self::parse_bounded(text, LIMIT)
    }
    fn parse_bounded(text: &str, budget: usize) -> Result<Self> {
        match text {
            "NaN" => return Ok(Self::Nan),
            "Infinity" | "+Infinity" => return Ok(Self::Infinity),
            "-Infinity" => return Ok(Self::NegativeInfinity),
            _ => {}
        }
        ensure!(text.len() <= budget, "numeric input exceeds digit budget");
        let (mantissa, exponent) = text
            .split_once(['e', 'E'])
            .map_or(Ok((text, 0_i32)), |(m, e)| Ok::<_, anyhow::Error>((m, e.parse::<i32>()?)))?;
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let digits = format!("{whole}{fraction}");
        let unsigned = digits.strip_prefix(['-', '+']).unwrap_or(&digits);
        ensure!(
            !unsigned.is_empty() && unsigned.bytes().all(|v| v.is_ascii_digit()),
            "invalid numeric input"
        );
        let mut coefficient = digits.parse::<BigInt>()?;
        let scale = i64::try_from(fraction.len())? - i64::from(exponent);
        ensure!(scale.unsigned_abs() <= u64::try_from(budget)?, "numeric scale exceeds budget");
        let scale = if scale < 0 {
            coefficient *= power(u32::try_from(-scale)?);
            0
        } else {
            u32::try_from(scale)?
        };
        Self::finite_bounded(coefficient, scale, budget)
    }
    fn finite(coefficient: BigInt, scale: u32) -> Result<Self> {
        Self::finite_bounded(coefficient, scale, LIMIT)
    }
    fn finite_bounded(coefficient: BigInt, scale: u32, budget: usize) -> Result<Self> {
        ensure!(
            coefficient.to_string().len() <= budget && usize::try_from(scale)? <= budget,
            "numeric result exceeds digit budget"
        );
        let value = Self::Finite { coefficient, scale };
        ensure!(value.text().len() <= budget, "numeric display exceeds budget");
        Ok(value)
    }
    pub(crate) fn text(&self) -> String {
        let Self::Finite { coefficient, scale } = self else {
            return match self {
                Self::NegativeInfinity => "-Infinity",
                Self::Infinity => "Infinity",
                _ => "NaN",
            }
            .into();
        };
        let text = coefficient.to_string();
        if *scale == 0 {
            return text;
        }
        let (sign, digits) = text.strip_prefix('-').map_or(("", text.as_str()), |v| ("-", v));
        let scale = usize::try_from(*scale).unwrap_or(LIMIT);
        let padded = format!("{:0>width$}", digits, width = scale + 1);
        let split = padded.len() - scale;
        format!("{sign}{}.{}", &padded[..split], &padded[split..])
    }
    pub(crate) fn key(&self) -> String {
        let Self::Finite { coefficient, scale } = self else {
            return self.text();
        };
        let mut coefficient = coefficient.clone();
        let mut scale = *scale;
        while scale > 0 && &coefficient % 10 == BigInt::from(0) {
            coefficient /= 10;
            scale -= 1;
        }
        Self::Finite { coefficient, scale }.text()
    }
    pub(crate) fn binary(&self, other: &Self, operation: &str) -> Result<Self> {
        if operation == "/" {
            return self.divide(other);
        }
        ensure!(matches!(operation, "+" | "-" | "*"), "unsupported numeric operation");
        if operation == "-" {
            return self.binary(&other.negated(), "+");
        }
        if let (
            Self::Finite { coefficient: left, scale: ls },
            Self::Finite { coefficient: right, scale: rs },
        ) = (self, other)
        {
            return if operation == "*" {
                Self::finite(left * right, ls.checked_add(*rs).context("numeric scale overflow")?)
            } else {
                let scale = (*ls).max(*rs);
                Self::finite(left * power(scale - ls) + right * power(scale - rs), scale)
            };
        }
        if matches!(self, Self::Nan) || matches!(other, Self::Nan) {
            return Ok(Self::Nan);
        }
        if operation == "+" {
            return Ok(match (self, other) {
                (Self::NegativeInfinity, Self::Infinity)
                | (Self::Infinity, Self::NegativeInfinity) => Self::Nan,
                (Self::NegativeInfinity, _) | (_, Self::NegativeInfinity) => Self::NegativeInfinity,
                _ => Self::Infinity,
            });
        }
        let zero = Self::parse("0")?;
        if self == &zero || other == &zero {
            return Ok(Self::Nan);
        }
        Ok(if (self < &zero) == (other < &zero) { Self::Infinity } else { Self::NegativeInfinity })
    }
    fn negated(&self) -> Self {
        match self {
            Self::Finite { coefficient, scale } => {
                Self::Finite { coefficient: -coefficient, scale: *scale }
            }
            Self::NegativeInfinity => Self::Infinity,
            Self::Infinity => Self::NegativeInfinity,
            Self::Nan => Self::Nan,
        }
    }
}
fn power(scale: u32) -> BigInt {
    BigInt::from(10).pow(scale)
}
impl PartialEq for Numeric {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Numeric {}
impl PartialOrd for Numeric {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Numeric {
    fn cmp(&self, other: &Self) -> Ordering {
        self.order_key().cmp(&other.order_key())
    }
}
impl Numeric {
    pub(crate) fn average(ratio: &str) -> Result<Self> {
        let (sum, count) = ratio.split_once('/').context("invalid engine AVG operand")?;
        let denominator = count.parse::<i64>()?;
        ensure!(denominator > 0, "invalid engine AVG count");
        // Aligned sums can span both input digit extremes plus an i64 count;
        // only the final quotient must fit the public result/display budget.
        Self::parse_bounded(sum, 2 * LIMIT + 32)?
            .divide(&Self::Finite { coefficient: denominator.into(), scale: 0 })
    }
}

#[cfg(test)]
mod tests {
    use super::Numeric;
    use anyhow::Result;
    #[test]
    fn canonical_order_matches_independent_scaled_integer_comparison() -> Result<()> {
        let mut values = Vec::new();
        for coefficient in
            [-1001, -1000, -101, -100, -11, -10, -1, 0, 1, 9, 10, 11, 99, 100, 101, 1000, 1001]
        {
            for scale in 0..=6 {
                values.push(Numeric::parse(&format!("{coefficient}e-{scale}"))?);
            }
        }
        for text in ["1e4090", "1e-4090", "-1e4090", "-1e-4090", "-Infinity", "Infinity", "NaN"] {
            values.push(Numeric::parse(text)?);
        }
        let rank = |value: &Numeric| match value {
            Numeric::NegativeInfinity => 0,
            Numeric::Finite { .. } => 1,
            Numeric::Infinity => 2,
            Numeric::Nan => 3,
        };
        for a in &values {
            for b in &values {
                let expected = if let (
                    Numeric::Finite { coefficient: a, scale: sa },
                    Numeric::Finite { coefficient: b, scale: sb },
                ) = (a, b)
                {
                    let scale = (*sa).max(*sb);
                    (a * super::power(scale - sa)).cmp(&(b * super::power(scale - sb)))
                } else {
                    rank(a).cmp(&rank(b))
                };
                assert_eq!(a.cmp(b), expected);
            }
        }
        Ok(())
    }
    #[test]
    fn exact_numeric_keys_order_scale_and_limits() -> Result<()> {
        for (left, right) in [("1.00", "1"), ("0.000", "0"), ("1e2", "100"), ("-0.00", "0")] {
            let left = Numeric::parse(left)?;
            let right = Numeric::parse(right)?;
            assert_eq!(left, right);
            assert_eq!(left.key(), right.key());
        }
        let order = ["-Infinity", "-10000", "-0.01", "0", "0.001", "10000", "Infinity", "NaN"];
        for pair in order.windows(2) {
            assert!(Numeric::parse(pair[0])? < Numeric::parse(pair[1])?);
        }
        for value in ["", ".", "1.2.3", "1e100000", "1e-100000"] {
            assert!(Numeric::parse(value).is_err());
        }
        assert!(Numeric::parse(&"1".repeat(4097)).is_err());
        assert_eq!(
            Numeric::parse("-1.20")?.binary(&Numeric::parse("2.300")?, "+")?.text(),
            "1.100"
        );
        assert_eq!(
            Numeric::parse("-Infinity")?.binary(&Numeric::parse("Infinity")?, "+")?.text(),
            "NaN"
        );
        assert_eq!(
            Numeric::parse("Infinity")?.binary(&Numeric::parse("-1")?, "*")?.text(),
            "-Infinity"
        );
        assert!(Numeric::parse("1e4095")?.binary(&Numeric::parse("100")?, "*").is_err());
        Ok(())
    }
}

#[cfg(test)]
mod tests_arithmetic;
