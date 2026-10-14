//! Bounded exact `PostgreSQL` unconstrained numeric values.
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use serde::Serialize;
use std::cmp::Ordering;
const LIMIT: usize = 4096;
#[derive(Clone, Debug, Serialize)]
pub enum Numeric {
    NegativeInfinity,
    Finite { coefficient: BigInt, scale: u32 },
    Infinity,
    Nan,
}
impl Numeric {
    pub(crate) fn parse(text: &str) -> Result<Self> {
        match text {
            "NaN" => return Ok(Self::Nan),
            "Infinity" | "+Infinity" => return Ok(Self::Infinity),
            "-Infinity" => return Ok(Self::NegativeInfinity),
            _ => {}
        }
        ensure!(text.len() <= LIMIT, "numeric input exceeds digit budget");
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
        ensure!(scale.unsigned_abs() <= u64::try_from(LIMIT)?, "numeric scale exceeds budget");
        let scale = if scale < 0 {
            coefficient *= power(u32::try_from(-scale)?);
            0
        } else {
            u32::try_from(scale)?
        };
        Self::finite(coefficient, scale)
    }
    fn finite(coefficient: BigInt, scale: u32) -> Result<Self> {
        ensure!(
            coefficient.to_string().len() <= LIMIT && usize::try_from(scale)? <= LIMIT,
            "numeric result exceeds digit budget"
        );
        let value = Self::Finite { coefficient, scale };
        ensure!(value.text().len() <= LIMIT, "numeric display exceeds budget");
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
        if let (
            Self::Finite { coefficient: left, scale: ls },
            Self::Finite { coefficient: right, scale: rs },
        ) = (self, other)
        {
            let scale = (*ls).max(*rs);
            return (left * power(scale - ls)).cmp(&(right * power(scale - rs)));
        }
        let rank = |value: &Self| match value {
            Self::NegativeInfinity => 0,
            Self::Finite { .. } => 1,
            Self::Infinity => 2,
            Self::Nan => 3,
        };
        rank(self).cmp(&rank(other))
    }
}
#[cfg(test)]
mod tests {
    use super::Numeric;
    use anyhow::Result;
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
