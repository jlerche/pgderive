//! One exact decimal quantizer for rounding functions and numeric/integer casts.
use super::{LIMIT, Numeric, power};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
#[derive(Clone, Copy)]
pub enum Rounding {
    Nearest,
    Truncate,
    Ceiling,
    Floor,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub struct Typmod {
    pub(crate) precision: u16,
    pub(crate) scale: i16,
}
impl Typmod {
    pub(crate) fn new(precision: i64, scale: i64) -> Result<Self> {
        ensure!(
            (1..=1000).contains(&precision) && (-1000..=1000).contains(&scale),
            "numeric precision must be 1..1000 and scale -1000..1000"
        );
        Ok(Self { precision: u16::try_from(precision)?, scale: i16::try_from(scale)? })
    }
    pub(crate) fn from_encoded(value: i32) -> Result<Self> {
        let value = value.checked_sub(4).context("invalid numeric type modifier")?;
        ensure!(value >= 0, "invalid numeric type modifier");
        let scale = ((value & 0x7ff) ^ 0x400) - 0x400;
        Self::new(i64::from(value >> 16), i64::from(scale))
    }
    pub(crate) fn encoded(self) -> i32 {
        ((i32::from(self.precision) << 16) | (i32::from(self.scale) & 0x7ff)) + 4
    }
}
impl Numeric {
    pub(crate) fn rounded(&self, scale: i32, mode: Rounding) -> Result<Self> {
        let Self::Finite { coefficient, scale: prior } = self else {
            return Ok(self.clone());
        };
        ensure!(
            i64::from(scale) <= i64::try_from(LIMIT)?,
            "numeric rounding scale exceeds digit budget"
        );
        let delta = i64::from(*prior) - i64::from(scale);
        if delta <= 0 {
            return Self::finite(
                coefficient * power(u32::try_from(-delta)?),
                u32::try_from(scale)?,
            );
        }
        let digits = coefficient.to_string().len();
        if delta > i64::try_from(digits)? + 1 {
            let coefficient = match mode {
                Rounding::Ceiling if coefficient > &BigInt::default() => BigInt::from(1),
                Rounding::Floor if coefficient < &BigInt::default() => BigInt::from(-1),
                _ => BigInt::default(),
            };
            return finish(coefficient, scale);
        }
        let divisor = power(u32::try_from(delta)?);
        let mut rounded = coefficient / &divisor;
        let remainder = coefficient % &divisor;
        if remainder != BigInt::default() {
            let negative = coefficient < &BigInt::default();
            let change = match mode {
                Rounding::Nearest => (if negative { -remainder } else { remainder }) * 2 >= divisor,
                Rounding::Truncate => false,
                Rounding::Ceiling => !negative,
                Rounding::Floor => negative,
            };
            if change {
                rounded += if negative { -1 } else { 1 };
            }
        }
        finish(rounded, scale)
    }
    pub(crate) fn constrained(&self, typmod: Typmod) -> Result<Self> {
        ensure!(
            !matches!(self, Self::Infinity | Self::NegativeInfinity),
            "numeric field overflow: infinity in constrained numeric"
        );
        let result = self.rounded(i32::from(typmod.scale), Rounding::Nearest)?;
        if let Self::Finite { coefficient, scale } = &result {
            let exponent =
                i32::try_from(*scale)? + i32::from(typmod.precision) - i32::from(typmod.scale);
            let absolute =
                if coefficient < &BigInt::default() { -coefficient } else { coefficient.clone() };
            ensure!(
                absolute == BigInt::default()
                    || (exponent >= 0 && absolute < power(u32::try_from(exponent)?)),
                "numeric field overflow"
            );
        }
        Ok(result)
    }
    pub(crate) fn integer(&self) -> Result<i64> {
        let Self::Finite { coefficient, .. } = self.rounded(0, Rounding::Nearest)? else {
            anyhow::bail!("cannot convert numeric special to integer")
        };
        i64::try_from(coefficient).context("numeric to integer overflow")
    }
    pub(crate) fn absolute(&self) -> Self {
        if self < &(Self::Finite { coefficient: BigInt::default(), scale: 0 }) {
            self.negated()
        } else {
            self.clone()
        }
    }
}

fn finish(coefficient: BigInt, scale: i32) -> Result<Numeric> {
    if scale >= 0 {
        return Numeric::finite(coefficient, u32::try_from(scale)?);
    }
    if coefficient == BigInt::default() {
        return Numeric::finite(coefficient, 0);
    }
    let exponent = u32::try_from(-i64::from(scale))?;
    ensure!(usize::try_from(exponent)? <= LIMIT, "numeric rounded result exceeds digit budget");
    Numeric::finite(coefficient * power(exponent), 0)
}
