//! Integral bitwise operators and `PostgreSQL` shift widths.
use super::{Arithmetic, Typed, Value, numeric_common};
use anyhow::{Context, Result, ensure};

pub(super) fn bind(
    operation: Arithmetic,
    complement: bool,
    left: Option<Typed>,
    right: Typed,
) -> Result<Typed> {
    if complement {
        ensure!(left.is_none() && integral(right.oid), "bitwise complement requires an integer");
        let oid = right.oid;
        let mask = Typed { oid, value: Value::Literal(Some(-1)) };
        return Ok(binary(operation, mask, right, oid));
    }
    let left = left.context("bitwise operator requires two operands")?;
    let oid = if operation.is_shift() {
        ensure!(
            matches!(right.oid, 0 | 21 | 23 | 705)
                && (integral(left.oid) || (matches!(left.oid, 0 | 705) && right.oid == 23)),
            "shift requires an integral value and an integer count"
        );
        if integral(left.oid) { left.oid } else { 23 }
    } else {
        ensure!(
            [left.oid, right.oid].iter().all(|oid| integral(*oid) || matches!(oid, 0 | 705))
                && (integral(left.oid) || integral(right.oid)),
            "bitwise operator requires integral operands"
        );
        numeric_common(&[left.oid, right.oid])
    };
    let count_oid = if operation.is_shift() { 23 } else { oid };
    Ok(binary(operation, contextual(left, oid)?, contextual(right, count_oid)?, oid))
}

const fn integral(oid: u32) -> bool {
    matches!(oid, 20 | 21 | 23)
}

fn contextual(value: Typed, oid: u32) -> Result<Typed> {
    if matches!(value.oid, 0 | 705) { Typed::contextual_literal(value, oid) } else { Ok(value) }
}

fn binary(operation: Arithmetic, left: Typed, right: Typed, oid: u32) -> Typed {
    Typed { oid, value: Value::Binary { operation, left: Box::new(left), right: Box::new(right) } }
}

pub(super) fn shift(operation: Arithmetic, left: i64, right: i64, oid: u32) -> Result<i64> {
    let count = i32::try_from(right)?;
    let width = if oid == 20 { 64 } else { 32 };
    ensure!(
        cfg!(target_arch = "x86_64") || (0..width).contains(&count),
        "negative and oversized shift counts require x86-64 PostgreSQL semantics"
    );
    // PostgreSQL's x86-64 shifts mask the count to the instruction width.
    let bits = u32::from_ne_bytes(count.to_ne_bytes());
    let left_shift = operation == Arithmetic::ShiftLeft;
    if oid == 20 {
        return Ok(if left_shift { left.wrapping_shl(bits) } else { left.wrapping_shr(bits) });
    }
    let left = i32::try_from(left)?;
    let shifted = if left_shift { left.wrapping_shl(bits) } else { left.wrapping_shr(bits) };
    if oid == 21 {
        // C promotes smallint to int before shifting and retains the low 16 bits.
        let [a, b, ..] = shifted.to_le_bytes();
        return Ok(i64::from(i16::from_le_bytes([a, b])));
    }
    Ok(i64::from(shifted))
}
