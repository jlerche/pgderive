//! Resolve integral distance offsets using the integer btree operator family.
use crate::compiler::{
    parser::partition::RangeBound,
    partition::{Frame, Order},
    typed::Typed,
};
use anyhow::{Context, Result, ensure};

pub(super) fn bind(start: RangeBound, end: RangeBound, order: &[Order]) -> Result<Frame> {
    let [key] = order else {
        anyhow::bail!("RANGE offsets require exactly one ORDER BY expression");
    };
    let oid = key.column.oid;
    ensure!(matches!(oid, 20 | 21 | 23), "RANGE offsets require an integral ORDER BY expression");
    Ok(Frame { start: offset(start, oid)?, end: offset(end, oid)? })
}

fn offset(bound: RangeBound, key: u32) -> Result<Option<i64>> {
    let (value, preceding) = match bound {
        RangeBound::Unbounded => return Ok(None),
        RangeBound::Current => return Ok(Some(0)),
        RangeBound::Offset { value, preceding } => (value, preceding),
    };
    let value = Typed::bind_contextual(
        &value,
        &|_| anyhow::bail!("RANGE offset requires a constant expression"),
        0,
    )?;
    let target = match value.oid() {
        0 | 705 => key,
        20 | 21 | 23 if key == 20 => 20,
        oid @ (20 | 21 | 23) => oid,
        _ => anyhow::bail!("RANGE offset requires an integral type"),
    };
    let distance = value
        .coerce_common(target)?
        .fold()?
        .evaluate(&crate::transaction::Row::new())?
        .context("RANGE offset must not be NULL")?
        .parse::<i64>()?;
    ensure!(distance >= 0, "invalid preceding or following size in window function");
    Ok(Some(if preceding { -distance } else { distance }))
}
