//! Resolve distance offsets using the ordering type’s btree operator family.
use crate::compiler::{
    parser::partition::RangeBound,
    partition::{Frame, Mode, Order, TemporalRangeFrame, TemporalRangeOffset},
    typed::Typed,
};
use crate::temporal::Interval;
use anyhow::{Context, Result, ensure};

pub(super) fn bind(
    start: RangeBound,
    end: RangeBound,
    order: Vec<Order>,
    ranking: bool,
) -> Result<Mode> {
    let [key] = order.as_slice() else {
        anyhow::bail!("RANGE offsets require exactly one ORDER BY expression");
    };
    let oid = key.column.oid;
    let mode = match oid {
        20 | 21 | 23 => Mode::Range {
            frame: Frame { start: offset(start, oid)?, end: offset(end, oid)? },
            order,
        },
        1114 | 1184 => Mode::TemporalRange {
            frame: TemporalRangeFrame { start: interval(start)?, end: interval(end)? },
            order,
        },
        _ => anyhow::bail!("RANGE offsets require an integral or timestamp ORDER BY expression"),
    };
    if ranking {
        let (Mode::Range { order, .. } | Mode::TemporalRange { order, .. }) = mode else {
            anyhow::bail!("invalid RANGE ranking mode");
        };
        Ok(Mode::Ranking { order })
    } else {
        Ok(mode)
    }
}

fn constant(value: &pg_query::Node) -> Result<Typed> {
    Typed::bind_contextual(
        value,
        &|_| anyhow::bail!("RANGE offset requires a constant expression"),
        0,
    )
}

fn interval(bound: RangeBound) -> Result<Option<TemporalRangeOffset>> {
    let (value, preceding) = match bound {
        RangeBound::Unbounded => return Ok(None),
        RangeBound::Current => return Ok(Some(TemporalRangeOffset::Current)),
        RangeBound::Offset { value, preceding } => (value, preceding),
    };
    let value = constant(&value)?;
    ensure!(matches!(value.oid(), 0 | 705 | 1186), "timestamp RANGE offset requires an interval");
    let value = evaluated(value, 1186)?;
    ensure!(
        Interval::parse(&value)? >= Interval::duration(0),
        "invalid preceding or following size in window function"
    );
    Ok(Some(TemporalRangeOffset::Interval { value, preceding }))
}

fn evaluated(value: Typed, oid: u32) -> Result<String> {
    value
        .coerce_common(oid)?
        .fold()?
        .evaluate(&crate::transaction::Row::new())?
        .context("RANGE offset must not be NULL")
}

fn offset(bound: RangeBound, key: u32) -> Result<Option<i64>> {
    let (value, preceding) = match bound {
        RangeBound::Unbounded => return Ok(None),
        RangeBound::Current => return Ok(Some(0)),
        RangeBound::Offset { value, preceding } => (value, preceding),
    };
    let value = constant(&value)?;
    let target = match value.oid() {
        0 | 705 => key,
        20 | 21 | 23 if key == 20 => 20,
        oid @ (20 | 21 | 23) => oid,
        _ => anyhow::bail!("RANGE offset requires an integral type"),
    };
    let distance = evaluated(value, target)?.parse::<i64>()?;
    ensure!(distance >= 0, "invalid preceding or following size in window function");
    Ok(Some(if preceding { -distance } else { distance }))
}
