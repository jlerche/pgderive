//! Partition-wide ranking over weighted row occurrences and SQL peer groups.
use super::{Bag, Ordered, compare, ordered};
use crate::{
    compiler::partition::{Aggregate, Function, Order, Partition},
    engine::{
        dataflow::PartitionWork,
        execution::{Consolidator, EvaluationFailed, Limits, PlanFault},
    },
    transaction::Row,
};
use anyhow::{Context, Result};
use std::cmp::Ordering;

pub(super) fn evaluate(
    spec: &Partition,
    bag: &Bag,
    order: &[Order],
    context: (Limits, &mut PartitionWork),
) -> Result<Bag> {
    let (limits, work) = context;
    let rows = ordered(bag, order, limits, work)?;
    let cumulative = spec.aggregates.iter().any(|value| value.function == Function::CumeDist);
    let mut states = vec![None; spec.aggregates.len()];
    let mut peers = Peers::default();
    let mut updates = Consolidator::new(limits)?;
    for (index, (keys, row)) in rows.iter().enumerate() {
        if index == 0 || compare(keys, &rows[index - 1].0, order) != Ordering::Equal {
            peers.first = index;
            peers.dense += 1;
            if cumulative {
                peers.end = peer_end(&rows, index, order, work)?;
            }
        }
        let mut output = row.clone();
        for (function, state) in spec.aggregates.iter().zip(&mut states) {
            work.charge(1)?;
            output.insert(function.field.clone(), value(function, &rows, index, &peers, state)?);
        }
        updates.add(((), output), 1)?;
    }
    updates.finish_batch()
}

#[derive(Default)]
struct Peers {
    first: usize,
    end: usize,
    dense: usize,
}

fn peer_end(
    rows: &[Ordered],
    first: usize,
    order: &[Order],
    work: &mut PartitionWork,
) -> Result<usize> {
    for index in first + 1..rows.len() {
        work.charge(1)?;
        if compare(&rows[first].0, &rows[index].0, order) != Ordering::Equal {
            return Ok(index);
        }
    }
    Ok(rows.len())
}

fn value(
    aggregate: &Aggregate,
    rows: &[Ordered],
    index: usize,
    peers: &Peers,
    state: &mut Option<Buckets>,
) -> Result<Option<String>> {
    let value = match aggregate.function {
        Function::Rank => (peers.first + 1).to_string(),
        Function::DenseRank => peers.dense.to_string(),
        Function::RowNumber => (index + 1).to_string(),
        Function::PercentRank => fraction(peers.first, (rows.len() - 1).max(1)),
        Function::CumeDist => fraction(peers.end, rows.len()),
        Function::Ntile => return tile(aggregate, &rows[index].1, index, rows.len(), state),
        Function::Lag | Function::Lead => {
            return crate::worker::program::navigation::evaluate(
                aggregate,
                aggregate.navigation.as_ref().context("missing navigation specification")?,
                rows,
                index,
                aggregate.function == Function::Lead,
            );
        }
        _ => anyhow::bail!("aggregate requires framed evaluator"),
    };
    Ok(Some(value))
}

#[expect(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    reason = "PostgreSQL converts both counts to float8 before division"
)]
fn fraction(numerator: usize, denominator: usize) -> String {
    (numerator as f64 / denominator as f64).to_string()
}

#[derive(Clone, Copy)]
struct Buckets {
    first: usize,
    width: usize,
    wide_rows: usize,
    remainder: usize,
}

fn tile(
    aggregate: &Aggregate,
    row: &Row,
    index: usize,
    total: usize,
    state: &mut Option<Buckets>,
) -> Result<Option<String>> {
    // PostgreSQL retries on NULL until it reads the first non-NULL count.
    // That count fixes the partition's bucket sizes, even after leading NULLs.
    let buckets = if let Some(buckets) = *state {
        buckets
    } else {
        let count = aggregate.position.as_ref().context(PlanFault("missing NTILE argument"))?;
        let Some(count) = count.evaluate(row)? else {
            return Ok(None);
        };
        let count = count.parse::<i32>().context(PlanFault("invalid int4 bucket count"))?;
        if count <= 0 {
            return Err(anyhow::anyhow!("argument of ntile must be greater than zero")
                .context(EvaluationFailed));
        }
        let count = usize::try_from(count)?.min(total);
        let width = total / count;
        let remainder = total % count;
        let buckets =
            Buckets { first: index, width, wide_rows: remainder * width + remainder, remainder };
        *state = Some(buckets);
        buckets
    };
    let offset = index - buckets.first;
    let tile = if offset < buckets.wide_rows {
        offset / (buckets.width + 1)
    } else {
        buckets.remainder + (offset - buckets.wide_rows) / buckets.width
    };
    Ok(Some((tile + 1).to_string()))
}
