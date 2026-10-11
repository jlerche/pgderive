//! Sparse exact value counts and zero/positive membership for integral DISTINCT.
use super::relational::Key;
use crate::{
    compiler::partition::{Function, Mode, Partition},
    engine::dataflow::{Expand, Project, Statistics},
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
const MEMBER: &str = "@distinct_value_membership";
const COUNT: &str = "@distinct_occurrence_count";

fn payload(spec: &Partition, text: &str) -> Result<(usize, i64)> {
    let pair: (usize, i64) = serde_json::from_str(text)?;
    ensure!(serde_json::to_string(&pair)? == text, "noncanonical distinct member");
    let aggregate = spec.aggregates.get(pair.0).context("distinct index out of range")?;
    ensure!(
        matches!(aggregate.function, Function::CountDistinct),
        "distinct member targets ordinary aggregate"
    );
    Ok(pair)
}
fn width(spec: &Partition) -> Result<usize> {
    let Mode::Grouped { keys } = &spec.mode else {
        anyhow::bail!("distinct counts require grouped mode");
    };
    Ok(keys.len())
}
pub(super) fn input(spec: &Partition) -> Expand<Key, Row, Key, Row> {
    let spec = spec.clone();
    Expand::new(move |key: &Key, row: &Row, emit| {
        ensure!(key.len() == width(&spec)?, "distinct group key width mismatch");
        ensure!(
            !row.contains_key(MEMBER) && !row.contains_key(COUNT),
            "distinct internal field collision"
        );
        for (index, aggregate) in spec.aggregates.iter().enumerate() {
            if !matches!(aggregate.function, Function::CountDistinct) {
                continue;
            }
            if aggregate
                .filter
                .as_ref()
                .map(|filter| filter.qualifies(row))
                .transpose()?
                .is_some_and(|value| !value)
            {
                continue;
            }
            let argument = aggregate.argument.as_ref().context("missing distinct argument")?;
            let Some(value) = row.get(&argument.name).context("missing distinct input")? else {
                continue;
            };
            let value: i64 = value.parse()?;
            let mut keyed = key.clone();
            keyed.push(serde_json::to_string(&(index, value))?);
            emit((keyed, row.clone()))?;
        }
        Ok(())
    })
}
pub(super) fn counts(spec: &Partition) -> Statistics<Key, Row> {
    let decoded = spec.clone();
    let encoded = spec.clone();
    Statistics::new(
        1,
        |_: &Row| Ok(vec![BigInt::from(1)]),
        move |row: &Row| {
            ensure!(row.len() == 2, "invalid distinct state fields");
            payload(
                &decoded,
                row.get(MEMBER)
                    .and_then(Option::as_deref)
                    .context("missing distinct state member")?,
            )?;
            let count: BigInt = row
                .get(COUNT)
                .and_then(Option::as_deref)
                .context("missing distinct state count")?
                .parse()?;
            ensure!(count > BigInt::from(0), "nonpositive persisted distinct count");
            Ok(vec![count])
        },
        move |key: &Key, values: &[BigInt]| {
            ensure!(key.len() == width(&encoded)? + 1, "distinct state key width mismatch");
            let member = key.last().context("missing distinct navigation suffix")?;
            payload(&encoded, member)?;
            let count = values.first().context("missing distinct count")?;
            ensure!(count >= &BigInt::from(0), "negative distinct occurrence count");
            if count == &BigInt::from(0) {
                return Ok(None);
            }
            Ok(Some(Row::from([
                (MEMBER.into(), Some(member.clone())),
                (COUNT.into(), Some(count.to_string())),
            ])))
        },
    )
}
pub(super) fn membership(spec: &Partition) -> Project<Key, Row, Key, Row> {
    let spec = spec.clone();
    Project::new(move |key: &Key, row: &Row| {
        ensure!(key.len() == width(&spec)? + 1 && row.len() == 2, "invalid distinct state layout");
        let member =
            row.get(MEMBER).and_then(Option::as_deref).context("missing distinct member")?;
        payload(&spec, member)?;
        ensure!(
            key.last().is_some_and(|suffix| suffix == member),
            "distinct state key/member mismatch"
        );
        let count: BigInt = row
            .get(COUNT)
            .and_then(Option::as_deref)
            .context("missing distinct occurrence count")?
            .parse()?;
        ensure!(count > BigInt::from(0), "nonpositive distinct membership count");
        let mut group = key.clone();
        group.pop().context("missing distinct suffix")?;
        Ok(Some((group, Row::from([(MEMBER.into(), Some(member.into()))]))))
    })
}
pub(super) fn measure(spec: &Partition, row: &Row) -> Result<Option<Vec<BigInt>>> {
    let Some(member) = row.get(MEMBER) else {
        ensure!(!row.contains_key(COUNT), "distinct state row used as ordinary input");
        return Ok(None);
    };
    ensure!(row.len() == 1, "distinct membership/ordinary field collision");
    let text = member.as_deref().context("NULL distinct membership")?;
    let (index, _) = payload(spec, text)?;
    let mut values = vec![BigInt::from(0); 1 + spec.aggregates.len() * 2];
    values[1 + index * 2] = BigInt::from(1);
    Ok(Some(values))
}

#[cfg(test)]
mod tests {
    use super::{COUNT, MEMBER, measure, membership, payload};
    use crate::{
        compiler::{compile, partition::Partition, relational::Node},
        engine::{Batch, dataflow::TimedBatch},
        transaction::Row,
    };
    use anyhow::{Context, Result};
    fn spec() -> Result<Partition> {
        let compiled = compile(
            "SELECT b.auction AS g,COUNT(DISTINCT b.price) AS n FROM source.bid b GROUP BY b.auction",
            &crate::worker::tests::contract(),
        )?;
        compiled
            .relational()
            .context("missing distinct IR")?
            .nodes
            .iter()
            .find_map(|node| match node {
                Node::DistinctCounts { spec, .. } => Some(spec.clone()),
                _ => None,
            })
            .context("missing sparse counts")
    }
    #[test]
    fn tagged_members_reject_collision_noncanonical_and_out_of_range() -> Result<()> {
        let spec = spec()?;
        for text in ["[0, 1]", "[999,1]", "[0,null]", "[0,9223372036854775808]"] {
            assert!(payload(&spec, text).is_err());
        }
        let member = Row::from([(MEMBER.into(), Some("[0,1]".into()))]);
        assert_eq!(
            measure(&spec, &member)?.context("missing measure")?,
            vec![0.into(), 1.into(), 0.into()]
        );
        let mut collision = member;
        collision.insert("0:id".into(), Some("1".into()));
        assert!(measure(&spec, &collision).is_err());
        let state_as_input = Row::from([(COUNT.into(), Some("1".into()))]);
        assert!(measure(&spec, &state_as_input).is_err());
        assert!(measure(&spec, &Row::from([(MEMBER.into(), None)])).is_err());
        Ok(())
    }
    #[test]
    fn retained_membership_checks_exact_suffix_and_positive_state() -> Result<()> {
        let spec = spec()?;
        let operator = membership(&spec);
        for (suffix, count) in [("[0,2]", "1"), ("[0,1]", "0"), ("[0,1]", "-1")] {
            let state = Row::from([
                (MEMBER.into(), Some("[0,1]".into())),
                (COUNT.into(), Some(count.into())),
            ]);
            let input = TimedBatch {
                time: 1,
                batch: Batch::from_updates([((vec!["null".into(), suffix.into()], state), 1)])?,
            };
            assert!(operator.evaluate(&input).is_err());
        }
        Ok(())
    }
    #[tokio::test]
    async fn sparse_counts_reject_malformed_retained_rows() -> Result<()> {
        use crate::engine::{dataflow::Arrangement, execution::Limits};
        use object_store::memory::InMemory;
        use std::sync::Arc;
        let spec = spec()?;
        let operator = super::counts(&spec);
        for count in ["0", "-1", "invalid"] {
            let writer = Arrangement::new(Arc::new(InMemory::new()), "bad-count-v1".into(), 1)?;
            let state = Row::from([
                (MEMBER.into(), Some("[0,1]".into())),
                (COUNT.into(), Some(count.into())),
            ]);
            let key = vec!["null".into(), "[0,1]".into()];
            let initial =
                TimedBatch { time: 1, batch: Batch::from_updates([((key.clone(), state), 1)])? };
            let prior = writer.stage(&writer.empty(), &initial).await?;
            let input =
                TimedBatch { time: 2, batch: Batch::from_updates([((key, Row::new()), 1)])? };
            assert!(operator.evaluate(&input, &prior, Limits::default()).await.is_err());
            assert_eq!(prior.time(), 1);
        }
        Ok(())
    }
}
