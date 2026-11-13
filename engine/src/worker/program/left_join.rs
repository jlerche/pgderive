//! Left equijoin differentiated over complete tagged input partitions.
use super::relational::Key;
#[cfg(test)]
use crate::engine::{
    Batch,
    dataflow::Partition,
    execution::{Consolidator, Limits},
};
use crate::{
    compiler::{
        ColumnRef,
        left_join::{Input, Spec},
    },
    engine::dataflow::Project,
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
#[cfg(test)]
use num_bigint::BigInt;
const SIDE: &str = "\0left_join_side";
pub(super) fn input(spec: &Input) -> Project<Key, Row, Key, Row> {
    let spec = spec.clone();
    Project::new(move |_: &Key, row: &Row| {
        ensure!(!row.contains_key(SIDE), "left join internal field collision");
        let components = spec
            .keys
            .iter()
            .map(|column| crate::compiler::native_key(column, row))
            .collect::<Result<Vec<_>>>()?;
        let key = if components.iter().any(Option::is_none) {
            vec!["null".into(), if spec.left { "left".into() } else { "right".into() }]
        } else {
            let mut key = vec!["match".into()];
            key.extend(
                components.into_iter().collect::<Option<Vec<_>>>().context("missing join key")?,
            );
            key
        };
        let mut tagged = row.clone();
        tagged.insert(SIDE.into(), Some(if spec.left { "left".into() } else { "right".into() }));
        Ok(Some((key.into(), tagged)))
    })
}
#[cfg(test)]
pub(super) fn operator(spec: &Spec, limits: Limits) -> Partition<Key, Row> {
    let spec = spec.clone();
    Partition::new_zero_preserving(move |_: &Key, rows: &Batch<(), Row>, work| {
        let mut left = Vec::new();
        let mut right = Vec::new();
        for (((), row), weight) in rows.iter() {
            work.charge(1)?;
            ensure!(*weight > 0, "negative left join input membership");
            match row.get(SIDE).and_then(Option::as_deref) {
                Some("left") => left.push((row, *weight)),
                Some("right") => right.push((row, *weight)),
                _ => anyhow::bail!("invalid left join input tag"),
            }
        }
        let mut output = Consolidator::new(limits)?;
        for (row, weight) in &left {
            let mut matched = false;
            for (other, multiplicity) in &right {
                work.charge(1)?;
                if let Some(result) = matching(&spec, row, other)? {
                    matched = true;
                    output
                        .add(((), result), BigInt::from(*weight) * BigInt::from(*multiplicity))?;
                }
            }
            if !matched {
                work.charge(1)?;
                let result = null_extend(&spec.right, row)?;
                output.add(((), result), *weight)?;
            }
        }
        if spec.full {
            for (row, weight) in right {
                let mut matched = false;
                for (other, _) in &left {
                    work.charge(1)?;
                    matched |= matching(&spec, other, row)?.is_some();
                }
                if !matched {
                    output.add(((), null_extend(&spec.left, row)?), weight)?;
                }
            }
        }
        output.finish_batch()
    })
}
pub(super) fn stripped(row: &Row) -> Row {
    row.iter()
        .filter(|(field, _)| field.as_str() != SIDE)
        .map(|(field, value)| (field.clone(), value.clone()))
        .collect()
}

fn validate_pair(left: &Row, right: &Row) -> Result<()> {
    ensure!(
        !left.keys().any(|field| field != SIDE && right.contains_key(field)),
        "left join field collision"
    );
    Ok(())
}
fn combined(left: &Row, right: &Row) -> Row {
    left.iter()
        .chain(right)
        .filter(|(field, _)| field.as_str() != SIDE)
        .map(|(field, value)| (field.clone(), value.clone()))
        .collect()
}
pub(super) fn pair(left: &Row, right: &Row) -> Result<Row> {
    let mut result = stripped(left);
    for (field, value) in right {
        if field != SIDE {
            ensure!(
                result.insert(field.clone(), value.clone()).is_none(),
                "left join field collision"
            );
        }
    }
    Ok(result)
}
#[cfg(test)]
pub(super) fn matching(spec: &Spec, left: &Row, right: &Row) -> Result<Option<Row>> {
    let result = pair(left, right)?;
    Ok(spec
        .predicate
        .as_ref()
        .map_or(Ok(true), |predicate| predicate.qualifies(&result))?
        .then_some(result))
}
pub(super) struct Matcher(Option<crate::compiler::Predicate>);
impl Matcher {
    pub(super) fn new(spec: &Spec) -> Result<Self> {
        Ok(Self(spec.predicate.as_ref().map(|value| value.for_join(&spec.right)).transpose()?))
    }
    pub(super) fn matches(&self, left: &Row, right: &Row) -> Result<bool> {
        validate_pair(left, right)?;
        self.0.as_ref().map_or(Ok(true), |predicate| predicate.qualifies_pair((left, right)))
    }
    pub(super) fn matching(&self, left: &Row, right: &Row) -> Result<Option<Row>> {
        Ok(self.matches(left, right)?.then(|| combined(left, right)))
    }
}

pub(super) fn null_extend(columns: &[ColumnRef], row: &Row) -> Result<Row> {
    let mut result = stripped(row);
    for column in columns {
        ensure!(result.insert(column.name.clone(), None).is_none(), "outer join field collision");
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::{Matcher, SIDE, pair};
    use crate::transaction::Row;
    use anyhow::Result;

    #[test]
    fn pairs_strip_side_tags_preserve_fields_and_reject_collisions() -> Result<()> {
        let left = Row::from([(SIDE.into(), Some("left".into())), ("a".into(), None)]);
        let right = Row::from([
            (SIDE.into(), Some("right".into())),
            ("b".into(), Some("quoted\"λ".into())),
        ]);
        let joined = pair(&left, &right)?;
        assert_eq!(joined, Row::from([("a".into(), None), ("b".into(), Some("quoted\"λ".into()))]));
        let collision = Row::from([("a".into(), Some("value".into()))]);
        assert_eq!(
            pair(&left, &collision).err().map(|error| error.to_string()),
            Some("left join field collision".into())
        );
        assert_eq!(left.len(), 2);
        assert_eq!(right.len(), 2);
        Ok(())
    }
    #[test]
    fn borrowed_matching_validates_collisions_before_reading_predicate_columns() -> Result<()> {
        use crate::compiler::{compile, relational::Node};
        let mut native = crate::worker::tests::contract();
        native.relations[0].columns[2].oid = 23;
        native.relations[0].columns[2].collation = 0;
        let compiled = compile(
            "SELECT a.id,b.id FROM source.auction a FULL JOIN source.bid b ON a.other=b.auction AND a.id<b.price",
            &native,
        )?;
        let spec = compiled
            .relational()
            .nodes
            .iter()
            .find_map(|node| match node {
                Node::LeftJoin { spec, .. } => Some(spec),
                _ => None,
            })
            .ok_or_else(|| anyhow::anyhow!("join absent"))?;
        let matcher = Matcher::new(spec)?;
        let left = Row::from([("collision".into(), None)]);
        let right = left.clone();
        assert_eq!(
            matcher.matches(&left, &right).err().map(|e| e.to_string()),
            Some("left join field collision".into())
        );
        assert_eq!(
            matcher.matching(&left, &right).err().map(|e| e.to_string()),
            Some("left join field collision".into())
        );
        assert!(matcher.matches(&Row::new(), &Row::new()).is_err());
        Ok(())
    }
}
