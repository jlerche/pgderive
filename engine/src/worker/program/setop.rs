//! Exact per-side set counts differentiated at complete committed boundaries.
use super::{group_cursor::project, relational::Key};
use crate::{
    compiler::setop::{Kind, Spec},
    engine::{
        Batch,
        dataflow::{Group, GroupCursor, PartitionWork, Project},
    },
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
const SIDE: &str = "\0set_side";
pub(super) fn tag(left: bool) -> Project<Key, Row, Key, Row> {
    Project::new(move |key: &Key, row: &Row| {
        let mut row = row.clone();
        ensure!(
            row.insert(SIDE.into(), Some(if left { "left" } else { "right" }.into())).is_none(),
            "set tag field collision"
        );
        Ok(Some((key.clone(), row)))
    })
}
#[derive(Default)]
struct Counts {
    left: BigInt,
    right: BigInt,
    representative: Option<Row>,
}
impl Counts {
    fn observe(&mut self, row: &Row, weight: i64, spec: &Spec) -> Result<()> {
        ensure!(weight > 0, "nonpositive set input membership");
        match row.get(SIDE).and_then(Option::as_deref) {
            Some("left") => {
                self.left += weight;
                if self.representative.is_none() {
                    self.representative = Some(project(&spec.columns, row)?);
                }
            }
            Some("right") => self.right += weight,
            _ => anyhow::bail!("invalid set input tag"),
        }
        Ok(())
    }
    fn finish(self, spec: &Spec) -> Result<Batch<(), Row>> {
        let multiplicity = if spec.all {
            match spec.kind {
                Kind::Intersect => self.left.min(self.right),
                Kind::Except => (self.left - self.right).max(BigInt::from(0)),
                Kind::Union => anyhow::bail!("UNION uses weighted addition"),
            }
        } else {
            BigInt::from(u8::from(
                self.left > BigInt::from(0)
                    && match spec.kind {
                        Kind::Intersect => self.right > BigInt::from(0),
                        Kind::Except => self.right == BigInt::from(0),
                        Kind::Union => anyhow::bail!("UNION uses shared DISTINCT"),
                    },
            ))
        };
        if multiplicity == BigInt::from(0) {
            return Batch::from_updates([]);
        }
        let weight = i64::try_from(multiplicity).context("set output multiplicity exceeds i64")?;
        Batch::from_updates([(
            ((), self.representative.context("set output has no left representative")?),
            weight,
        )])
    }
}
pub(super) fn operator(spec: &Spec) -> Group<Key, Row> {
    let spec = spec.clone();
    Group::new(move |input, work| {
        let spec = spec.clone();
        Box::pin(async move {
            let old = evaluate(&spec, input.before().await?, work).await?;
            let new = evaluate(&spec, input.after().await?, work).await?;
            Batch::from_updates(
                old.iter()
                    .map(|(row, weight)| (row.clone(), -*weight))
                    .chain(new.iter().map(|(row, weight)| (row.clone(), *weight))),
            )
        })
    })
}
async fn evaluate(
    spec: &Spec,
    mut cursor: GroupCursor<Key, Row>,
    work: &mut PartitionWork,
) -> Result<Batch<(), Row>> {
    let mut counts = Counts::default();
    while let Some((row, weight)) = cursor.next(work).await? {
        counts.observe(&row, weight, spec)?;
    }
    counts.finish(spec)
}

#[cfg(test)]
mod tests {
    use super::{Counts, Kind, Row, Spec};
    use crate::compiler::ColumnRef;
    use anyhow::Result;
    fn spec(kind: Kind, all: bool) -> Spec {
        Spec {
            kind,
            all,
            columns: vec![ColumnRef {
                name: "x".into(),
                oid: 23,
                nullable: true,
                right: false,
                collation: 0,
                modifier: None,
            }],
        }
    }
    fn row(side: &str) -> Row {
        [("x".into(), None), (super::SIDE.into(), Some(side.into()))].into()
    }
    #[test]
    fn counts_remain_exact_above_i64_and_only_final_outputs_narrow() -> Result<()> {
        let difference = spec(Kind::Except, true);
        let mut counts = Counts::default();
        counts.observe(&row("left"), i64::MAX, &difference)?;
        counts.observe(&row("left"), i64::MAX, &difference)?;
        counts.observe(&row("right"), i64::MAX, &difference)?;
        counts.observe(&row("right"), i64::MAX - 3, &difference)?;
        assert_eq!(counts.finish(&difference)?.iter().next().map(|(_, weight)| *weight), Some(3));
        let intersection = spec(Kind::Intersect, false);
        let mut counts = Counts::default();
        counts.observe(&row("left"), i64::MAX, &intersection)?;
        counts.observe(&row("left"), i64::MAX, &intersection)?;
        counts.observe(&row("right"), 1, &intersection)?;
        assert_eq!(counts.finish(&intersection)?.iter().next().map(|(_, weight)| *weight), Some(1));
        let mut overflow = Counts::default();
        overflow.observe(&row("left"), i64::MAX, &difference)?;
        overflow.observe(&row("left"), 1, &difference)?;
        assert!(overflow.finish(&difference).is_err());
        assert!(Counts::default().observe(&row("left"), -1, &difference).is_err());
        assert!(Counts::default().observe(&row("invalid"), 1, &difference).is_err());
        Ok(())
    }
}
