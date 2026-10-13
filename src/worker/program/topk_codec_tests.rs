use super::{Spec, encoding};
use crate::{
    compiler::{ColumnRef, partition::Order},
    transaction::Row,
};
use anyhow::{Context, Result};
use std::cmp::Ordering;
fn column(name: &str, oid: u32, descending: bool, nulls_first: bool) -> Order {
    Order {
        column: ColumnRef { name: name.into(), right: false, oid, nullable: true },
        descending,
        nulls_first,
    }
}
fn compare(left: &Row, right: &Row, spec: &Spec) -> Result<Ordering> {
    for order in &spec.order {
        let parse = |row: &Row| -> Result<Option<i64>> {
            row.get(&order.column.name)
                .context("missing oracle field")?
                .as_ref()
                .map(|text| text.parse::<i64>())
                .transpose()
                .map_err(Into::into)
        };
        let a: Option<i64> = parse(left)?;
        let b: Option<i64> = parse(right)?;
        let value = match (a, b) {
            (Some(a), Some(b)) => {
                if order.descending {
                    b.cmp(&a)
                } else {
                    a.cmp(&b)
                }
            }
            (None, None) => Ordering::Equal,
            (None, Some(_)) => {
                if order.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (Some(_), None) => {
                if order.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
        };
        if value != Ordering::Equal {
            return Ok(value);
        }
    }
    Ok(Ordering::Equal)
}
#[test]
fn encoded_order_matches_signed_boundaries_and_mixed_null_prefixes() -> Result<()> {
    let mut rows = Vec::new();
    for a in [Some(i64::MIN), Some(-1), Some(0), Some(1), Some(i64::MAX), None] {
        for b in [Some(-2), Some(0), None] {
            rows.push(
                [
                    (String::from("0:a"), a.map(|value| value.to_string())),
                    ("0:b".into(), b.map(|value| value.to_string())),
                    ("0:id".into(), Some(rows.len().to_string())),
                ]
                .into_iter()
                .collect::<Row>(),
            );
        }
    }
    for flags in 0..16 {
        let spec = Spec {
            order: vec![
                column("0:a", 20, flags & 1 != 0, flags & 2 != 0),
                column("0:b", 23, flags & 4 != 0, flags & 8 != 0),
                column("0:id", 23, false, false),
            ],
            field: "@aggregate_0".into(),
            limit: 3,
        };
        let mut encoded = rows
            .iter()
            .map(|row| Ok((encoding(&spec, row)?, row.clone())))
            .collect::<Result<Vec<_>>>()?;
        encoded.sort_by(|a, b| a.0.cmp(&b.0));
        // Pairwise comparisons independently establish both ordering and equality.
        for pair in encoded.windows(2) {
            assert_eq!(compare(&pair[0].1, &pair[1].1, &spec)?, Ordering::Less);
        }
    }
    Ok(())
}
