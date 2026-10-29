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
            field: Some("@aggregate_0".into()),
            limit: 3,
            offset: 0,
            identity: Vec::new(),
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

#[test]
fn native_tokens_match_shared_scalar_order_and_variable_prefixes() -> Result<()> {
    for (oid, values) in [
        (16, vec!["f", "t"]),
        (
            2950,
            vec![
                "00000000-0000-0000-0000-000000000000",
                "00000000-0000-0000-0000-000000000001",
                "ffffffff-ffff-ffff-ffff-ffffffffffff",
            ],
        ),
        (
            1700,
            vec![
                "-Infinity",
                "-1000",
                "-1.01",
                "-1.00",
                "-0.001",
                "-0.0",
                "0.00",
                "0.001",
                "1",
                "1.00",
                "1.01",
                "1000",
                "Infinity",
                "NaN",
            ],
        ),
        (25, vec!["", "\u{1}", "a", "a/", "a0", "aa", "é", "🙂"]),
        (1043, vec!["", "a", "a ", "a  ", "aa"]),
    ] {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                native_case(oid, &values, descending, nulls_first)?;
            }
        }
    }
    Ok(())
}

fn native_case(oid: u32, values: &[&str], descending: bool, nulls_first: bool) -> Result<()> {
    let spec = Spec {
        order: vec![column("v", oid, descending, nulls_first)],
        field: None,
        limit: 3,
        offset: 0,
        identity: Vec::new(),
    };
    let mut rows = values
        .iter()
        .map(|value| {
            Ok((
                [("v".into(), Some((*value).into()))].into(),
                Some(crate::compiler::value::decode(oid, value)?),
            ))
        })
        .collect::<Result<Vec<(Row, _)>>>()?;
    rows.push(([("v".into(), None)].into(), None));
    for (a, av) in &rows {
        for (b, bv) in &rows {
            let expected = match (av, bv) {
                (Some(a), Some(b)) => {
                    if descending {
                        b.cmp(a)
                    } else {
                        a.cmp(b)
                    }
                }
                (None, None) => Ordering::Equal,
                (None, Some(_)) => {
                    if nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (Some(_), None) => {
                    if nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
            };
            assert_eq!(encoding(&spec, a)?.cmp(&encoding(&spec, b)?), expected);
        }
    }
    Ok(())
}
