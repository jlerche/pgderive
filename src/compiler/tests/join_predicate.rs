//! Borrowed join predicates preserve combined-row evaluation, including lazy failures.
use super::expression;
use crate::{
    compiler::{ColumnRef, Predicate},
    transaction::Row,
};
use anyhow::{Context as _, Result};

#[test]
fn borrowed_join_predicates_preserve_nulls_lazy_branches_and_errors() -> Result<()> {
    let right =
        [ColumnRef { right: false, name: "big".into(), oid: 20, nullable: true, modifier: None }];
    for sql in [
        "n < big",
        "coalesce(big,n) = n",
        "CASE WHEN n IS NULL THEN big ELSE n END = big",
        "abs(big) > n",
        "n IS NULL OR 10/big > n",
        "n IS NOT NULL AND 10/big > n",
        "n IN (big,2)",
    ] {
        let combined = Predicate::new(expression(sql).with_context(|| sql.to_owned())?);
        let borrowed = combined.for_join(&right)?;
        for left in [None, Some("0"), Some("2")] {
            for right in [None, Some("0"), Some("3"), Some("-9223372036854775808")] {
                let a = Row::from([("n".into(), left.map(str::to_owned))]);
                let b = Row::from([("big".into(), right.map(str::to_owned))]);
                let mut row = a.clone();
                row.extend(b.clone());
                let result =
                    |value: anyhow::Result<bool>| value.map_err(|error| format!("{error:#}"));
                assert_eq!(
                    result(borrowed.qualifies_pair((&a, &b))),
                    result(combined.qualifies(&row)),
                    "{sql}"
                );
            }
        }
    }
    Ok(())
}
