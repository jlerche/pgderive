//! Merged USING metadata is distinct from equality and qualified originals.
use crate::{
    compiler::{compile, properties},
    source::{
        Contract,
        tests::{column, contract},
    },
};
use anyhow::Result;
fn native(left: (u32, i32, u32), right: (u32, i32, u32)) -> Contract {
    let mut native = contract();
    let mut other = native.relations[0].clone();
    other.oid = 43;
    other.table = "other".into();
    for (table, (oid, modifier, collation)) in
        [(&mut native.relations[0], left), (&mut other, right)]
    {
        let mut key = column(2, "k", oid);
        key.modifier = modifier;
        key.collation = collation;
        table.columns.push(key);
    }
    native.relations.push(other);
    native
}
#[test]
fn merged_modifier_oid_collation_and_qualified_originals_follow_postgres() -> Result<()> {
    let numeric = crate::numeric::Typmod::new(8, 2)?.encoded();
    for (left, right, oid, modifier) in [
        ((21, -1, 0), (20, -1, 0), 20, -1),
        ((1082, -1, 0), (1082, -1, 0), 1082, -1),
        ((1700, numeric, 0), (1700, numeric, 0), 1700, numeric),
        ((1700, numeric, 0), (1700, crate::numeric::Typmod::new(9, 4)?.encoded(), 0), 1700, -1),
        ((1043, 12, 950), (1043, 12, 950), 1043, 12),
        ((1043, 12, 950), (1043, 20, 950), 1043, -1),
        ((1043, 12, 950), (25, -1, 950), 1043, -1),
    ] {
        let native = native(left, right);
        for kind in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"] {
            let compiled = compile(
                &format!(
                    "SELECT k,l.k AS l,r.k AS r FROM public.items l {kind} public.other r USING(k)"
                ),
                &native,
            )?;
            let facts = properties::occurrence(compiled.relational(), &native)?;
            let output = &compiled.output().columns;
            assert_eq!(output[0].column.oid, oid);
            assert_eq!(facts.modifier(&output[0].column), Some(modifier));
            assert_eq!(facts.modifier(&output[1].column), Some(left.1));
            assert_eq!(facts.modifier(&output[2].column), Some(right.1));
        }
    }
    for (kind, expected) in
        [("JOIN", 100), ("LEFT JOIN", 100), ("RIGHT JOIN", 950), ("FULL JOIN", 950)]
    {
        let native = native((25, -1, 100), (25, -1, 950));
        let compiled = compile(
            &format!("SELECT k FROM public.items l {kind} public.other r USING(k)"),
            &native,
        )?;
        let facts = properties::occurrence(compiled.relational(), &native)?;
        assert_eq!(facts.collation(&compiled.output().columns[0].column), Some(expected));
    }
    Ok(())
}
#[test]
fn full_nullable_group_pairs_do_not_claim_occurrence_uniqueness() -> Result<()> {
    let native = native((23, -1, 0), (23, -1, 0));
    let sql = "SELECT l.k AS l,r.k AS r FROM(SELECT k FROM public.items GROUP BY k)l FULL JOIN(SELECT k FROM public.other GROUP BY k)r ON l.k=r.k";
    let compiled = compile(sql, &native)?;
    let facts = properties::occurrence(compiled.relational(), &native)?;
    assert!(!facts.unique_rows());
    assert!(
        compile(&format!("SELECT ROW_NUMBER() OVER(ORDER BY q.l,q.r) FROM({sql})q"), &native)
            .is_err()
    );
    Ok(())
}

#[test]
fn mixed_date_timestamp_using_rejects_even_when_the_merged_value_is_unused() {
    for (left, right) in [(1082, 1114), (1184, 1082)] {
        let native = native((left, -1, 0), (right, -1, 0));
        for kind in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"] {
            assert!(
                compile(
                    &format!("SELECT l.id FROM public.items l {kind} public.other r USING(k)"),
                    &native,
                )
                .is_err(),
                "{left} {kind} {right}"
            );
        }
    }
}

#[test]
fn full_and_right_merged_keys_are_nullable_when_the_preserved_right_key_is_nullable() -> Result<()>
{
    let mut native = native((23, -1, 0), (23, -1, 0));
    native.relations[0].columns[1].nullable = false;
    for kind in ["FULL JOIN", "RIGHT JOIN"] {
        let compiled = compile(
            &format!(
                "SELECT k,l.k AS l,r.k AS r FROM public.items l {kind} public.other r USING(k)"
            ),
            &native,
        )?;
        assert!(compiled.output().columns[0].column.nullable, "{kind}");
        assert!(compiled.output().columns[1].column.nullable, "{kind}");
        assert!(compiled.output().columns[2].column.nullable, "{kind}");
    }
    Ok(())
}
