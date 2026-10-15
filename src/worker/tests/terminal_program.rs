use super::contract;
use crate::{compiler::compile, source::Column};
use anyhow::{Context, Result};
pub(super) fn native(oid: u32) -> crate::source::Contract {
    let mut native = contract();
    native.relations[1].columns.extend([
        Column {
            position: 4,
            name: "bidder".into(),
            oid: 20,
            modifier: -1,
            nullable: true,
            primary: false,
            collation: 0,
        },
        Column {
            position: 5,
            name: "date_time".into(),
            oid,
            modifier: -1,
            nullable: true,
            primary: false,
            collation: 0,
        },
        Column {
            position: 6,
            name: "extra".into(),
            oid: 25,
            modifier: -1,
            nullable: true,
            primary: false,
            collation: 100,
        },
    ]);
    native
}
#[test]
fn q14_has_distinct_native_inputs_and_visible_typed_outputs() -> Result<()> {
    let sql = include_str!("../../../queries/nexmark/q14.sql").replace("nexmark.", "source.");
    for oid in [1114, 1184] {
        let sql = if oid == 1114 {
            sql.replace("date_time AT TIME ZONE 'UTC'", "date_time")
        } else {
            sql.clone()
        };
        let compiled = compile(&sql, &native(oid))?;
        let layout = compiled.output();
        assert_eq!(layout.columns.len(), 5);
        assert!(layout.terminal.is_some());
        assert!(compiled.validate_bound().is_err());
        let equivalent = compile(
            &sql.replace("SELECT auction,bidder", "SELECT b.auction,b.bidder")
                .replace("FROM source.bid", "FROM source.bid b"),
            &native(oid),
        )?;
        assert_eq!(serde_json::to_value(&compiled)?, serde_json::to_value(&equivalent)?);
        for changed in [sql.replace("50000000", "40000000"), sql.replace("'dayTime'", "'day'")] {
            assert_ne!(
                serde_json::to_value(&compiled)?,
                serde_json::to_value(compile(&changed, &native(oid))?)?
            );
        }
    }
    Ok(())
}
#[test]
fn terminal_programs_reject_non_native_or_untyped_semantics() {
    for expression in [
        "1e2 * price",
        "replace(extra,1,'')",
        "extract(hour FROM date_time)",
        "timezone('Europe/London',date_time)",
        "length(DISTINCT extra)",
        "length(extra) OVER ()",
        "CASE WHEN price>1 THEN 'x' ELSE 1 END",
        "CASE WHEN price>1 THEN 'x' END",
        "random()",
        "public.length(extra)",
    ] {
        let sql = format!("SELECT {expression} AS output FROM source.bid");
        assert!(compile(&sql, &native(1184)).is_err(), "accepted {sql}");
    }
}

#[test]
fn terminal_filter_retains_hidden_operand_without_exposing_it() -> Result<()> {
    let compiled =
        compile("SELECT auction FROM source.bid WHERE 0.908 * price > 1", &native(1184))?;
    let layout = compiled.output();
    assert_eq!(layout.columns.len(), 1);
    assert!(layout.terminal.is_none());

    Ok(())
}

#[test]
fn integral_casts_and_coalesce_bind_closed_signatures() -> Result<()> {
    let native = native(1114);
    for sql in [
        "SELECT COALESCE(price, auction) AS value FROM source.bid",
        "SELECT COALESCE(NULL, price, 1) AS value FROM source.bid",
        "SELECT COALESCE(NULL, NULL) AS value FROM source.bid",
        "SELECT NULL::integer AS value FROM source.bid",
        "SELECT CAST(price AS smallint) AS value FROM source.bid",
        "SELECT price::integer AS value FROM source.bid",
        "SELECT price::numeric AS value FROM source.bid",
        "SELECT price * price AS value FROM source.bid",
        "SELECT price + auction AS value FROM source.bid",
        "SELECT price::smallint - auction::smallint AS value FROM source.bid",
    ] {
        let compiled = compile(sql, &native)?;
        assert!(compiled.output().terminal.is_some());
    }
    for sql in [
        "SELECT price::text AS value FROM source.bid",
        "SELECT price::numeric(10,2) AS value FROM source.bid",
        "SELECT COALESCE(price, extra) AS value FROM source.bid",
        "SELECT price::public.int4 AS value FROM source.bid",
    ] {
        assert!(compile(sql, &native).is_err(), "accepted {sql}");
    }
    Ok(())
}

#[test]
fn grouped_scalar_keys_execute_before_partition_state() -> Result<()> {
    use crate::compiler::relational::Node;
    let compiled = compile(
        "SELECT COALESCE(price, 1) AS key, COUNT(*) AS n FROM source.bid GROUP BY COALESCE(price, 1)",
        &native(1114),
    )?;
    let relational = compiled.relational();
    let computed = relational
        .nodes
        .iter()
        .find_map(|node| if let Node::Map { computed, .. } = node { Some(computed) } else { None })
        .context("missing scalar map")?;
    assert_eq!(computed.len(), 1);
    let field = crate::compiler::relational::field(0, "price");
    assert_eq!(computed[0].expression.evaluate(&[(field.clone(), None)].into())?, Some("1".into()));
    assert_eq!(
        computed[0].expression.evaluate(&[(field, Some("9".into()))].into())?,
        Some("9".into())
    );
    Ok(())
}

#[test]
fn typed_predicates_execute_nested_case_conditions_and_nulls() -> Result<()> {
    use crate::compiler::relational::Node;
    let native = native(1114);
    for (predicate, zero, null) in [
        ("(CASE WHEN extra='x' THEN price+1 ELSE price END)>0", true, false),
        (
            "(CASE WHEN date_time-date_time>interval '1 second' THEN price+1 ELSE price END)>0",
            false,
            false,
        ),
        ("(CASE WHEN extra='x' THEN price+1 ELSE price END) IS NULL", false, true),
        ("(CASE WHEN mod(price,3)=0 THEN price+1 ELSE price END) IS NULL", false, true),
    ] {
        let compiled =
            compile(&format!("SELECT price FROM source.bid WHERE {predicate}"), &native)?;
        let predicate = compiled
            .relational()
            .nodes
            .iter()
            .find_map(|node| match node {
                Node::Filter { predicate, .. } => Some(predicate),
                _ => None,
            })
            .context("missing compiled filter")?;
        for (price, expected) in [(Some("0"), zero), (None, null)] {
            let row = [
                ("0:price".into(), price.map(str::to_owned)),
                ("0:extra".into(), Some("x".into())),
                ("0:date_time".into(), Some("2000-01-01 00:00:00".into())),
            ]
            .into();
            assert_eq!(predicate.qualifies(&row)?, expected);
        }
    }
    Ok(())
}
