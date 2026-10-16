use crate::{
    compiler::{self, Compiled, relational::Node},
    source::{Column, Contract, Identity, Relation},
};
use anyhow::{Context, Result};

fn contract(oid: u32) -> Contract {
    Contract {
        environment: crate::source::Environment::fixture(),
        identity: Identity { system: "1".into(), timeline: 1, database: "db".into() },
        publication: "pub".into(),
        slot: "slot".into(),
        relations: vec![Relation {
            oid: 1,
            schema: "source".into(),
            table: "bid".into(),
            columns: vec![
                Column {
                    position: 1,
                    name: "id".into(),
                    oid,
                    modifier: -1,
                    nullable: true,
                    primary: false,
                    collation: 0,
                },
                Column {
                    position: 2,
                    name: "pk".into(),
                    oid: 23,
                    modifier: -1,
                    nullable: false,
                    primary: true,
                    collation: 0,
                },
            ],
        }],
    }
}
fn raw(sql: &str, oid: u32) -> Result<Compiled> {
    let (parsed, _) = compiler::parser::parse(sql)?;
    compiler::bind::bind(parsed, &contract(oid))
}

#[test]
fn widening_retains_one_typed_operand_and_shares_visible_definition() -> Result<()> {
    use crate::catalog::terminal::program::{Expression, Function};
    for (oid, target, function) in [
        (21, "integer", Function::Int4),
        (21, "bigint", Function::Int8),
        (23, "bigint", Function::Int8),
    ] {
        let sql = format!(
            "SELECT id::{target} AS a,id::{target} AS b,id AS raw,CASE WHEN pk>0 THEN 1 ELSE 0 END AS flag FROM source.bid WHERE pk > 0"
        );
        let mut compiled = raw(&sql, oid)?;
        assert!(compiled.output().terminal.is_none());
        assert!(
            compiled
                .relational
                .nodes
                .iter()
                .any(|node| matches!(node, Node::Map { computed, .. } if !computed.is_empty()))
        );
        super::apply(&mut compiled)?;
        assert_eq!(compiled.output().columns.len(), 2);
        assert_eq!(compiled.output().columns[0].column.oid, oid);
        let (program, _) =
            compiled.output().terminal.as_ref().context("terminal absent")?.placement_program()?;
        assert_eq!(program.outputs[0].expression, program.outputs[1].expression);
        assert_eq!(
            program
                .expressions
                .iter()
                .filter(|node| matches!(node, Expression::Call { .. }))
                .count(),
            1
        );
        assert!(program.expressions.iter().any(
            |node| matches!(node, Expression::Call { function: actual, .. } if *actual == function)
        ));
        let before = serde_json::to_value(&compiled)?;
        super::apply(&mut compiled)?;
        assert_eq!(before, serde_json::to_value(&compiled)?);
    }
    Ok(())
}

#[test]
fn narrowing_arithmetic_lazy_trees_and_maintenance_values_remain_local() -> Result<()> {
    for (sql, oid) in [
        ("SELECT id::smallint AS value FROM source.bid", 23),
        ("SELECT (id+1)::bigint AS value FROM source.bid", 23),
        ("SELECT coalesce(id,0)::bigint AS value FROM source.bid", 23),
        ("SELECT id::integer AS value FROM source.bid", 20),
        (
            "SELECT q.value,COUNT(*) AS n FROM (SELECT id::bigint AS value FROM source.bid) q GROUP BY q.value",
            23,
        ),
    ] {
        let mut compiled = raw(sql, oid)?;
        let before = serde_json::to_value(&compiled)?;
        super::apply(&mut compiled)?;
        assert_eq!(before, serde_json::to_value(&compiled)?, "{sql}");
    }
    Ok(())
}

#[test]
fn partial_map_keeps_fallible_siblings_local() -> Result<()> {
    let mut compiled = raw(
        "SELECT id::bigint AS wide,CASE WHEN pk>0 THEN id+1 ELSE 0 END AS fallible FROM source.bid",
        23,
    )?;
    super::apply(&mut compiled)?;
    let computed = compiled
        .relational
        .nodes
        .iter()
        .find_map(|node| if let Node::Map { computed, .. } = node { Some(computed) } else { None })
        .context("local sibling absent")?;
    assert_eq!(computed.len(), 1);
    assert_eq!(compiled.output().columns.len(), 2);
    assert!(compiled.output().terminal.is_some());
    Ok(())
}
