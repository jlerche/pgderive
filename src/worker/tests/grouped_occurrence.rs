//! Logical group identity replaces source identities before positional ranking.
use super::contract;
use crate::{
    compiler::compile,
    source::{Contract, Relation},
};
use anyhow::Result;
fn native() -> Contract {
    let mut native = contract();
    let template = native.relations[1].columns[2].clone();
    for (relation, position, name, oid) in [
        (0, 4, "date_time", 1184),
        (0, 5, "expires", 1184),
        (0, 6, "seller", 20),
        (1, 4, "date_time", 1184),
    ] {
        let mut column = template.clone();
        column.position = position;
        column.name = name.into();
        column.oid = oid;
        native.relations[relation].columns.push(column);
    }
    let mut id = native.relations[1].columns[0].clone();
    id.position = 1;
    let mut time = template;
    time.position = 2;
    time.name = "recorded_time".into();
    time.oid = 1184;
    native.relations.push(Relation {
        oid: 3,
        schema: "source".into(),
        table: "global_clock".into(),
        columns: vec![id, time],
    });
    native
}
#[test]
fn q6_clock_group_rank_average_compiles_with_complete_group_identity() -> Result<()> {
    let native = native();
    let sql = include_str!("../../../queries/nexmark/q6.sql").replace("nexmark.", "source.");
    assert!(!compile(&sql, &native)?.relational().nodes.is_empty());
    assert!(compile(&sql.replace(",closed.auction_id DESC", ""), &native).is_err());
    assert!(
        compile(
            &sql.replace("closed.expires DESC,closed.auction_id DESC", "closed.auction_id DESC"),
            &native
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn grouped_and_scoped_identity_must_survive_visible_boundaries() -> Result<()> {
    let native = native();
    let grouped = "SELECT q.auction,q.n,row_number() OVER(ORDER BY q.auction) AS position FROM(SELECT auction,COUNT(*) AS n FROM source.bid GROUP BY auction) q";
    assert!(!compile(grouped, &native)?.relational().nodes.is_empty());
    assert!(compile(&grouped.replace("ORDER BY q.auction", "ORDER BY q.n"), &native).is_err());
    let scoped = "SELECT q.auction,row_number() OVER(ORDER BY q.auction) AS position FROM(SELECT auction FROM source.bid) q";
    assert!(compile(scoped, &native).is_err());
    let keyed = scoped
        .replace("SELECT auction FROM", "SELECT id,auction FROM")
        .replace("ORDER BY q.auction", "ORDER BY q.id");
    assert!(!compile(&keyed, &native)?.relational().nodes.is_empty());
    Ok(())
}

#[test]
fn scoped_cross_join_positional_identity_requires_both_inputs() -> Result<()> {
    let native = native();
    let sql = "SELECT q.lid,q.rid,row_number() OVER(ORDER BY q.lid,q.rid) AS position FROM(SELECT l.id AS lid,r.id AS rid FROM(SELECT id FROM source.bid) l CROSS JOIN(SELECT id FROM source.auction) r) q";
    assert!(!compile(sql, &native)?.relational().nodes.is_empty());
    assert!(compile(&sql.replace("q.lid,q.rid)", "q.lid)"), &native).is_err());
    assert!(compile(&sql.replace("q.lid,q.rid)", "q.rid)"), &native).is_err());
    Ok(())
}
