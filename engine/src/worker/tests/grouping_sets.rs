use super::{History, JoinedState, RowBatch, apply_joined, batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row};
use anyhow::Result;
use std::collections::BTreeMap;

#[test]
fn grouping_syntax_composes_with_ordinary_query_clauses() -> Result<()> {
    let contract = contract();
    for sql in [
        "SELECT category, COUNT(*) FROM source.auction GROUP BY GROUPING SETS ((category), ())",
        "SELECT category, other, GROUPING(category, other), COUNT(*) FROM source.auction GROUP BY ROLLUP ((category, other))",
        "SELECT category, other, GROUPING(category, other), COUNT(*) FROM source.auction GROUP BY CUBE(category, other)",
        "SELECT category AS k, GROUPING(category), COUNT(*) FROM source.auction GROUP BY DISTINCT GROUPING SETS ((k), (1), ())",
        "SELECT category, GROUPING(category), COUNT(*) FROM source.auction GROUP BY ROLLUP(category) HAVING GROUPING(category)=1 ORDER BY GROUPING(category)",
        "SELECT category, GROUPING(category), COUNT(*) FROM source.auction GROUP BY category",
        "SELECT COUNT(*) FROM source.auction GROUP BY GROUPING SETS ((), ())",
        "SELECT GROUPING(category, other), COUNT(*) FROM source.auction GROUP BY (category, other)",
        "SELECT GROUPING(+id), COUNT(*) FROM source.auction GROUP BY ROLLUP(+id)",
        "SELECT 'literal' AS k, GROUPING('literal'::text), COUNT(*) FROM source.auction GROUP BY ROLLUP(k)",
        "SELECT GROUPING((category::varchar(3))::varchar(3)), COUNT(*) FROM source.auction GROUP BY ROLLUP(category::varchar(3))",
        "SELECT id, category FROM source.auction GROUP BY id",
        "SELECT id::integer, category FROM source.auction GROUP BY 1",
        "SELECT id, category, COUNT(*) FROM source.auction GROUP BY id",
        "SELECT id, category, COUNT(*) FROM source.auction GROUP BY 1 HAVING category IS NOT NULL",
        "SELECT a.category, COUNT(*) FROM source.auction a JOIN source.bid b USING (id) GROUP BY id",
        "SELECT a.category, COUNT(*) FROM source.auction a FULL JOIN source.bid b USING (id) GROUP BY a.id",
        "SELECT a.category, GROUPING(a.id), COUNT(*) FROM source.auction a JOIN source.bid b ON a.id=b.auction GROUP BY a.id, ROLLUP(b.price)",
        "SELECT id, category, GROUPING(id), COUNT(*) FROM source.auction GROUP BY id, ROLLUP(other)",
    ] {
        compile(sql, &contract).map_err(|error| error.context(sql))?;
    }
    for sql in [
        "SELECT GROUPING(other) FROM source.auction GROUP BY ROLLUP(category)",
        "SELECT other, COUNT(*) FROM source.auction GROUP BY ROLLUP(category)",
        "SELECT GROUPING(category) FROM source.auction",
        "SELECT GROUPING(+id) FROM source.auction GROUP BY ROLLUP(id)",
        "SELECT GROUPING(id) FROM source.auction GROUP BY ROLLUP(+id)",
        "SELECT GROUPING(-id) FROM source.auction GROUP BY ROLLUP(0-id)",
        "SELECT 'literal' AS k, GROUPING('literal') FROM source.auction GROUP BY ROLLUP(k)",
        "SELECT id, category, COUNT(*) FROM source.auction GROUP BY ROLLUP(id)",
        "SELECT b.price, COUNT(*) FROM source.auction a LEFT JOIN source.bid b USING (id) GROUP BY id",
        "SELECT a.category, COUNT(*) FROM source.auction a FULL JOIN source.bid b USING (id) GROUP BY id",
        "SELECT d.category, COUNT(*) FROM (SELECT id,category FROM source.auction) d JOIN source.bid b ON d.id=b.auction GROUP BY d.id",
        "SELECT +id, category FROM source.auction GROUP BY 1",
        "SELECT id::bigint, category FROM source.auction GROUP BY 1",
        "SELECT id, category FROM (SELECT id, category FROM source.auction) s GROUP BY id",
        "SELECT id, category, GROUPING(category), COUNT(*) FROM source.auction GROUP BY id, ROLLUP(other)",
        "SELECT id, category, COUNT(*) FROM (SELECT id, category FROM source.auction) s GROUP BY id, GROUPING SETS(())",
    ] {
        assert!(compile(sql, &contract).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn deferrable_primary_key_does_not_prove_grouping_dependency() -> Result<()> {
    let mut native = contract();
    native.relations[0].primary_key_deferrable = true;
    for sql in [
        "SELECT category, COUNT(*) FROM source.auction GROUP BY id",
        "SELECT category, COUNT(*) FROM source.auction GROUP BY id, ROLLUP(other)",
        "SELECT a.category, COUNT(*) FROM source.auction a JOIN source.bid b
         ON a.id=b.auction GROUP BY a.id",
    ] {
        assert!(compile(sql, &native).is_err(), "{sql}");
    }
    compile("SELECT category, COUNT(*) FROM source.auction GROUP BY id, category", &native)?;
    Ok(())
}

#[tokio::test]
async fn grouping_set_deltas_keep_repeated_groups_and_generated_nulls() -> Result<()> {
    let contract = contract();
    let compiled = compile(
        "SELECT category, GROUPING(category), COUNT(*) FROM source.auction
         GROUP BY GROUPING SETS ((category), (), (category), ())",
        &contract,
    )?;
    let names = compiled.output().columns.iter().map(|c| c.column.name.clone()).collect::<Vec<_>>();
    let options = settings();
    let null = row(&[("id", Some("1")), ("category", None), ("other", None)]);
    let text = row(&[("id", Some("2")), ("category", Some("a")), ("other", None)]);
    let oracle = |state: &JoinedState| -> Result<RowBatch> {
        let mut groups = BTreeMap::new();
        let mut total = 0;
        for (row, weight) in &state.0 {
            *groups.entry(row["category"].clone()).or_insert(0) += weight;
            total += weight;
        }
        let output = |key, mask: &str, count: i64| {
            let values = [key, Some(mask.into()), Some(count.to_string())];
            (
                (
                    crate::worker::program::relational::Key::new(),
                    names.iter().cloned().zip(values).collect::<Row>(),
                ),
                2,
            )
        };
        Batch::from_updates(
            groups
                .into_iter()
                .filter(|(_, count)| *count > 0)
                .map(|(key, count)| output(key, "0", count))
                .chain([output(None, "1", total)]),
        )
    };
    History::new((&contract, &compiled, &options))
        .run(
            [
                batch(vec![]),
                batch(vec![("auction", null.clone(), 1), ("auction", text.clone(), 1)]),
                batch(vec![("auction", null, -1), ("auction", text, -1)]),
            ],
            oracle,
            apply_joined,
            |_| true,
        )
        .await?;
    Ok(())
}
