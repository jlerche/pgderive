use super::super::program;
use super::{batch, contract, row, settings};
use crate::{
    compiler::{Compiled, compile},
    engine::Batch,
    transaction::Row,
    weighted,
};
use anyhow::Result;
use std::collections::BTreeMap;

const SQL: &str = "SELECT a.category, COUNT(*), SUM(b.price) FROM source.auction a JOIN source.bid b ON a.id=b.auction WHERE a.other IS NOT NULL GROUP BY a.category";
fn native() -> crate::source::Contract {
    let mut native = contract();
    native.relations[0].columns[1].oid = 23;
    native.relations[0].columns[1].collation = 0;
    native
}
fn identity(sql: &str) -> Result<String> {
    let native = native();
    Ok(program::plan(&native, &compile(sql, &native)?)?.identity().into())
}
#[test]
fn normalized_identity_and_native_layout_are_restart_boundaries() -> Result<()> {
    let normalized = "select category as g, count(*) as n, sum(price) as s from SOURCE.AUCTION as x inner join source.bid as y on y.auction=x.id where x.other is not null and x.other is not null group by category;";
    assert_eq!(identity(SQL)?, identity(normalized)?);
    assert_eq!(identity(SQL)?, identity(&SQL.replace("SELECT", "/* comment */ SELECT"))?);
    assert_ne!(identity(SQL)?, identity(&SQL.replace("IS NOT NULL", "IS NULL"))?);
    assert_eq!(
        identity(SQL)?,
        identity(&SQL.replace("a.other IS NOT NULL", "((a.other) IS NOT NULL)"))?
    );
    assert_eq!(identity(SQL)?, identity(&SQL.replace("a.other IS NOT NULL", "a.other NOTNULL"))?);
    assert_eq!(identity(SQL)?, identity(&SQL.replace("a.category", r#"a.U&"cat\0065gory""#))?);
    assert_eq!(
        identity(SQL)?,
        identity(&SQL.replace("a.category", r#"a.U&"cat!0065gory" UESCAPE '!'"#))?
    );
    assert_eq!(
        identity(SQL)?,
        identity(
            &SQL.replace("source.auction a", "source.auction between").replace("a.", "between.")
        )?
    );
    let mut changed = native();
    changed.relations[1].columns[2].nullable = false;
    assert_ne!(identity(SQL)?, program::plan(&changed, &compile(SQL, &changed)?)?.identity());
    let mut compiled = compile(SQL, &native())?;
    compiled.revision =
        Some("sql-grouped-v1:row-text-v1:json-v2:group-string-v1:i64-sum-v1".into());
    assert_ne!(identity(SQL)?, program::plan(&native(), &compiled)?.identity());
    let mut selectors = compile(SQL, &native())?.selectors;
    selectors.group = "id".into();
    let legacy = Compiled::legacy(selectors, &native())?;
    assert_ne!(
        identity(&SQL.replace("a.category", "a.id"))?,
        program::plan(&native(), &legacy)?.identity()
    );
    Ok(())
}
#[test]
fn rejects_unsupported_syntax_names_types_and_shapes() {
    let unsupported = [
        SQL.replace("a.category", "id"),
        SQL.replace("source.auction", "auction"),
        SQL.replace("a.id=b.auction", "a.id=a.category"),
        SQL.replace("a.id=b.auction", "a.id=b.id AND b.price IS NULL"),
        SQL.replace("a.category", "b.auction"),
        SQL.replace("SUM(b.price)", "SUM(a.id)"),
        SQL.replace("SUM(b.price)", "SUM(a.other)"),
        SQL.replace("COUNT(*)", "COUNT(b.price)"),
        SQL.replace("COUNT(*)", "COUNT(DISTINCT b.price)"),
        SQL.replace("COUNT(*)", "COUNT(*) FILTER (WHERE b.price IS NULL)"),
        SQL.replace("COUNT(*)", "COUNT(*) OVER ()"),
        SQL.replace("SUM(b.price)", "SUM(VARIADIC b.price)"),
        SQL.replace("SUM(b.price)", "SUM(b.price ORDER BY b.id)"),
        SQL.replace("SUM(b.price)", "SUM(b.price) WITHIN GROUP (ORDER BY b.id)"),
        SQL.replace("SUM(b.price)", "pg_catalog.SUM(b.price)"),
        SQL.replace("a.category", "a.*"),
        SQL.replace("a.other IS NOT NULL", "ROW(a.other) IS NULL"),
        SQL.replace("GROUP BY", "GROUP BY DISTINCT"),
        SQL.replace("GROUP BY a.category", "GROUP BY a.category, a.id"),
        SQL.replace("SELECT", "SELECT DISTINCT"),
        SQL.replace("FROM source.auction", "INTO source.sink FROM source.auction"),
        SQL.replace("source.auction a", "ONLY source.auction a"),
        SQL.replace("source.auction a", "source.auction a(id)"),
        SQL.replace("a.id=b.auction", "a.id OPERATOR(pg_catalog.=) b.auction"),
        SQL.replace("JOIN source.bid b ON a.id=b.auction", "JOIN source.bid b USING (id)"),
        SQL.replace("JOIN source.bid b ON a.id=b.auction", "NATURAL JOIN source.bid b"),
        SQL.replace("source.auction a", "(SELECT * FROM source.auction) a"),
        format!("{SQL} LIMIT ALL"),
        format!("{SQL} FOR UPDATE"),
        format!("{SQL} HAVING COUNT(*) > 0"),
        format!("{SQL} UNION ALL {SQL}"),
        format!("WITH x AS (SELECT 1) {SQL}"),
        "DELETE FROM source.auction".into(),
        SQL.replace("a.other", &format!("{}a.other{}", "(".repeat(65), ")".repeat(65))),
        format!("{SQL} {}", " + ".repeat(2048)),
        SQL.replace(
            "a.other IS NOT NULL",
            &std::iter::repeat_n("a.other IS NULL", 512).collect::<Vec<_>>().join(" AND "),
        ),
        SQL.replace("IS NOT NULL", "> 1"),
        SQL.replace("JOIN", "LEFT JOIN"),
        SQL.replace("source.bid b", "source.auction b"),
        SQL.replace("source.bid b", "source.bid a"),
        SQL.replace("a.other", "auction.other"),
        SQL.replace("a.other", "a.missing"),
        SQL.replace("source.bid", "source.missing"),
        SQL.replace("GROUP BY a.category", "GROUP BY a.id"),
        format!("{SQL} ORDER BY a.category"),
        format!("{SQL}; {SQL}"),
        "SELECT * FROM source.auction".into(),
        SQL.replace("a.category", "a.\"Category\""),
        SQL.replace("source.auction", "db.source.auction"),
        SQL.replace("a.other", "a.b.c.d"),
        SQL.replace("a.other", "\"\""),
        SQL.replace("a.other", &format!("a.\"{}\"", "x".repeat(64))),
        SQL.replace("a.other", "é"),
        SQL.replace("a.other", "\"unfinished"),
        format!("{SQL}\0"),
        "x".repeat(16_385),
        String::new(),
    ];
    for sql in unsupported {
        assert!(compile(&sql, &native()).is_err(), "accepted {sql}");
    }
    assert!(compile(SQL, &contract()).is_err()); // text grouping
    let mut wrong = native();
    wrong.relations[1].columns[1].oid = 20;
    assert!(compile(SQL, &wrong).is_err());
    wrong = native();
    wrong.relations[1].columns[2].oid = 25;
    assert!(compile(SQL, &wrong).is_err());
    wrong = native();
    wrong.relations[0].columns[0].oid = 25;
    wrong.relations[1].columns[1].oid = 25;
    assert!(compile(SQL, &wrong).is_err());
}
#[test]
fn quoted_names_qualifiers_and_conjunctions() -> Result<()> {
    let sql = "SELECT source.auction.category, COUNT(*), SUM(source.bid.price) FROM source.auction JOIN source.bid ON source.bid.auction=source.auction.id WHERE source.bid.price IS NULL AND source.auction.other IS NOT NULL GROUP BY source.auction.category";
    let first = compile(sql, &native())?;
    let second = compile(
        &sql.replace(
            "source.bid.price IS NULL AND source.auction.other IS NOT NULL",
            "source.auction.other IS NOT NULL AND source.bid.price IS NULL",
        ),
        &native(),
    )?;
    assert_eq!(
        program::plan(&native(), &first)?.identity(),
        program::plan(&native(), &second)?.identity()
    );
    let mut names = native();
    names.relations[0].schema = "Source space".into();
    names.relations[0].table = "Auction\"Name".into();
    names.relations[0].columns[1].name = "Groupé".into();
    let quoted = "SELECT \"A\".\"Groupé\", COUNT(*), SUM(b.price) FROM \"Source space\".\"Auction\"\"Name\" AS \"A\" JOIN source.bid b ON \"A\".id=b.auction GROUP BY \"A\".\"Groupé\"";
    compile(quoted, &names)?;
    assert!(compile(&quoted.replace("\"A\".", "a."), &names).is_err());
    let unicode = quoted.replace(r#""Groupé""#, r#"U&"Group\00e9""#);
    assert_eq!(
        program::plan(&names, &compile(quoted, &names)?)?.identity(),
        program::plan(&names, &compile(&unicode, &names)?)?.identity()
    );
    names.relations[0].columns[1].name = "x".repeat(63);
    let truncated = SQL.replace("a.category", &format!("a.\"{}\"", "x".repeat(64)));
    names.relations[0].schema = "source".into();
    names.relations[0].table = "auction".into();
    compile(&truncated, &names)?;
    let defined: crate::worker::QueryDefinition =
        serde_json::from_value(serde_json::json!({"sql":SQL}))?;
    defined.compile(&native())?;
    assert!(
        serde_json::from_value::<crate::worker::QueryDefinition>(
            serde_json::json!({"sql":SQL,"group":"id"})
        )
        .is_err()
    );
    Ok(())
}

// Independent full source-map recomputation: no compiled expression/operator calls.
type Output = Batch<Option<String>, (i64, Option<i64>)>;
fn oracle(tables: &[BTreeMap<Row, i64>; 2]) -> Result<Output> {
    let mut groups = BTreeMap::<Option<String>, (i64, i64, i64)>::new();
    for (left, lhs) in &tables[0] {
        for (right, rhs) in &tables[1] {
            if left["id"].is_none() || left["id"] != right["auction"] || left["other"].is_none() {
                continue;
            }
            let entry = groups.entry(left["category"].clone()).or_default();
            let weight = lhs * rhs;
            entry.0 += weight;
            if let Some(price) = &right["price"] {
                entry.1 += weight;
                entry.2 += weight * price.parse::<i64>()?;
            }
        }
    }
    Batch::from_updates(groups.into_iter().filter(|(_, state)| state.0 != 0).map(
        |(group, (count, non_null, total))| ((group, (count, (non_null != 0).then_some(total))), 1),
    ))
}
async fn step(
    query: &mut program::Query,
    compiled: &Compiled,
    tables: &mut [BTreeMap<Row, i64>; 2],
    changes: weighted::Batch,
) -> Result<()> {
    for update in &changes.updates {
        let index = usize::from(update.tuple.table == "bid");
        *tables[index].entry(update.tuple.row.clone()).or_default() += update.weight;
    }
    let prepared =
        query.prepare(program::inputs(&changes, &compiled.selectors, query.time() + 1)?).await?;
    query.commit(prepared)?;
    assert_eq!(query.snapshot().output.materialize().await?, oracle(tables)?);
    Ok(())
}
#[tokio::test]
async fn compiled_history_matches_independent_memory_oracle_and_cold_restart() -> Result<()> {
    let compiled = compile(SQL, &native())?;
    let options = settings();
    let mut query = program::build(&native(), &compiled, options.clone())?;
    let mut tables = [BTreeMap::new(), BTreeMap::new()];
    let left = row(&[("id", Some("1")), ("category", None), ("other", Some("yes"))]);
    let nil = row(&[("id", Some("3")), ("auction", Some("1")), ("price", None)]);
    let bid = row(&[("id", Some("4")), ("auction", Some("1")), ("price", Some("7"))]);
    let absent = row(&[("id", Some("5")), ("auction", None), ("price", Some("100"))]);
    step(
        &mut query,
        &compiled,
        &mut tables,
        batch(vec![
            ("auction", left.clone(), 1),
            ("bid", nil.clone(), 1),
            ("bid", bid.clone(), 1),
            ("bid", absent, 1),
        ]),
    )
    .await?;
    let mut next_left = left.clone();
    next_left.insert("category".into(), Some("2".into()));
    let mut next_bid = bid.clone();
    next_bid.insert("price".into(), Some("9".into()));
    step(
        &mut query,
        &compiled,
        &mut tables,
        batch(vec![
            ("auction", left, -1),
            ("auction", next_left.clone(), 1),
            ("bid", bid, -1),
            ("bid", next_bid.clone(), 1),
        ]),
    )
    .await?;
    let checkpoint = query.checkpoint()?;
    query = program::build(&native(), &compile(SQL, &native())?, options)?;
    query.restore_checkpoint(checkpoint).await?;
    assert_eq!(query.snapshot().output.materialize().await?, oracle(&tables)?);
    let mut excluded = next_left.clone();
    excluded.insert("other".into(), None);
    step(
        &mut query,
        &compiled,
        &mut tables,
        batch(vec![
            ("auction", next_left.clone(), -1),
            ("auction", excluded.clone(), 1),
            ("bid", next_bid, -1),
        ]),
    )
    .await?;
    step(
        &mut query,
        &compiled,
        &mut tables,
        batch(vec![("auction", excluded, -1), ("auction", next_left, 1)]),
    )
    .await?;
    assert_eq!(
        query.snapshot().output.materialize().await?,
        Batch::from_updates([((Some("2".into()), (1, None)), 1)])?
    );
    step(&mut query, &compiled, &mut tables, batch(vec![("bid", nil, -1)])).await?;
    let empty = row(&[]);
    assert!(compiled.qualifies((&empty, &empty)).is_err());
    Ok(())
}

#[tokio::test]
async fn right_null_predicate_and_overflow_fail_without_advancing_state() -> Result<()> {
    let compiled = compile(&SQL.replace("WHERE", "WHERE b.price IS NULL AND"), &native())?;
    let mut query = program::build(&native(), &compiled, settings())?;
    let left = row(&[("id", Some("1")), ("category", None), ("other", Some("yes"))]);
    let nil = row(&[("id", Some("3")), ("auction", Some("1")), ("price", None)]);
    let source = batch(vec![("auction", left.clone(), 1), ("bid", nil.clone(), 1)]);
    let prepared = query.prepare(program::inputs(&source, &compiled.selectors, 1)?).await?;
    query.commit(prepared)?;
    let expected = Batch::from_updates([((None, (1, None)), 1)])?;
    assert_eq!(query.snapshot().output.materialize().await?, expected);
    let mut value = nil.clone();
    value.insert("price".into(), Some("7".into()));
    let changes = batch(vec![("bid", nil.clone(), -1), ("bid", value.clone(), 1)]);
    let prepared = query.prepare(program::inputs(&changes, &compiled.selectors, 2)?).await?;
    query.commit(prepared)?;
    assert_eq!(query.snapshot().output.materialize().await?, Batch::from_updates([])?);
    let changes = batch(vec![("bid", value, -1), ("bid", nil, 1)]);
    let prepared = query.prepare(program::inputs(&changes, &compiled.selectors, 3)?).await?;
    query.commit(prepared)?;
    assert_eq!(query.snapshot().output.materialize().await?, expected);

    let compiled = compile(SQL, &native())?;
    let query = program::build(&native(), &compiled, settings())?;
    let huge =
        row(&[("id", Some("4")), ("auction", Some("1")), ("price", Some("9223372036854775807"))]);
    let extra = row(&[("id", Some("5")), ("auction", Some("1")), ("price", Some("1"))]);
    let source = batch(vec![("auction", left, 1), ("bid", huge, 1), ("bid", extra, 1)]);
    let before = query.checkpoint()?;
    assert!(query.prepare(program::inputs(&source, &compiled.selectors, 1)?).await.is_err());
    assert_eq!(query.checkpoint()?, before);
    let mut revised = compiled;
    revised.revision = Some("incompatible".into());
    let mut other = program::build(&native(), &revised, settings())?;
    assert!(other.restore_checkpoint(before).await.is_err());
    Ok(())
}
