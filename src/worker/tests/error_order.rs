use anyhow::Result;

const CORPUS: &str = include_str!("../../../scripts/error_order_corpus.json");

#[derive(serde::Deserialize)]
struct Case {
    sql: String,
    raises: bool,
}

fn integers() -> crate::source::Contract {
    let mut native = super::contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    native.relations[1].columns[2].oid = 23;
    native.relations[1].columns[2].collation = 0;
    native
}

async fn raises(sql: &str) -> Result<bool> {
    let native = integers();
    let compiled = match crate::compiler::compile(sql, &native) {
        Ok(compiled) => compiled,
        Err(error) if format!("{error:#}").contains("constant expression failed") => {
            return Ok(true);
        }
        Err(error) => return Err(error),
    };
    let query = crate::worker::program::relational::build(&native, &compiled, &super::settings())?;
    let batch = super::batch(vec![
        ("auction", super::row(&[("id", Some("1")), ("category", None), ("other", Some("0"))]), 1),
        ("bid", super::row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("1"))]), 1),
    ]);
    let inputs = crate::worker::program::relational::inputs(&batch, compiled.relational(), 1)?;
    match query.prepare(inputs).await {
        Ok(_) => Ok(false),
        Err(error) if error.is::<crate::engine::execution::EvaluationFailed>() => Ok(true),
        Err(error) => Err(error),
    }
}

#[tokio::test]
async fn expression_errors_match_postgresql() -> Result<()> {
    let mut different = Vec::new();
    for case in serde_json::from_str::<Vec<Case>>(CORPUS)? {
        match raises(&case.sql).await {
            Ok(raised) if raised == case.raises => {}
            result => different.push(format!(
                "{} postgres_raises={} pgderive={:?}",
                case.sql,
                case.raises,
                result.map_err(|error| format!("{error:#}"))
            )),
        }
    }
    assert!(different.is_empty(), "{}", different.join("\n"));
    Ok(())
}

async fn second_tick_raises(sql: &str, repaired: bool) -> Result<bool> {
    let native = integers();
    let compiled = crate::compiler::compile(sql, &native)?;
    let ir = compiled.relational();
    let mut query =
        crate::worker::program::relational::build(&native, &compiled, &super::settings())?;
    let auction =
        |other: &str| super::row(&[("id", Some("1")), ("category", None), ("other", Some(other))]);
    let first = super::batch(vec![("auction", auction("5"), 1)]);
    let prepared =
        query.prepare(crate::worker::program::relational::inputs(&first, ir, 1)?).await?;
    query.commit(prepared)?;
    let bid = super::row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("5"))]);
    let mut updates = vec![("bid", bid, 1)];
    if repaired {
        updates.extend([("auction", auction("5"), -1), ("auction", auction("6"), 1)]);
    }
    let merged = super::batch(updates);
    match query.prepare(crate::worker::program::relational::inputs(&merged, ir, 2)?).await {
        Ok(_) => Ok(false),
        Err(error) if error.is::<crate::engine::execution::EvaluationFailed>() => Ok(true),
        Err(error) => Err(error),
    }
}

#[tokio::test]
async fn repairing_the_other_join_side_in_a_merged_epoch_clears_the_failure() -> Result<()> {
    const JOIN: &str = "source.auction a JOIN source.bid b ON b.auction=a.id";
    const LEFT: &str = "source.auction a LEFT JOIN source.bid b ON b.auction=a.id";
    for sql in [
        format!("SELECT a.id, 1/(b.price-a.other) AS v FROM {JOIN}"),
        format!("SELECT a.id FROM {JOIN} WHERE 1/(b.price-a.other)>-9"),
        format!("SELECT a.id, 1/(b.price-a.other) AS v FROM {LEFT}"),
        format!("SELECT a.id, sum(1/(b.price-a.other)) AS v FROM {JOIN} GROUP BY a.id"),
        format!("SELECT a.id FROM {JOIN} AND 1/(b.price-a.other)>-9"),
    ] {
        assert!(second_tick_raises(&sql, false).await?, "unrepaired accepted: {sql}");
        assert!(!second_tick_raises(&sql, true).await?, "repaired raised: {sql}");
    }
    Ok(())
}
