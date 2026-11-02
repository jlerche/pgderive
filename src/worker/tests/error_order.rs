use anyhow::Result;

const CORPUS: &str = include_str!("../../../scripts/error_order_corpus.json");

#[derive(serde::Deserialize)]
struct Case {
    sql: String,
    raises: bool,
}

async fn raises(sql: &str) -> Result<bool> {
    let mut native = super::contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    native.relations[1].columns[2].oid = 23;
    native.relations[1].columns[2].collation = 0;
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
