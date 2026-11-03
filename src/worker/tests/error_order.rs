use anyhow::Result;

const CORPUS: &str = include_str!("../../../scripts/error_order_corpus.json");

#[derive(serde::Deserialize)]
struct Case {
    sql: String,
    raises: bool,
    #[serde(default)]
    rows: Vec<String>,
    #[serde(default = "auction_row")]
    auction: Vec<[Option<i32>; 3]>,
    #[serde(default = "bid_row")]
    bid: Vec<[Option<i32>; 3]>,
}
fn auction_row() -> Vec<[Option<i32>; 3]> {
    vec![[Some(1), None, Some(0)]]
}
fn bid_row() -> Vec<[Option<i32>; 3]> {
    vec![[Some(1), Some(1), Some(1)]]
}
fn rows<'a>(
    table: &'a str,
    names: [&str; 3],
    rows: &[[Option<i32>; 3]],
) -> Vec<(&'a str, crate::transaction::Row, i64)> {
    rows.iter()
        .map(|values| {
            let row = names
                .iter()
                .zip(values)
                .map(|(name, value)| ((*name).to_owned(), value.map(|value| value.to_string())))
                .collect();
            (table, row, 1)
        })
        .collect()
}

fn integers() -> crate::source::Contract {
    let mut native = super::contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    native.relations[1].columns[2].oid = 23;
    native.relations[1].columns[2].collation = 0;
    native
}

/// The sorted output lines, as `psql -At` prints them; `None` when the query raises.
async fn outcome(case: &Case) -> Result<Option<Vec<String>>> {
    let native = integers();
    let compiled = match crate::compiler::compile(&case.sql, &native) {
        Ok(compiled) => compiled,
        Err(error) if format!("{error:#}").contains("constant expression failed") => {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let query = crate::worker::program::relational::build(&native, &compiled, &super::settings())?;
    let mut updates = rows("auction", ["id", "category", "other"], &case.auction);
    updates.extend(rows("bid", ["id", "auction", "price"], &case.bid));
    let batch = super::batch(updates);
    let inputs = crate::worker::program::relational::inputs(&batch, compiled.relational(), 1)?;
    let prepared = match query.prepare(inputs).await {
        Ok(prepared) => prepared,
        Err(error) if error.is::<crate::engine::execution::EvaluationFailed>() => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut lines = Vec::new();
    for ((_, row), weight) in prepared.output().batch.iter() {
        let line =
            compiled.output().columns.iter().map(|output| {
                row.get(&output.column.name).and_then(Clone::clone).unwrap_or_default()
            });
        let line = line.collect::<Vec<_>>().join("|");
        lines.extend(std::iter::repeat_n(line, usize::try_from(*weight)?));
    }
    lines.sort();
    Ok(Some(lines))
}

#[tokio::test]
async fn expression_errors_match_postgresql() -> Result<()> {
    let mut different = Vec::new();
    for case in serde_json::from_str::<Vec<Case>>(CORPUS)? {
        let expected = (!case.raises).then(|| case.rows.clone());
        match outcome(&case).await {
            Ok(result) if result == expected => {}
            result => different.push(format!(
                "{} auction={:?} bid={:?} postgres={expected:?} pgderive={:?}",
                case.sql,
                case.auction,
                case.bid,
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

#[tokio::test]
async fn implied_equality_spans_integer_widths() -> Result<()> {
    let mut native = integers();
    native.relations[1].columns[0].oid = 20;
    let sql = "SELECT a.id FROM source.auction a JOIN source.bid b ON a.id=b.id WHERE a.id=1 AND 1/b.price>0";
    let compiled = crate::compiler::compile(sql, &native)?;
    let query = crate::worker::program::relational::build(&native, &compiled, &super::settings())?;
    let batch = super::batch(vec![
        ("auction", super::row(&[("id", Some("1")), ("category", None), ("other", Some("1"))]), 1),
        ("bid", super::row(&[("id", Some("1")), ("auction", None), ("price", Some("1"))]), 1),
        ("bid", super::row(&[("id", Some("2")), ("auction", None), ("price", Some("0"))]), 1),
    ]);
    let inputs = crate::worker::program::relational::inputs(&batch, compiled.relational(), 1)?;
    let prepared = query.prepare(inputs).await?;
    assert_eq!(prepared.output().batch.iter().count(), 1);
    Ok(())
}

#[test]
fn set_operation_keeps_sink_proof_beside_not_null_column() -> Result<()> {
    let sql = "SELECT s.k FROM (SELECT other AS k FROM source.auction UNION SELECT id FROM source.bid) s WHERE s.k IS NOT NULL";
    let native = integers();
    let compiled = crate::compiler::compile(sql, &native)?;
    crate::compiler::properties::Analysis::infer(&native, compiled.relational())?
        .prove_sink(&compiled, &["k".to_owned()])
}
#[test]
fn materialized_boundary_keeps_sink_proof() -> Result<()> {
    let sql = "WITH w AS MATERIALIZED (SELECT other AS k FROM source.auction WHERE other IS NOT NULL) SELECT k FROM w GROUP BY k";
    let native = integers();
    let compiled = crate::compiler::compile(sql, &native)?;
    crate::compiler::properties::Analysis::infer(&native, compiled.relational())?
        .prove_sink(&compiled, &["k".to_owned()])
}
