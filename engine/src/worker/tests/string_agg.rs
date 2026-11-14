use super::{History, JoinedState, RowBatch, apply_joined, batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row};
use anyhow::Result;

#[tokio::test]
async fn string_agg_tracks_ordered_occurrences_and_row_delimiters() -> Result<()> {
    let contract = contract();
    let compiled = compile(
        "SELECT string_agg(category, other ORDER BY id DESC) FILTER (WHERE id > 0)
         FROM source.auction",
        &contract,
    )?;
    let name = compiled.output().columns[0].column.name.clone();
    let options = settings();
    let a = row(&[("id", Some("1")), ("category", Some("a")), ("other", Some("/"))]);
    let b = row(&[("id", Some("2")), ("category", Some("b")), ("other", Some("!"))]);
    let c = row(&[("id", Some("3")), ("category", Some("c")), ("other", None)]);
    let null = row(&[("id", Some("4")), ("category", None), ("other", Some("?"))]);
    let ignored = row(&[("id", Some("0")), ("category", Some("x")), ("other", Some("?"))]);
    let expected = |state: &JoinedState| -> Result<RowBatch> {
        let mut result = None;
        for current in [&c, &b, &a] {
            let count =
                state.0.iter().find(|(row, _)| *row == current).map_or(0, |(_, count)| *count);
            for _ in 0..count {
                append_expected(&mut result, current);
            }
        }
        Batch::from_updates([(
            (crate::worker::program::relational::Key::new(), Row::from([(name.clone(), result)])),
            1,
        )])
    };
    History::new((&contract, &compiled, &options))
        .run(
            [
                batch(vec![]),
                batch(vec![
                    ("auction", a.clone(), 2),
                    ("auction", b.clone(), 1),
                    ("auction", null, 1),
                    ("auction", ignored, 1),
                ]),
                batch(vec![("auction", a.clone(), -1), ("auction", c.clone(), 1)]),
                batch(vec![
                    ("auction", a.clone(), -1),
                    ("auction", b.clone(), -1),
                    ("auction", c.clone(), -1),
                ]),
            ],
            expected,
            apply_joined,
            |_| true,
        )
        .await?;
    Ok(())
}

fn append_expected(result: &mut Option<String>, row: &Row) {
    if let Some(output) = result {
        output.push_str(row["other"].as_deref().unwrap_or(""));
    }
    result.get_or_insert_with(String::new).push_str(row["category"].as_deref().unwrap_or(""));
}

#[test]
fn string_agg_checks_distinct_arguments_and_sort_domains() -> Result<()> {
    let mut contract = contract();
    contract.relations[0].columns[1].oid = 1043;
    contract.relations[0].columns[1].collation = 950;
    let expanded = "SELECT string_agg(b.id::text, ',') OVER (ORDER BY b.id, w.n)
        FROM source.bid b CROSS JOIN generate_series(0,2) w(n)";
    compile(expanded, &contract)?;
    assert!(compile(&expanded.replace(", w.n", ""), &contract).is_err());
    for sql in [
        "SELECT string_agg(DISTINCT category, ',' ORDER BY category) FROM source.auction",
        "SELECT string_agg(DISTINCT category COLLATE \"C\", ',' ORDER BY category::text COLLATE \"C\") FROM source.auction",
        "SELECT string_agg(category, NULL ORDER BY id USING > NULLS LAST) FROM source.auction",
        "SELECT string_agg(v, ',' ORDER BY id) FROM (VALUES (1, 'a'), (1, 'a')) t(id, v)",
        "SELECT string_agg(category, other) OVER (ORDER BY id ROWS 1 PRECEDING) FROM source.auction",
        "SELECT string_agg(MIN(category COLLATE \"C\"), ':') OVER (ORDER BY id) FROM source.auction GROUP BY id",
        "SELECT string_agg(category, ',' ORDER BY INTERVAL '1 hour', id) FROM source.auction",
        "SELECT string_agg(category, ',' ORDER BY 'constant') FROM source.auction",
    ] {
        let compiled = compile(sql, &contract).map_err(|error| error.context(sql))?;
        assert_eq!(compiled.output().columns[0].column.oid, 25);
    }
    for sql in [
        "SELECT string_agg(DISTINCT category, ',' ORDER BY category::text) FROM source.auction",
        "SELECT string_agg(DISTINCT 'x', ',' ORDER BY 'x') FROM source.auction",
        "SELECT string_agg(DISTINCT category, ',' ORDER BY id) FROM source.auction",
        "SELECT string_agg(category, ',' ORDER BY other) FROM source.auction",
        "SELECT string_agg(category, ',') FROM source.auction",
        "SELECT string_agg(category, ',') OVER () FROM source.auction",
        "SELECT string_agg(DISTINCT category, ',') OVER (ORDER BY id) FROM source.auction",
        "SELECT string_agg(category, ',' ORDER BY id) OVER () FROM source.auction",
        "SELECT string_agg(id, ',' ORDER BY id) FROM source.auction",
    ] {
        assert!(compile(sql, &contract).is_err(), "{sql}");
    }
    Ok(())
}
