//! Native DISTINCT/existence against independent raw-row bags through restart.
use super::{batch, contract, row, settings};
use crate::{compiler::compile, engine::Batch, transaction::Row, worker::program::relational};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
type State = BTreeMap<Row, i64>;
fn oracle(
    left: &State,
    right: &State,
    anti: Option<bool>,
    distinct: bool,
) -> Result<Batch<relational::Key, Row>> {
    let mut output = BTreeMap::new();
    for (a, weight) in left {
        if *weight == 0 {
            continue;
        }
        let present = right
            .iter()
            .any(|(b, weight)| *weight > 0 && a["other"].is_some() && a["other"] == b["auction"]);
        if anti.is_some_and(|anti| present == anti) {
            continue;
        }
        let row = [("0:category".into(), a["category"].clone())].into();
        *output.entry((crate::worker::program::relational::Key::new(), row)).or_insert(0_i64) +=
            weight;
    }
    Batch::from_updates(
        output.into_iter().map(|(tuple, weight)| (tuple, if distinct { 1 } else { weight })),
    )
}
#[tokio::test]
async fn membership_signed_history_matches_raw_rows_and_object_backed_restart() -> Result<()> {
    for (anti, distinct) in [
        (None, true),
        (Some(false), false),
        (Some(true), false),
        (Some(false), true),
        (Some(true), true),
    ] {
        history(anti, distinct).await?;
    }
    Ok(())
}
async fn history(anti: Option<bool>, distinct: bool) -> Result<()> {
    let mut native = contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    let clause = anti.map_or_else(String::new, |anti| {
        format!(
            " WHERE {}EXISTS (SELECT 1 FROM source.bid b WHERE b.auction=a.other)",
            if anti { "NOT " } else { "" }
        )
    });
    let sql = format!(
        "SELECT {}a.category FROM source.auction a{clause}",
        if distinct { "DISTINCT " } else { "" }
    );
    let compiled = compile(&sql, &native)?;
    let ir = compiled.relational();
    let options = settings();
    let scanned = relational::build_scanned(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("category", None), ("other", Some("1"))]);
    let a2 = row(&[("id", Some("2")), ("category", None), ("other", Some("1"))]);
    let null = row(&[("id", Some("3")), ("category", Some("null")), ("other", None)]);
    let b = row(&[("id", Some("1")), ("auction", Some("1")), ("price", None)]);
    let b2 = row(&[("id", Some("2")), ("auction", Some("1")), ("price", None)]);
    let bn = row(&[("id", Some("3")), ("auction", None), ("price", None)]);
    let histories = [
        batch(vec![
            ("auction", a.clone(), 2),
            ("auction", a2.clone(), 1),
            ("auction", null.clone(), 1),
            ("bid", b.clone(), 1),
            ("bid", b2.clone(), 1),
            ("bid", bn.clone(), 1),
        ]),
        batch(vec![("auction", a.clone(), -1), ("bid", b.clone(), -1)]),
        batch(vec![("bid", b2.clone(), -1)]),
        batch(vec![("auction", a2.clone(), -1), ("bid", b.clone(), 1)]),
        batch(vec![("auction", a, -1), ("auction", null, -1), ("bid", b, -1), ("bid", bn, -1)]),
    ];
    let query = super::History {
        scanned: Some(scanned),
        ..super::History::new((&native, &compiled, &options))
    }
    .run(
        histories,
        |state: &super::JoinedState| oracle(&state.0, &state.1, anti, distinct),
        super::apply_joined,
        |index| index == 2,
    )
    .await?;

    let before = query.checkpoint()?;
    let absent = row(&[("id", Some("999")), ("category", None), ("other", Some("1"))]);
    assert!(
        query
            .prepare(relational::inputs(
                &batch(vec![("auction", absent, -1)]),
                ir,
                query.time() + 1
            )?)
            .await
            .is_err()
    );
    assert_eq!(query.checkpoint()?, before);
    Ok(())
}
#[test]
fn unsupported_membership_shapes_fail_during_compilation() {
    let native = contract();
    for sql in [
        "SELECT DISTINCT ON (id) id+1 FROM source.auction",
        "SELECT a.id FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE b.auction+1>a.id)",
        "SELECT a.id FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE b.auction=a.id OR b.id=1)",
        "SELECT a.id FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE b.auction+1=b.id)",
        "SELECT a.id FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE b.auction=a.id OFFSET -1)",
    ] {
        assert!(compile(sql, &native).is_err(), "{sql}");
    }
}

#[tokio::test]
async fn compound_self_existence_and_repeated_distinct_columns_preserve_null_semantics()
-> Result<()> {
    let native = contract();
    let sql = "SELECT DISTINCT l.auction,l.auction AS again FROM source.bid l WHERE EXISTS(SELECT 1 FROM source.bid r WHERE r.auction=l.auction AND r.price=l.price)";
    let compiled = compile(sql, &native)?;
    let ir = compiled.relational();
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("4"))]);
    let b = row(&[("id", Some("2")), ("auction", Some("1")), ("price", Some("4"))]);
    let null = row(&[("id", Some("3")), ("auction", Some("1")), ("price", None)]);
    let null_key = row(&[("id", Some("4")), ("auction", None), ("price", Some("4"))]);
    let histories = [
        batch(vec![
            ("bid", a.clone(), 1),
            ("bid", b.clone(), 1),
            ("bid", null.clone(), 1),
            ("bid", null_key, 1),
        ]),
        batch(vec![("bid", a, -1)]),
        batch(vec![]),
        batch(vec![("bid", b, -1), ("bid", null, -1)]),
    ];
    let mut state = State::new();
    let mut previous = Batch::from_updates([])?;
    for changes in histories {
        super::apply_rows(&mut state, &changes);
        let rows = state.iter().filter(|(left, weight)| {
            **weight > 0
                && state.iter().any(|(right, count)| {
                    *count > 0
                        && ["auction", "price"]
                            .iter()
                            .all(|field| left[*field].is_some() && left[*field] == right[*field])
                })
        });
        let expected = rows
            .map(|(row, _)| {
                (
                    (
                        crate::worker::program::relational::Key::new(),
                        [("0:auction".into(), row["auction"].clone())].into(),
                    ),
                    1,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let current = Batch::from_updates(expected)?;
        let delta = Batch::from_updates(
            previous
                .iter()
                .map(|(tuple, w)| (tuple.clone(), -*w))
                .chain(current.iter().map(|(tuple, w)| (tuple.clone(), *w))),
        )?;
        let work = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        assert_eq!(work.output().batch, delta);
        query.commit(work)?;
        previous = current;
        super::restart_relational(&mut query, &native, &compiled, &options).await?;
    }
    Ok(())
}

#[test]
fn membership_identity_normalizes_aliases_and_distinguishes_anti_and_distinct() -> Result<()> {
    let native = contract();
    let semi = "SELECT a.category FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE b.auction=a.id)";
    let renamed = "SELECT l.category FROM source.auction l WHERE EXISTS(SELECT 1 FROM source.bid r WHERE l.id=r.auction)";
    let compiled = compile(semi, &native)?;
    assert_eq!(serde_json::to_vec(&compiled)?, serde_json::to_vec(&compile(renamed, &native)?)?);
    let identity = |sql: &str| -> Result<_> {
        let compiled = compile(sql, &native)?;
        Ok(relational::build(&native, &compiled, &settings())?.plan().identity().to_string())
    };
    assert_ne!(identity(semi)?, identity(&semi.replace("WHERE EXISTS", "WHERE NOT EXISTS"))?);
    assert_ne!(identity(semi)?, identity(&semi.replacen("SELECT ", "SELECT DISTINCT ", 1))?);
    Ok(())
}

#[tokio::test]
async fn distinct_threshold_retains_full_rows_without_narrowing_occurrence_totals() -> Result<()> {
    let native = contract();
    let compiled = compile("SELECT DISTINCT category FROM source.auction", &native)?;
    let ir = compiled.relational();
    let options = settings();
    let mut query = relational::build(&native, &compiled, &options)?;
    let a = row(&[("id", Some("1")), ("category", None), ("other", None)]);
    let b = row(&[("id", Some("2")), ("category", None), ("other", None)]);
    let phantom = row(&[("id", Some("999")), ("category", None), ("other", None)]);
    let histories = [
        (batch(vec![("auction", a.clone(), i64::MAX), ("auction", b.clone(), i64::MAX)]), 1),
        (batch(vec![("auction", a.clone(), -1)]), 0),
        (batch(vec![("auction", a, -(i64::MAX - 1))]), 0),
        (batch(vec![("auction", b, -i64::MAX)]), -1),
    ];
    for (index, (changes, weight)) in histories.into_iter().enumerate() {
        let work = query.prepare(relational::inputs(&changes, ir, query.time() + 1)?).await?;
        let expected = if weight == 0 {
            Batch::from_updates([])?
        } else {
            Batch::from_updates([(
                (
                    crate::worker::program::relational::Key::new(),
                    [("0:category".into(), None)].into(),
                ),
                weight,
            )])?
        };
        assert_eq!(work.output().batch, expected, "DISTINCT tick {index}");
        query.commit(work)?;
        let checkpoint = query.checkpoint()?;
        query = relational::build(&native, &compiled, &options)?;
        query.restore_checkpoint(checkpoint.clone()).await?;
        if index < 3 {
            // A projected collision cannot make an absent source row retractable.
            let invalid = batch(vec![("auction", phantom.clone(), -1)]);
            assert!(
                query.prepare(relational::inputs(&invalid, ir, query.time() + 1)?).await.is_err()
            );
            assert_eq!(query.checkpoint()?, checkpoint);
        }
    }
    Ok(())
}

#[test]
fn computed_distinct_and_grouped_existence_share_partition_lowering() -> Result<()> {
    for sql in [
        "SELECT DISTINCT abs(id) AS k FROM source.auction WHERE id>0",
        "SELECT a.category,COUNT(*) FROM source.auction a WHERE EXISTS(SELECT 1 FROM source.bid b WHERE abs(a.id)=abs(b.auction)) GROUP BY a.category",
        "SELECT a.category,COUNT(*) FROM source.auction a WHERE NOT EXISTS(SELECT 1 FROM source.bid b WHERE a.id+1=b.auction+1) GROUP BY a.category",
    ] {
        let compiled = compile(sql, &contract())?;
        assert!(!compiled.relational().nodes.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn select_distinct_cursor_avoids_cold_hot_group_reads_and_rejects_tail_retraction()
-> Result<()> {
    let native = contract();
    let compiled = compile("SELECT DISTINCT b.auction FROM source.bid b", &native)?;
    let ir = compiled.relational();
    let mut options = settings();
    options.block_rows = 1;
    options.limits.cache_entries = 0;
    options.limits.cache_bytes = 0;
    let rows = (0..256)
        .map(|id| {
            row(&[("id", Some(&id.to_string())), ("auction", Some("1")), ("price", Some("7"))])
        })
        .collect::<Vec<_>>();
    let initial = batch(rows.iter().map(|row| ("bid", row.clone(), 1)).collect());
    let delta = batch(vec![("bid", rows[255].clone(), -1)]);
    let mut seed = relational::build(&native, &compiled, &options)?;
    seed.commit(seed.prepare(relational::inputs(&initial, ir, 1)?).await?)?;
    let checkpoint = seed.checkpoint()?;
    let mut reads = Vec::new();
    for scanned in [true, false] {
        let mut query = if scanned {
            relational::build_scanned(&native, &compiled, &options)?
        } else {
            relational::build(&native, &compiled, &options)?
        };
        query.restore_checkpoint(checkpoint.clone()).await?;
        let before = query.cache_stats()?.misses;
        let prepared = query.prepare(relational::inputs(&delta, ir, 2)?).await?;
        assert_eq!(prepared.output().batch, Batch::from_updates([])?);
        reads.push(query.cache_stats()?.misses - before);
        query.commit(prepared)?;
        let committed = query.checkpoint()?;
        assert!(query.prepare(relational::inputs(&delta, ir, 3)?).await.is_err());
        assert_eq!(query.checkpoint()?, committed);
    }
    assert!(reads[1] * 10 < reads[0], "full/cursor cold reads: {reads:?}");
    eprintln!("SELECT DISTINCT full/cursor cold reads: {reads:?}");
    Ok(())
}

fn ordered_contract() -> crate::source::Contract {
    let mut native = contract();
    native.relations[0].columns[2].oid = 23;
    native.relations[0].columns[2].collation = 0;
    native
}

#[test]
fn correlated_comparison_cuts_off_within_each_equality_bucket() -> Result<()> {
    let native = ordered_contract();
    for comparison in ["IN", "NOT IN", "< ANY", ">= ALL"] {
        let query = format!(
            "SELECT a.other {comparison} (SELECT b.price FROM source.bid b
             WHERE b.auction=a.id ORDER BY b.id DESC NULLS FIRST LIMIT 2 OFFSET 1)
             FROM source.auction a"
        );
        compile(&query, &native).map_err(|error| error.context(query))?;
    }
    for rhs in [
        "SELECT DISTINCT b.price FROM source.bid b WHERE b.auction=a.id
         ORDER BY 1 ASC NULLS LAST LIMIT 1 OFFSET 1",
        "SELECT b.price FROM source.bid b WHERE b.auction=a.id
         ORDER BY b.auction FETCH FIRST 1 ROW WITH TIES",
    ] {
        let query = format!("SELECT a.other IN ({rhs}) FROM source.auction a");
        compile(&query, &native).map_err(|error| error.context(query))?;
    }
    let wildcard = "SELECT (a.id,a.id,a.other) IN (SELECT b.* FROM source.bid b
        WHERE b.auction=a.id ORDER BY b.id LIMIT 1) FROM source.auction a";
    compile(wildcard, &native).map_err(|error| error.context(wildcard))?;
    for tail in ["LIMIT 2", "ORDER BY 2 LIMIT 2", "ORDER BY b.auction LIMIT 2"] {
        let query = format!(
            "SELECT a.other IN (SELECT b.price FROM source.bid b
             WHERE b.auction=a.id {tail}) FROM source.auction a"
        );
        assert!(compile(&query, &native).is_err(), "{query}");
    }
    Ok(())
}

#[test]
fn residual_comparison_orders_after_correlation() -> Result<()> {
    let native = ordered_contract();
    for condition in ["b.auction=a.id AND b.id>a.id", "b.id>a.id"] {
        for comparison in ["IN", "< ANY", "< ALL"] {
            for tail in [
                "ORDER BY b.id DESC LIMIT 2 OFFSET 1",
                "ORDER BY 1 FETCH FIRST 1 ROW WITH TIES",
                "ORDER BY b.id LIMIT 0",
            ] {
                let query = format!(
                    "SELECT a.other {comparison} (SELECT b.price FROM source.bid b
                     WHERE {condition} {tail}) FROM source.auction a"
                );
                compile(&query, &native).map_err(|error| error.context(query))?;
            }
        }
    }
    Ok(())
}

#[test]
fn quantified_global_aggregates_use_the_single_result_or_empty_identity() -> Result<()> {
    let native = ordered_contract();
    for comparison in ["IN", "< ANY", ">= ALL"] {
        for aggregate in [
            "count(*) FROM source.bid b WHERE b.auction=a.id",
            "max(b.price) FROM source.bid b WHERE b.auction=a.id HAVING count(*)>1",
            "max(b.price) FROM source.bid b LIMIT 0",
        ] {
            let sql =
                format!("SELECT a.other {comparison} (SELECT {aggregate}) FROM source.auction a");
            compile(&sql, &native).map_err(|error| error.context(sql))?;
        }
    }
    let fallible_join = "SELECT a.other < ANY (SELECT max(b.price) FROM source.bid b
        JOIN source.bid c ON b.id=c.auction AND 1/(c.id-c.id)>0) FROM source.auction a";
    assert!(compile(fallible_join, &native).is_err());
    Ok(())
}

#[test]
fn quantified_grouped_inputs_reuse_ordinary_grouping() -> Result<()> {
    let native = ordered_contract();
    for comparison in ["< ANY", ">= ALL"] {
        for rhs in [
            "SELECT max(b.price) FROM source.bid b GROUP BY b.auction HAVING count(*)>1",
            "SELECT b.price FROM source.bid b GROUP BY ROLLUP(b.price)",
            "SELECT b.price FROM source.bid b GROUP BY 1",
        ] {
            let sql = format!("SELECT a.other {comparison} ({rhs}) FROM source.auction a");
            compile(&sql, &native).map_err(|error| error.context(sql))?.validate_execution()?;
        }
    }
    let fallible = "SELECT a.other < ANY (SELECT max(b.price) FROM source.bid b
        GROUP BY 1/(b.id-b.id)) FROM source.auction a";
    assert!(compile(fallible, &native).is_err());
    Ok(())
}

#[test]
fn equality_correlated_grouped_comparisons_preserve_local_grouping() -> Result<()> {
    let native = ordered_contract();
    for comparison in ["IN", "< ANY", ">= ALL"] {
        for tail in [
            "",
            "ORDER BY 1 DESC OFFSET 1 LIMIT 2",
            "ORDER BY peak DESC NULLS LAST FETCH FIRST 1 ROW WITH TIES",
            "LIMIT 0",
        ] {
            let sql = format!(
                "SELECT a.other {comparison} (SELECT max(b.price) AS peak FROM source.bid b
                 WHERE b.auction=a.id GROUP BY b.id HAVING count(*)>0 {tail}) FROM source.auction a"
            );
            compile(&sql, &native).map_err(|error| error.context(sql))?.validate_execution()?;
        }
    }
    for invalid in [
        "SELECT b.auction FROM source.bid b WHERE b.auction=a.id GROUP BY b.price",
        "SELECT max(b.price) FROM source.bid b WHERE b.auction=a.id AND b.id>a.id GROUP BY b.price",
        "SELECT max(b.price) FROM source.bid b WHERE b.auction=a.id GROUP BY ROLLUP(b.price)",
        "SELECT max(b.price) FROM source.bid b WHERE b.auction=a.id GROUP BY b.price LIMIT 1",
        "SELECT max(b.price) FROM source.bid b WHERE b.auction=a.id GROUP BY b.price ORDER BY 2",
    ] {
        let sql = format!("SELECT a.other < ANY ({invalid}) FROM source.auction a");
        assert!(compile(&sql, &native).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn nested_positive_exists_preserves_scope_and_presence() -> Result<()> {
    let native = ordered_contract();
    let exists = "SELECT a.id, EXISTS (SELECT 1 FROM source.bid b WHERE b.auction=a.id
        AND EXISTS (SELECT 1 FROM source.bid c WHERE c.auction=b.id AND c.price>a.other))
        FROM source.auction a";
    compile(exists, &native).map_err(|error| error.context(exists))?;
    let siblings = exists.replace(
        "c.price>a.other))",
        "c.price>a.other) AND EXISTS (SELECT 1 FROM source.bid d WHERE d.auction=b.id))",
    );
    compile(&siblings, &native).map_err(|error| error.context(siblings.clone()))?;
    let repeated_alias =
        siblings.replace("source.bid d WHERE d.auction", "source.bid c WHERE c.auction");
    compile(&repeated_alias, &native).map_err(|error| error.context(repeated_alias))?;
    let shadowed = "SELECT EXISTS (SELECT 1 FROM source.bid b WHERE b.auction=a.id
        AND EXISTS (SELECT 1 FROM source.bid b WHERE b.price>a.other AND b.auction=a.id))
        FROM source.auction a";
    compile(shadowed, &native).map_err(|error| error.context(shadowed))?;
    for invalid in [
        shadowed
            .replace("source.bid b WHERE b.price>a.other", "source.bid a WHERE a.other>b.price"),
        siblings.replace("d.auction=b.id", "d.auction=c.id"),
        exists.replace("b.auction=a.id", "b.auction=c.id"),
        exists.replace("b.auction=a.id", "auction=a.id"),
        exists.replace("AND EXISTS", "OR EXISTS"),
        exists.replace("AND EXISTS", "AND NOT EXISTS"),
        exists.replace("c.price>a.other", "c.price>a.other OFFSET 1"),
        exists.replace("source.bid c", "source.bid a"),
    ] {
        assert!(compile(&invalid, &native).is_err(), "{invalid}");
    }
    Ok(())
}

#[test]
fn correlated_comparisons_bind_distinct_keys_before_the_cutoff() -> Result<()> {
    let native = ordered_contract();
    for condition in ["b.auction=a.id", "b.auction=a.id AND b.id>a.id", "b.id>a.id"] {
        for shape in [
            "DISTINCT b.price ORDER BY 1 LIMIT 1 OFFSET 1",
            "DISTINCT ON (id) b.price AS id ORDER BY id, b.id LIMIT 1",
            "DISTINCT ON (b.auction) b.price ORDER BY b.auction, b.id DESC LIMIT 1",
        ] {
            let (projection, order) =
                shape.split_once(" ORDER BY ").context("missing test order")?;
            let sql = format!(
                "SELECT a.other IN (SELECT {projection} FROM source.bid b WHERE {condition}
                 ORDER BY {order}) FROM source.auction a"
            );
            compile(&sql, &native).map_err(|error| error.context(sql))?;
        }
    }
    let prefix = "SELECT a.other IN (SELECT DISTINCT ON (b.price) b.price FROM source.bid b
        WHERE b.id>a.id ORDER BY b.id,b.price) FROM source.auction a";
    let ambiguous = "SELECT a.other IN (SELECT DISTINCT ON (b.auction) b.price FROM source.bid b
        WHERE b.id>a.id ORDER BY b.auction) FROM source.auction a";
    assert!(compile(prefix, &native).is_err());
    assert!(compile(ambiguous, &native).is_err());
    Ok(())
}

#[tokio::test]
async fn ordered_membership_retracts_each_buckets_cutoff_independently() -> Result<()> {
    for (condition, keyed, residual) in [
        ("b.auction=a.id", true, false),
        ("b.auction=a.id AND b.id<>a.id", true, true),
        ("b.id<>a.id", false, true),
    ] {
        ordered_membership_history(condition, keyed, residual).await?;
    }
    Ok(())
}

async fn ordered_membership_history(condition: &str, keyed: bool, residual: bool) -> Result<()> {
    let native = ordered_contract();
    let sql = format!(
        "SELECT a.id, a.other IN (SELECT b.price FROM source.bid b WHERE {condition}
        ORDER BY b.id DESC LIMIT 1 OFFSET 1) AS member FROM source.auction a"
    );
    let compiled = compile(&sql, &native)?;
    let field = &compiled.output().columns[1].column.name;
    let options = settings();
    let a = row(&[("id", Some("1")), ("category", None), ("other", Some("7"))]);
    let c = row(&[("id", Some("2")), ("category", None), ("other", Some("7"))]);
    let b1 = row(&[("id", Some("1")), ("auction", Some("1")), ("price", Some("7"))]);
    let b2 = row(&[("id", Some("2")), ("auction", Some("1")), ("price", None)]);
    let b3 = row(&[("id", Some("3")), ("auction", Some("2")), ("price", Some("9"))]);
    let b4 = row(&[("id", Some("4")), ("auction", Some("2")), ("price", Some("8"))]);
    let b5 = row(&[("id", Some("5")), ("auction", Some("1")), ("price", Some("9"))]);
    let histories = [
        batch(vec![
            ("auction", a, 1),
            ("auction", c, 1),
            ("bid", b1, 1),
            ("bid", b2, 1),
            ("bid", b3, 1),
            ("bid", b4, 1),
        ]),
        batch(vec![("bid", b5.clone(), 1)]),
        batch(vec![("bid", b5, -1)]),
    ];
    super::History::new((&native, &compiled, &options))
        .run(
            histories,
            |state| ordered_probe_oracle(state, field, keyed, residual),
            super::apply_joined,
            |tick| tick == 1,
        )
        .await?;
    Ok(())
}

fn ordered_probe_oracle(
    (outer, inner): &super::JoinedState,
    field: &str,
    keyed: bool,
    residual: bool,
) -> Result<super::RowBatch> {
    Batch::from_updates(outer.iter().map(|(outer, weight)| {
        let mut sorted = inner
            .iter()
            .filter(|(rhs, count)| {
                **count > 0
                    && (!keyed || rhs["auction"] == outer["id"])
                    && (!residual || rhs["id"] != outer["id"])
            })
            .map(|(rhs, _)| rhs)
            .collect::<Vec<_>>();
        sorted.sort_by(|a, b| b["id"].cmp(&a["id"]));
        let member = sorted.get(1).map_or(Some("f"), |rhs| {
            rhs["price"].as_ref().map(|_| if rhs["price"] == outer["other"] { "t" } else { "f" })
        });
        let output = row(&[("0:id", outer["id"].as_deref()), (field, member)]);
        ((relational::Key::new(), output), *weight)
    }))
}
