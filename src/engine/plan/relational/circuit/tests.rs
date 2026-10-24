use super::*;
use crate::engine::reader::WriteLimits;
use crate::engine::{
    dataflow::{Project, Statistics},
    execution::{Consolidator, Limits, Sealed},
    plan::{Arrangement, Definition, Settings, Source},
    reader::ObjectBatch,
    trace::Run,
};
use num_bigint::BigInt;
use std::{sync::atomic::Ordering, time::Instant};

fn value(key: i64, count: i64) -> String {
    format!("{count}:{}", format!("{key:016x}").repeat(48))
}
fn operator() -> Statistics<i64, String> {
    Statistics::with_measure(
        1,
        |_, s: &String| Ok(vec![BigInt::from(s.parse::<i64>()?)]),
        |s: &String| {
            Ok(vec![BigInt::from(s.split(':').next().context("count missing")?.parse::<i64>()?)])
        },
        |key, v| {
            let count = i64::try_from(v[0].clone())?;
            ensure!(count >= 0, "negative statistic");
            Ok((count > 0).then(|| value(*key, count)))
        },
    )
}
fn execution(store: Arc<dyn object_store::ObjectStore>) -> Result<Arc<Execution<i64, String>>> {
    let plan = Plan::new(Definition {
        revision: "statistics-proof".into(),
        sources: vec![Source { id: "source".into(), schema: "wide".into() }],
        nodes: [
            ("source", Kind::Source, "source"),
            ("input", Kind::Project, "source"),
            ("stats", Kind::Statistics, "input"),
        ]
        .into_iter()
        .map(|(id, kind, input)| Node {
            id: id.into(),
            kind,
            inputs: vec![input.into()],
            schema: "wide".into(),
        })
        .collect(),
        arrangements: ["input", "stats"]
            .into_iter()
            .map(|id| Arrangement { id: id.into(), node: id.into(), schema: "wide".into() })
            .collect(),
        outputs: vec!["stats".into()],
    })?;
    let operators = [
        (
            "input".into(),
            Operator::Project(Project::new(|k: &i64, v: &String| Ok(Some((*k, v.clone()))))),
        ),
        ("stats".into(), Operator::Statistics(operator())),
    ]
    .into_iter()
    .collect();
    let settings =
        Settings { store, block_rows: 4096, limits: Limits::default(), encoded_cache: None };
    Ok(super::super::Relational::new(plan, operators, &settings)?.execution)
}
async fn fixture(
    count: i64,
) -> Result<(
    Arc<Execution<i64, String>>,
    RelationalState<i64, String>,
    Arc<crate::engine::tests::store::Store>,
)> {
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    let remote: Arc<dyn object_store::ObjectStore> = store.clone();
    let execution = execution(remote.clone())?;
    let batch = Batch::from_updates((0..count).map(|key| ((key, value(key, 1)), 1)))?;
    let roots = ObjectBatch::write_stream(
        remote.clone(),
        &mut batch.cursor().await?,
        "wide",
        WriteLimits::default(),
    )
    .await?;
    let mut runs = Vec::new();
    for root in roots {
        runs.push(Run::Object(
            ObjectBatch::open_with_cache(remote.clone(), root, "wide", execution.cache.clone())
                .await?,
        ));
    }
    let trace = execution.writers["stats"].empty().append_runs_validated(runs, 1)?;
    let state = RelationalState {
        entries: std::iter::once(("stats".into(), Entry { schema: "wide".into(), trace }))
            .collect(),
    };
    Ok((execution, state, store))
}
fn input(spill: bool) -> Result<Sealed<i64, String>> {
    let limits =
        if spill { Limits { resident_entries: 1, ..Limits::default() } } else { Limits::default() };
    let mut changes = Consolidator::new(limits)?;
    for n in 0..513 {
        changes.add(((n * 193) % 100_000, "1".into()), 1)?;
    }
    changes.seal()
}
#[tokio::test]
async fn sf10_statistics_handoff_eliminates_second_decoding_for_resident_and_spilled_deltas()
-> Result<()> {
    use futures_util::TryStreamExt as _;
    use object_store::ObjectStore as _;
    let initialized = Instant::now();
    let (execution, prior, store) = fixture(100_000).await?;
    let objects = store.list(None).try_collect::<Vec<_>>().await?;
    println!(
        "{}",
        serde_json::json!({"fixture_us":initialized.elapsed().as_micros(),"objects":objects.len(),"puts":store.puts.load(Ordering::SeqCst),"index_bytes":objects.iter().filter(|o|o.location.as_ref().contains("/batch/")).map(|o|o.size).sum::<u64>(),"stored_bytes":objects.iter().map(|o|o.size).sum::<u64>()})
    );
    for spill in [false, true] {
        let limits = if spill {
            Limits { resident_entries: 1, ..Limits::default() }
        } else {
            Limits::default()
        };
        let operator = operator();
        let lookup_before = execution.cache.stats()?;
        let lookup_started = Instant::now();
        let output =
            operator.evaluate_sealed(&input(spill)?, prior.trace("stats")?, limits).await?;
        let lookup_us = lookup_started.elapsed().as_micros();
        let lookup_after = execution.cache.stats()?;
        ensure!(
            lookup_after.decoded_bytes - lookup_before.decoded_bytes <= 12 * 1024 * 1024,
            "statistics lookup decoded too many unrelated bytes"
        );
        ensure!(
            lookup_after.decoded_rows - lookup_before.decoded_rows <= 16_000,
            "statistics lookup decoded too many unrelated rows"
        );
        assert_eq!(output.batch().resident().is_none(), spill);
        let before = execution.cache.stats()?;
        let started = Instant::now();
        let generic = execution
            .stage(
                "stats",
                &prior,
                TimedBatch { time: 2, batch: output.batch() },
                StagePolicy { staging: Staging::Memory, proof: None },
            )
            .await?
            .context("missing generic candidate")?;
        let generic_us = started.elapsed().as_micros();
        let decoded = execution.cache.stats()?.misses - before.misses;
        let reads = store.reads.load(Ordering::SeqCst);
        let before = execution.cache.stats()?;
        let started = Instant::now();
        let checked = execution
            .stage(
                "stats",
                &prior,
                TimedBatch { time: 2, batch: output.batch() },
                StagePolicy {
                    staging: Staging::Memory,
                    proof: Some(Validation::Statistics(&output)),
                },
            )
            .await?
            .context("missing checked candidate")?;
        let checked_us = started.elapsed().as_micros();
        assert_eq!(execution.cache.stats()?.misses, before.misses);
        assert_eq!(store.reads.load(Ordering::SeqCst), reads);
        ensure!(decoded > 500, "fixture did not reproduce repeated decoding");
        let expected = Batch::from_updates((0..513).flat_map(|n| {
            let key = n * 193;
            [((key, value(key, 1)), -1), ((key, value(key, 2)), 1)]
        }))?;
        assert_eq!(output.batch().materialize(Limits::default()).await?, expected);
        assert_eq!(generic.1.trace.time(), checked.1.trace.time());
        println!(
            "{}",
            serde_json::json!({"lookup_us":lookup_us,"lookup_decoded_bytes":lookup_after.decoded_bytes-lookup_before.decoded_bytes,"lookup_decoded_rows":lookup_after.decoded_rows-lookup_before.decoded_rows,"spill":spill,"generic_stage_us":generic_us,"checked_stage_us":checked_us,"generic_decode_misses":decoded,"checked_decode_misses":0})
        );
    }
    Ok(())
}
#[tokio::test]
async fn statistics_handoff_rejects_wrong_snapshot_delta_and_time_and_stages_objects() -> Result<()>
{
    let (execution, prior, _) = fixture(10).await?;
    let input: Sealed<i64, String> =
        Batch::from_updates([((1, "-1".into()), 1), ((2, "1".into()), 1), ((12, "1".into()), 1)])?
            .into();
    let operator = operator();
    let output = operator.evaluate_sealed(&input, prior.trace("stats")?, Limits::default()).await?;
    let wrong = prior.clone();
    let copied = output.batch().as_ref().clone();
    for (state, batch, time) in [
        (&wrong, output.batch().as_ref(), 2),
        (&prior, &copied, 2),
        (&prior, output.batch().as_ref(), 3),
    ] {
        assert!(
            execution
                .stage(
                    "stats",
                    state,
                    TimedBatch { time, batch },
                    StagePolicy {
                        staging: Staging::Memory,
                        proof: Some(Validation::Statistics(&output))
                    }
                )
                .await
                .is_err()
        );
    }
    let staged = execution
        .stage(
            "stats",
            &prior,
            TimedBatch { time: 2, batch: output.batch() },
            StagePolicy {
                staging: Staging::Objects(None),
                proof: Some(Validation::Statistics(&output)),
            },
        )
        .await?
        .context("missing stage")?;
    assert_eq!(
        staged.1.trace.materialize().await?,
        Batch::from_updates(
            (0..13)
                .filter(|key| *key != 1 && ![10, 11].contains(key))
                .map(|key| ((key, value(key, if key == 2 { 2 } else { 1 })), 1))
        )?
    );
    assert_eq!(prior.trace("stats")?.time(), 1);
    Ok(())
}

#[tokio::test]
async fn statistics_handoff_rejects_malformed_prior_and_failed_upload_preserves_boundary()
-> Result<()> {
    let mut store = crate::engine::tests::store::Store::unpaced();
    store.fail_at = Some(0);
    let execution = execution(Arc::new(store))?;
    let input: Sealed<i64, String> = Batch::from_updates([((1, "1".into()), 1)])?.into();
    let operator = operator();
    for updates in [vec![((1, value(1, 1)), 2)], vec![((1, value(1, 1)), 1), ((1, value(1, 2)), 1)]]
    {
        let prior = execution.writers["stats"]
            .empty()
            .append_validated(Run::Memory(Arc::new(Batch::from_updates(updates)?)), 1)?;
        assert!(operator.evaluate_sealed(&input, &prior, Limits::default()).await.is_err());
        assert_eq!(prior.time(), 1);
    }
    let trace = execution.writers["stats"].empty().append_validated(
        Run::Memory(Arc::new(Batch::from_updates([((1, value(1, 1)), 1)])?)),
        1,
    )?;
    let prior = RelationalState {
        entries: std::iter::once(("stats".into(), Entry { schema: "wide".into(), trace }))
            .collect(),
    };
    let output = operator.evaluate_sealed(&input, prior.trace("stats")?, Limits::default()).await?;
    assert!(
        execution
            .stage(
                "stats",
                &prior,
                TimedBatch { time: 2, batch: output.batch() },
                StagePolicy {
                    staging: Staging::Objects(None),
                    proof: Some(Validation::Statistics(&output))
                }
            )
            .await
            .is_err()
    );
    assert_eq!(prior.trace("stats")?.time(), 1);
    assert_eq!(
        prior.trace("stats")?.materialize().await?,
        Batch::from_updates([((1, value(1, 1)), 1)])?
    );
    Ok(())
}
