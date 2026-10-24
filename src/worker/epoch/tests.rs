use super::*;
use crate::{
    transaction::{Change, Operation},
    weighted,
};
pub(super) fn transaction(xid: u32, changes: Vec<Change>) -> Result<Transaction> {
    let batch = weighted::Batch::from_changes(&changes)?;
    Ok(Transaction {
        xid,
        commit_lsn: format!("0/{:X}", xid * 16),
        end_lsn: format!("0/{:X}", xid * 16 + 8),
        changes,
        batch,
        bytes: 16,
        commit_time_micros: None,
        received_at: None,
    })
}
pub(super) fn insert(table: &str, values: &[(&str, Option<&str>)]) -> Change {
    Change {
        schema: "source".into(),
        table: table.into(),
        operation: Operation::Insert,
        old: None,
        new: Some(crate::worker::tests::row(values)),
    }
}
#[test]
fn epochs_preserve_source_order_and_resource_accounting() -> Result<()> {
    let first = transaction(1, vec![insert("auction", &[("id", Some("1"))])])?;
    let mut epoch = Epoch::new(first, 3)?;
    epoch.push(transaction(2, vec![])?, 4, Limits::default())?;
    assert_eq!(epoch.transactions(), 2);
    assert_eq!(epoch.bytes(), 32);
    assert_eq!(epoch.decode_us(), 7);
    assert_eq!(epoch.progress()?.xid, 2);
    assert!(epoch.push(transaction(2, vec![])?, 0, Limits::default()).is_err());
    assert!(epoch.push(transaction(1, vec![])?, 0, Limits::default()).is_err());
    let limits = Limits { output_bytes: 32, ..Limits::default() };
    assert!(epoch.push(transaction(3, vec![])?, 0, limits).is_err());
    let limits = Limits { contributions: 0, ..Limits::default() };
    assert!(epoch.push(transaction(3, vec![])?, 0, limits).is_err());
    assert_eq!(epoch.transactions(), 2);
    Ok(())
}
#[tokio::test]
async fn epoch_joins_include_cross_terms_and_preserve_nulls() -> Result<()> {
    let contract = crate::worker::tests::contract();
    let compiled = crate::compiler::compile(
        "SELECT a.category, COUNT(*) AS positions, SUM(b.price) AS units FROM source.auction a JOIN source.bid b ON a.id=b.auction GROUP BY a.category",
        &contract,
    )?;
    let mut query = crate::worker::program::relational::build(
        &contract,
        &compiled,
        &crate::worker::tests::settings(),
    )?;
    let left = insert("auction", &[("id", Some("1")), ("category", None), ("other", None)]);
    let right = insert("bid", &[("id", Some("1")), ("auction", Some("1")), ("price", Some("7"))]);
    let mut epoch = Epoch::new(transaction(1, vec![left])?, 0)?;
    epoch.push(transaction(2, vec![right])?, 0, Limits::default())?;
    let input = crate::worker::program::relational::inputs(
        &epoch.transaction().batch,
        compiled.relational(),
        1,
    )?;
    let work = query.prepare_memtable(input, 64 * 1024 * 1024).await?;
    let rows = work.output().batch.iter().collect::<Vec<_>>();
    assert_eq!(rows.len(), 1);
    assert_eq!(*rows[0].1, 1);
    assert_eq!(
        compiled.output().row(&rows[0].0.1)?,
        vec![
            None,
            Some(crate::compiler::Cell::Integer(1)),
            Some(crate::compiler::Cell::Text("7".into()))
        ]
    );
    query.commit(work)?;
    Ok(())
}
#[test]
fn source_clock_age_handles_absence_future_and_invalid_values() {
    assert_eq!(source_age(None, SystemTime::now()), Duration::ZERO);
    assert_eq!(source_age(Some(-1), SystemTime::now()), Duration::ZERO);
    assert_eq!(source_age(Some(i64::MAX), SystemTime::now()), Duration::ZERO);
    assert!(!source_age(Some(1), SystemTime::now()).is_zero());
}
#[test]
fn adaptive_policy_learns_cost_and_avoids_sparse_or_late_waits() -> Result<()> {
    let mut scheduler = Controller::new(Policy::default());
    let mut epoch = Epoch::new(transaction(1, vec![])?, 0)?;
    assert_eq!(scheduler.decision(&epoch).reason, "cold_start");
    scheduler.record(10.0, Duration::from_millis(3));
    scheduler.record(20.0, Duration::from_millis(4));
    assert_eq!(scheduler.decision(&epoch).reason, "unknown_arrival");
    scheduler.observe(Some(100));
    scheduler.observe(Some(300));
    assert!(!scheduler.decision(&epoch).wait.is_zero());
    epoch.source_age = Duration::from_secs(1);
    assert_eq!(scheduler.decision(&epoch).reason, "freshness_pressure");
    epoch.source_age = Duration::ZERO;
    scheduler.observe(Some(1_000_300));
    assert_eq!(scheduler.decision(&epoch).reason, "sparse_input");
    let disabled = Controller::new(Policy { enabled: false, ..Policy::default() });
    assert_eq!(disabled.decision(&epoch).reason, "single_transaction");
    assert!(Policy { freshness_target_ms: 0, ..Policy::default() }.validate().is_err());
    assert!(Policy { max_bytes: 0, ..Policy::default() }.validate().is_err());
    assert!(Policy { max_transactions: 0, ..Policy::default() }.validate().is_err());
    Ok(())
}
#[test]
fn bounded_model_tracks_fixed_cost_and_residuals() {
    let mut model = model::Model::default();
    assert!(model.estimate().is_none());
    for n in 1_u32..=40 {
        model.record(f64::from(n), Duration::from_micros(u64::from(1000 + n * 100)));
    }
    let estimate = model.estimate().into_iter().collect::<Vec<_>>();
    assert_eq!(estimate.len(), 1);
    assert!((estimate[0].fixed - 0.001).abs() < 1e-6);
    assert!((estimate[0].seconds(10.0) - 0.002).abs() < 1e-6);
}

fn durable() -> Result<crate::catalog::Durable> {
    Ok(crate::catalog::Durable {
        stored: crate::catalog::Stored {
            epoch: 1,
            checkpoint: crate::engine::plan::Checkpoint {
                version: 1,
                plan_identity: "source-count".into(),
                time: 1,
                arrangements: vec![],
            },
        },
        binding: crate::catalog::Binding {
            source: crate::source::tests::contract(),
            sink: crate::harness::tuple_sink::sink("sink"),
        },
        fence: 1,
        commit: "0/20".parse()?,
        end: "0/28".parse()?,
        checkpoint_end: "0/10".parse()?,
        published_time: 2,
        xid: Some(2),
    })
}
#[test]
fn source_count_resolves_uncertain_epoch_commits_once() -> Result<()> {
    let progress = Progress::new(2, "0/20", "0/28")?;
    let mut count = Count::default();
    assert!(count.complete().is_err());
    assert!(count.attempt(progress.clone(), 0).is_err());
    count.attempt(progress.clone(), 7)?;
    assert!(count.attempt(progress.clone(), 1).is_err());
    count.resolve(&durable()?)?;
    assert_eq!(count.total(), 7);
    count.resolve(&durable()?)?;
    assert_eq!(count.total(), 7);
    count.attempt(Progress::new(3, "0/30", "0/38")?, 4)?;
    count.resolve(&durable()?)?;
    assert_eq!(count.total(), 7);
    count.attempt(progress, 2)?;
    let mut conflict = durable()?;
    conflict.xid = Some(99);
    assert!(count.resolve(&conflict).is_err());
    assert_eq!(count.total(), 7);
    count.resolve(&durable()?)?;
    assert_eq!(count.total(), 9);
    Ok(())
}
#[test]
fn controller_bounds_exploration_and_uses_freshness_slack() -> Result<()> {
    let mut epoch = Epoch::new(transaction(1, vec![])?, 0)?;
    let mut controller = Controller::new(Policy::default());
    controller.record(1.0, Duration::from_millis(3));
    controller.observe(Some(100));
    controller.observe(Some(200));
    epoch.transaction.bytes = 512;
    assert_eq!(controller.decision(&epoch).reason, "bounded_exploration");
    epoch.transaction.bytes = 16;
    let mut controller = Controller::new(Policy::default());
    controller.record(1.0, Duration::from_millis(49));
    controller.observe(Some(100));
    controller.observe(Some(1000));
    assert_eq!(controller.decision(&epoch).reason, "freshness_budget");
    let mut controller = Controller::new(Policy::default());
    controller.record(1.0, Duration::from_millis(11));
    controller.record(10.0, Duration::from_millis(101));
    controller.observe(Some(100));
    controller.observe(Some(200));
    assert_eq!(controller.decision(&epoch).reason, "amortized");
    Ok(())
}
#[tokio::test]
async fn epoch_union_keeps_full_tuple_identity_and_cancels_signed_rows() -> Result<()> {
    let contract = crate::worker::tests::contract();
    let compiled =
        crate::compiler::compile("SELECT id, auction, price FROM source.bid", &contract)?;
    let first = insert("bid", &[("id", Some("1")), ("auction", Some("1")), ("price", None)]);
    let second = insert("bid", &[("id", Some("1")), ("auction", Some("1")), ("price", Some("9"))]);
    let mut epoch = Epoch::new(transaction(1, vec![first.clone(), second])?, 0)?;
    let delete = Change { operation: Operation::Delete, old: first.new, new: None, ..first };
    epoch.push(transaction(2, vec![delete])?, 0, Limits::default())?;
    let input = crate::worker::program::relational::inputs(
        &epoch.transaction().batch,
        compiled.relational(),
        1,
    )?;
    let batch = input.batch[0].materialize(Limits::default()).await?;
    assert_eq!(batch.iter().count(), 1);
    assert_eq!(batch.iter().next().map(|(_, weight)| *weight), Some(1));
    Ok(())
}

#[test]
fn overdue_over_budget_epochs_still_obey_learned_exploration_ceiling() -> Result<()> {
    let mut epoch = Epoch::new(transaction(1, vec![])?, 0)?;
    let mut controller = Controller::new(Policy { freshness_target_ms: 1, ..Policy::default() });
    controller.record(1.0, Duration::from_millis(50));
    // The ceiling applies even without an arrival estimate and when source
    // age already exceeds the objective. Such pressure forbids waiting, but
    // must not remove the learned bound on ready work.
    epoch.source_age = Duration::from_secs(1);
    epoch.transaction.bytes = 512;
    let bounded = controller.decision(&epoch);
    assert_eq!(bounded.reason, "bounded_exploration");
    assert!(bounded.wait.is_zero());
    assert!(bounded.predicted >= Duration::from_millis(50));
    controller.observe(Some(100));
    controller.observe(Some(200));
    assert_eq!(controller.decision(&epoch).reason, "bounded_exploration");
    epoch.transaction.bytes = 16;
    let overdue = controller.decision(&epoch);
    assert_eq!(overdue.reason, "freshness_pressure");
    assert!(overdue.wait.is_zero());
    epoch.source_age = Duration::ZERO;
    let over_budget = controller.decision(&epoch);
    assert_eq!(over_budget.reason, "freshness_pressure");
    assert!(over_budget.wait.is_zero());
    Ok(())
}

#[test]
fn epoch_receipt_age_keeps_first_complete_transaction_clock() -> Result<()> {
    let mut first = transaction(1, vec![])?;
    let received = Instant::now()
        .checked_sub(Duration::from_secs(2))
        .ok_or_else(|| anyhow::anyhow!("receipt clock underflow"))?;
    first.received_at = Some(received);
    let mut epoch = Epoch::new(first, 0)?;
    let mut next = transaction(2, vec![])?;
    next.received_at = Some(Instant::now());
    epoch.push(next, 0, Limits::default())?;
    assert_eq!(epoch.transaction().received_at, Some(received));
    assert!(epoch.received_age() >= Duration::from_secs(2));
    Ok(())
}
