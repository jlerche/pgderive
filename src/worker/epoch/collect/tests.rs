use super::*;
use crate::worker::epoch::tests::{input_work, insert, key_plan, observation, transaction};
use anyhow::Context;
use tokio::sync::mpsc;

struct LogicalClock {
    origin: Instant,
}
impl Clock for LogicalClock {
    fn wall(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_hours(262_968) + self.origin.elapsed()
    }
}
struct Inputs(mpsc::Receiver<Input>);
impl InputStream for Inputs {
    async fn input(&mut self) -> Result<Input> {
        self.0.recv().await.context("complete source ended")
    }
}
fn input(xid: u32, micros: i64) -> Result<Input> {
    let id = xid.to_string();
    let change = insert("bid", &[("id", Some(&id)), ("auction", Some("1")), ("price", Some("7"))]);
    let mut transaction = transaction(xid, vec![change])?;
    transaction.commit_time_micros = Some(micros);
    transaction.received_at = Some(Instant::now());
    Ok(Input { transaction, decode_us: 1 })
}
fn controller(policy: &Policy) -> Controller {
    let mut controller = Controller::new(policy.clone());
    controller.record(input_work(1), observation(10, 2000));
    controller.record(input_work(32), observation(320, 2000));
    controller.observe(Some(0));
    controller.observe(Some(1000));
    controller
}
#[tokio::test(start_paused = true)]
async fn virtual_time_coalesces_paced_commits_and_preserves_order() -> Result<()> {
    let origin = Instant::now();
    let (send, received) = mpsc::channel(64);
    let producer = tokio::spawn(async move {
        for xid in 1..=2000 {
            tokio::time::sleep_until(origin + Duration::from_millis(u64::from(xid))).await;
            send.send(input(xid, i64::from(xid) * 1000)?).await?;
        }
        // Keep the source connected while the last epoch's deadline expires.
        std::future::pending::<Result<()>>().await
    });
    let mut stream = Inputs(received);
    let mut collector = Collector { held: None, clock: LogicalClock { origin }, plan: key_plan()? };
    let policy = Policy::default();
    let mut scheduler = controller(&policy);
    let (_stop, mut stop) = watch::channel(false);
    let mut transactions = 0;
    let mut epochs = 0;
    while transactions < 2000 {
        let first = collector.recv(&mut stream).await?;
        let collected = collector
            .gather(
                first,
                &mut stream,
                &mut scheduler,
                Bounds {
                    limits: Limits::default(),
                    policy: &policy,
                    remaining: 2000 - transactions,
                    source_changes: 100_000,
                    stop: &mut stop,
                },
            )
            .await?;
        assert_eq!(
            u64::try_from(collected.epoch.transaction().changes.len())?,
            collected.epoch.transactions()
        );
        transactions += collected.epoch.transactions();
        epochs += 1;
        assert_eq!(u64::from(collected.epoch.progress()?.xid), transactions);
        assert_eq!(collected.epoch.decode_us(), u128::from(collected.epoch.transactions()));
        assert!(collected.epoch.age() <= Duration::from_millis(50));
        assert_eq!(collected.epoch.received_age(), collected.epoch.age());
        let service = Duration::from_micros(2000 + collected.epoch.transactions() * 10);
        scheduler
            .record(collected.epoch.work(), observation(collected.epoch.transactions() * 10, 2000));
        tokio::time::sleep(service).await;
    }
    producer.abort();
    assert_eq!(transactions, 2000);
    assert!(epochs < transactions / 2, "production collector should coalesce dense input");
    assert!(origin.elapsed() >= Duration::from_secs(2));
    assert!(origin.elapsed() < Duration::from_secs(3));
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn virtual_time_idle_deadline_shutdown_and_held_input() -> Result<()> {
    let origin = Instant::now();
    let mut collector = Collector { held: None, clock: LogicalClock { origin }, plan: key_plan()? };
    let policy = Policy { max_transactions: 2, ..Policy::default() };
    let mut scheduler = controller(&policy);
    let (send, received) = mpsc::channel(8);
    let mut stream = Inputs(received);
    let (stop_send, mut stop) = watch::channel(false);
    let first = input(1, 0)?;
    send.send(input(2, 0)?).await?;
    let collected = collector
        .gather(
            first,
            &mut stream,
            &mut scheduler,
            Bounds {
                limits: Limits { output_bytes: 24, ..Limits::default() },
                policy: &policy,
                remaining: 2,
                source_changes: 100,
                stop: &mut stop,
            },
        )
        .await?;
    assert_eq!(collected.reason, "resource_ceiling");
    assert_eq!(collected.epoch.transactions(), 1);
    let held = collector.recv(&mut stream).await?;
    assert_eq!(held.transaction.xid, 2);
    tokio::time::advance(Duration::from_hours(12)).await;
    stop_send.send(true)?;
    let collected = collector
        .gather(
            held,
            &mut stream,
            &mut scheduler,
            Bounds {
                limits: Limits::default(),
                policy: &policy,
                remaining: 2,
                source_changes: 100,
                stop: &mut stop,
            },
        )
        .await?;
    assert_eq!(collected.reason, "shutdown");
    assert!(collected.epoch.received_age() >= Duration::from_hours(12));
    assert!(collected.epoch.age() >= Duration::from_hours(12));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn virtual_idle_uses_production_predicted_arrival_deadline() -> Result<()> {
    let origin = Instant::now();
    let mut collector = Collector { held: None, clock: LogicalClock { origin }, plan: key_plan()? };
    let policy = Policy::default();
    let mut scheduler = controller(&policy);
    let (_send, received) = mpsc::channel(8);
    let mut stream = Inputs(received);
    let (_stop_send, mut stop) = watch::channel(false);
    let collected = collector
        .gather(
            input(1, 0)?,
            &mut stream,
            &mut scheduler,
            Bounds {
                limits: Limits::default(),
                policy: &policy,
                remaining: 100,
                source_changes: 100,
                stop: &mut stop,
            },
        )
        .await?;
    assert_eq!(collected.reason, "predicted_arrival");
    assert_eq!(collected.epoch.transactions(), 1);
    // Tokio timers have millisecond granularity, even when time is paused.
    assert!(origin.elapsed() >= Duration::from_micros(1250));
    assert!(origin.elapsed() <= Duration::from_micros(2250));
    Ok(())
}
