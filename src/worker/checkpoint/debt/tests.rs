use super::*;
const LIMITS: Load = Load([1000, 248, 120, 4096]);
fn bytes(bytes: u64) -> Load {
    Load([bytes, 4, 0, 0])
}
#[tokio::test(start_paused = true)]
async fn predicts_exhaustion_before_hard_capacity_and_relearns_service() {
    let mut controller = Controller::default();
    let started = Instant::now();
    controller.completed(Duration::from_millis(100));
    controller.started(started, bytes(100));
    tokio::time::advance(Duration::from_millis(10)).await;
    assert!(controller.pressure(Instant::now(), started.elapsed(), bytes(200), LIMITS));
    assert!(200 < LIMITS.0[0]);
    controller.completed(Duration::from_millis(10));
    assert_eq!(controller.service, Duration::from_micros(87500));
    controller.started(Instant::now(), bytes(10));
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(!controller.pressure(Instant::now(), Duration::from_millis(100), bytes(20), LIMITS));
}
#[tokio::test(start_paused = true)]
async fn overdue_and_first_uploads_retain_forecast_headroom() {
    for trained in [false, true] {
        let mut controller = Controller::default();
        if trained {
            controller.completed(Duration::from_millis(1));
        }
        let start = Instant::now();
        controller.started(start, bytes(100));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(controller.pressure(Instant::now(), start.elapsed(), bytes(600), LIMITS));
    }
}
#[tokio::test(start_paused = true)]
async fn shrinking_memory_does_not_hide_zero_output_journal_or_root_debt() {
    for index in 1..4 {
        let mut controller = Controller::default();
        let start = Instant::now();
        let mut load = Load([900, 0, 0, 0]);
        controller.started(start, load);
        tokio::time::advance(Duration::from_millis(10)).await;
        load.0[0] = 0;
        load.0[index] = LIMITS.0[index] / 2 + 1;
        assert!(controller.pressure(Instant::now(), start.elapsed(), load, LIMITS));
    }
}
#[test]
fn zero_time_and_extreme_values_are_bounded_and_completion_clears_observations() {
    let now = Instant::now();
    let mut controller = Controller::default();
    assert!(!controller.pressure(now, Duration::MAX, bytes(0), LIMITS));
    controller.started(now, bytes(0));
    assert!(controller.pressure(now, Duration::ZERO, bytes(600), LIMITS));
    controller.started(now, bytes(0));
    assert!(controller.pressure(
        now + Duration::from_nanos(1),
        Duration::MAX,
        bytes(u64::MAX),
        Load([u64::MAX; 4])
    ));
    controller.completed(Duration::from_millis(50));
    assert!(controller.captured.is_none());
    controller.started(now, bytes(0));
    assert_eq!(controller.burst, [0; 4]);
}

#[tokio::test(start_paused = true)]
async fn unrelated_wakes_do_not_inflate_arrival_rate() {
    let start = Instant::now();
    let mut woke = Controller::default();
    let mut quiet = Controller::default();
    for controller in [&mut woke, &mut quiet] {
        controller.completed(Duration::from_millis(50));
        controller.started(start, bytes(0));
    }
    tokio::time::advance(Duration::from_millis(9)).await;
    assert!(!woke.pressure(Instant::now(), start.elapsed(), bytes(0), LIMITS));
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(
        woke.pressure(Instant::now(), start.elapsed(), bytes(100), LIMITS),
        quiet.pressure(Instant::now(), start.elapsed(), bytes(100), LIMITS)
    );
    assert!(!woke.pressure(Instant::now(), start.elapsed(), bytes(100), LIMITS));
}

#[tokio::test(start_paused = true)]
async fn debt_admission_stays_bounded_across_ten_thousand_service_cycles() {
    let mut controller = Controller::default();
    let mut paused = 0;
    let mut maximum = 0;
    for cycle in 0..10_000_u64 {
        // Fixed offered work, changing service cost, no wall-clock sleeps or
        // idle-day jumps. Every logical millisecond below executes an admission.
        let service = Duration::from_millis(5 + cycle % 100);
        let start = Instant::now();
        controller.started(start, bytes(100));
        let mut memory = 100;
        while start.elapsed() < service {
            tokio::time::advance(Duration::from_millis(1)).await;
            let load = bytes(memory);
            if controller.pressure(Instant::now(), start.elapsed(), load, LIMITS) {
                paused += 1;
                tokio::time::advance(service.saturating_sub(start.elapsed())).await;
                break;
            }
            memory += 10;
            maximum = maximum.max(memory);
            assert!(memory < LIMITS.0[0]);
        }
        controller.completed(start.elapsed());
    }
    assert!(paused > 0);
    assert!(maximum < 900);
}

#[tokio::test(start_paused = true)]
async fn cyclic_root_allocations_are_not_forecast_as_sustained_growth() {
    let mut controller = Controller::default();
    let start = Instant::now();
    controller.started(start, Load([100, 4, 0, 0]));
    for epoch in 1..=160_u64 {
        tokio::time::advance(Duration::from_millis(5)).await;
        let load = Load([100, 4 + epoch % 8, epoch % 8, epoch]);
        assert!(!controller.pressure(Instant::now(), start.elapsed(), load, LIMITS));
    }
}
