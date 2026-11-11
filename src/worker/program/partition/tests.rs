//! Exact scan reference remains the fallback for ineligible aggregate sets.
use super::{Bag, Frames, Mode, Partition, evaluate, framed};
use crate::{
    compiler::{compile, relational::Node},
    engine::{dataflow::PartitionWork, execution::Limits},
    worker::tests::{contract, row},
};
use anyhow::{Context, Result};
use std::time::Instant;
const BUDGET: usize = 100_000_000;
fn spec(frame: &str, peer: bool) -> Result<Partition> {
    let order = if peer { "price" } else { "id" };
    let window = format!("OVER(ORDER BY {order} {frame})");
    bound(&format!(
        "SELECT id,COUNT(*) {window},SUM(price) {window},AVG(price) {window} FROM source.bid"
    ))
}
fn bound(sql: &str) -> Result<Partition> {
    let compiled = compile(sql, &contract())?;
    compiled
        .relational
        .nodes
        .into_iter()
        .find_map(|node| if let Node::Partition { spec, .. } = node { Some(spec) } else { None })
        .context("missing window partition")
}
fn extrema_spec(frame: &str, peer: bool) -> Result<Partition> {
    let order = if peer { "price" } else { "id" };
    let window = format!("OVER(ORDER BY {order} {frame})");
    let aggregates = ["MIN(price)", "MAX(price)", "SUM(price)", "COUNT(price)"]
        .map(|value| format!("{value} FILTER(WHERE price>=0) {window}"));
    bound(&format!("SELECT id,{} FROM source.bid", aggregates.join(",")))
}

fn input(length: usize, weighted: bool) -> Result<Bag> {
    let mut updates = Vec::new();
    for id in 0..length {
        let price =
            if id % 7 == 0 { None } else { Some((i64::try_from(id % 19)? - 9).to_string()) };
        let weight = if weighted { i64::try_from(id % 3 + 1)? } else { 1 };
        updates.push((
            (
                (),
                row(&[
                    ("0:id", Some(&id.to_string())),
                    ("0:auction", None),
                    ("0:price", price.as_deref()),
                ]),
            ),
            weight,
        ));
    }
    Bag::from_updates(updates)
}
fn run(spec: &Partition, bag: &Bag, sliding: bool) -> Result<(Bag, usize)> {
    let mut work = PartitionWork::new(BUDGET);
    let limits = Limits { contributions: BUDGET, ..Limits::default() };
    let frames = if sliding { Frames::new(spec) } else { Frames::scan() };
    let result = match &spec.mode {
        Mode::Rows { order, frame } | Mode::Peers { order, frame, .. } => {
            framed(spec, bag, (order, frame), (limits, &mut work), frames)?
        }
        _ => anyhow::bail!("expected framed mode"),
    };
    Ok((result, BUDGET - work.remaining_for_test()))
}
#[test]
fn sliding_matches_scan_for_offset_empty_and_peer_frames() -> Result<()> {
    for peer in [false, true] {
        for frame in [
            "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            "BETWEEN 3 PRECEDING AND 2 FOLLOWING",
            "BETWEEN 3 FOLLOWING AND 7 FOLLOWING",
            "BETWEEN 7 PRECEDING AND 3 PRECEDING",
            "BETWEEN 2 FOLLOWING AND 1 FOLLOWING",
            "BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            "BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
        ] {
            let unit = if peer { "GROUPS" } else { "ROWS" };
            let spec = spec(&format!("{unit} {frame}"), peer)?;
            for length in [0, 1, 2, 7, 31] {
                let bag = input(length, peer)?;
                assert_eq!(
                    run(&spec, &bag, true)?.0,
                    run(&spec, &bag, false)?.0,
                    "{unit} {frame}, {length}"
                );
            }
        }
    }
    Ok(())
}
#[test]
fn sliding_large_prefix_has_linear_charged_work_and_identical_output() -> Result<()> {
    let bag = input(400, false)?;
    for peer in [false, true] {
        let unit = if peer { "GROUPS" } else { "ROWS" };
        let spec = spec(&format!("{unit} BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"), peer)?;
        let scan = run(&spec, &bag, false)?;
        let optimized = run(&spec, &bag, true)?;
        assert_eq!(scan.0, optimized.0);
        assert!(optimized.1 * 3 < scan.1, "scan/sliding: {}/{}", scan.1, optimized.1);
        eprintln!("{unit} charged scan/sliding: {}/{}", scan.1, optimized.1);
    }
    Ok(())
}
#[test]
fn insufficient_work_budget_still_rejects() -> Result<()> {
    let spec = spec("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", false)?;
    let mut work = PartitionWork::new(1);
    assert!(evaluate(&spec, &input(10, false)?, (Limits::default(), &mut work)).is_err());
    Ok(())
}
#[test]
#[ignore = "manual uninstrumented release measurement; ordinary gate uses deterministic work assertions"]
fn measure_sliding_release_frames() -> Result<()> {
    let bag = input(1000, false)?;
    for peer in [false, true] {
        let unit = if peer { "GROUPS" } else { "ROWS" };
        let spec = spec(&format!("{unit} BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"), peer)?;
        let mut measurements = Vec::new();
        for optimized in [false, true] {
            let started = Instant::now();
            let (output, work) = run(&spec, &bag, optimized)?;
            measurements.push((output, work, started.elapsed().as_micros()));
        }
        assert_eq!(measurements[0].0, measurements[1].0);
        eprintln!(
            "WINDOW_MEASUREMENT {{\"unit\":\"{unit}\",\"rows\":1000,\"scan_work\":{},\"sliding_work\":{},\"scan_us\":{},\"sliding_us\":{}}}",
            measurements[0].1, measurements[1].1, measurements[0].2, measurements[1].2
        );
    }
    Ok(())
}

#[test]
fn sliding_exact_bigint_sum_crosses_machine_width() -> Result<()> {
    let spec = spec("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", false)?;
    let bag = Bag::from_updates((0..4).map(|id| {
        (((), row(&[("0:id", Some(&id.to_string())), ("0:price", Some("9223372036854775807"))])), 1)
    }))?;
    assert_eq!(run(&spec, &bag, true)?.0, run(&spec, &bag, false)?.0);
    let output = run(&spec, &bag, true)?.0;
    assert!(output.iter().any(|(((), row), _)| row.get("@aggregate_1").and_then(Option::as_deref)
        == Some("36893488147419103228")));
    Ok(())
}

#[test]
fn transient_sliding_sum_can_cross_i64_between_valid_frames() -> Result<()> {
    let spec = spec("ROWS BETWEEN 2 PRECEDING AND CURRENT ROW", false)?;
    let prices = [
        "-9223372036854775808",
        "9223372036854775807",
        "9223372036854775807",
        "-9223372036854775808",
    ];
    let bag = Bag::from_updates(prices.iter().enumerate().map(|(id, price)| {
        (((), row(&[("0:id", Some(&id.to_string())), ("0:price", Some(price))])), 1)
    }))?;
    assert_eq!(run(&spec, &bag, true)?.0, run(&spec, &bag, false)?.0);
    Ok(())
}
#[test]
fn integral_output_width_checks_are_shared_by_scan_and_sliding() -> Result<()> {
    use crate::compiler::partition::Function;
    use num_bigint::BigInt;
    let spec = spec("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", false)?;
    let oversized = BigInt::from(i64::MAX) + 1;
    // Synthetic sufficient statistics exercise unreachable-at-small-fixture-size
    // overflow boundaries without allocating billions of valid int4 occurrences.
    for function in [Function::Sum, Function::Average] {
        let mut aggregate = spec.aggregates[1].clone();
        aggregate.function = function;
        for oid in [21, 23] {
            aggregate.argument.as_mut().context("missing input")?.oid = oid;
            assert!(integral_result(&aggregate, &BigInt::from(2), &oversized).is_err());
            assert!(integral_result(&aggregate, &BigInt::from(2), &BigInt::from(i64::MAX)).is_ok());
        }
        aggregate.argument.as_mut().context("missing input")?.oid = 20;
        assert!(integral_result(&aggregate, &BigInt::from(2), &oversized).is_ok());
    }
    assert!(integral_result(&spec.aggregates[0], &oversized, &BigInt::from(0)).is_err());
    Ok(())
}

fn integral_result(
    aggregate: &crate::compiler::partition::Aggregate,
    count: &num_bigint::BigInt,
    sum: &num_bigint::BigInt,
) -> Result<Option<String>> {
    crate::worker::program::aggregate::Linear { count: count.clone(), sum: sum.clone() }
        .result(aggregate)
}

#[test]
fn extrema_departures_duplicates_nulls_and_mixed_sets_match_scan() -> Result<()> {
    for peer in [false, true] {
        let unit = if peer { "GROUPS" } else { "ROWS" };
        for bounds in [
            "UNBOUNDED PRECEDING AND CURRENT ROW",
            "3 PRECEDING AND 2 FOLLOWING",
            "2 FOLLOWING AND 1 FOLLOWING",
            "CURRENT ROW AND UNBOUNDED FOLLOWING",
        ] {
            let spec = extrema_spec(&format!("{unit} BETWEEN {bounds}"), peer)?;
            for length in [0, 1, 7, 31, 80] {
                assert_eq!(
                    run(&spec, &input(length, peer)?, true)?.0,
                    run(&spec, &input(length, peer)?, false)?.0
                );
            }
        }
    }
    Ok(())
}
#[test]
fn extrema_large_frames_reduce_charged_recomputation() -> Result<()> {
    for length in [64, 256, 1024] {
        let spec = extrema_spec("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", false)?;
        let bag = input(length, false)?;
        let scan = run(&spec, &bag, false)?;
        let optimized = run(&spec, &bag, true)?;
        assert_eq!(scan.0, optimized.0);
        assert!(optimized.1 < length * 12);
        assert!(optimized.1 * 8 < scan.1, "{length}: {}/{}", scan.1, optimized.1);
        eprintln!("EXTREMA_WORK rows={length} scan={} sliding={}", scan.1, optimized.1);
    }
    Ok(())
}

#[test]
#[ignore = "manual release measurement; the gate asserts deterministic extrema work"]
fn measure_extrema_release_frames() -> Result<()> {
    let bag = input(1000, false)?;
    for peer in [false, true] {
        let unit = if peer { "GROUPS" } else { "ROWS" };
        let spec =
            extrema_spec(&format!("{unit} BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"), peer)?;
        let mut measurements = Vec::new();
        for optimized in [false, true] {
            let started = Instant::now();
            let (output, work) = run(&spec, &bag, optimized)?;
            measurements.push((output, work, started.elapsed().as_micros()));
        }
        assert_eq!(measurements[0].0, measurements[1].0);
        eprintln!(
            "EXTREMA_MEASUREMENT {{\"unit\":\"{unit}\",\"rows\":1000,\"scan_work\":{},\"sliding_work\":{},\"scan_us\":{},\"sliding_us\":{}}}",
            measurements[0].1, measurements[1].1, measurements[0].2, measurements[1].2
        );
    }
    Ok(())
}

#[test]
fn numeric_mixed_extrema_retain_scan_fallback() -> Result<()> {
    let window = "OVER(ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)";
    let spec = bound(&format!(
        "SELECT id,MIN(price) {window},SUM(price::numeric) {window} FROM source.bid"
    ))?;
    let bag = input(31, false)?;
    let scan = run(&spec, &bag, false)?;
    let selected = run(&spec, &bag, true)?;
    assert_eq!(scan, selected);
    Ok(())
}

#[test]
fn extrema_keep_signed_width_boundaries_until_final_duplicate_departs() -> Result<()> {
    let window = "OVER(ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)";
    let spec =
        bound(&format!("SELECT id,MIN(price) {window},MAX(price) {window} FROM source.bid"))?;
    let values =
        [Some(i64::MIN), Some(i64::MAX), Some(i64::MIN), None, Some(0), Some(-1), Some(i64::MAX)];
    let bag = Bag::from_updates(values.into_iter().enumerate().map(|(id, value)| {
        (
            (
                (),
                row(&[
                    ("0:id", Some(&id.to_string())),
                    ("0:price", value.map(|v| v.to_string()).as_deref()),
                ]),
            ),
            1,
        )
    }))?;
    assert_eq!(run(&spec, &bag, true)?.0, run(&spec, &bag, false)?.0);
    Ok(())
}

#[test]
fn frame_value_checks_position_before_accessing_the_frame() -> Result<()> {
    let window = "OVER(ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)";
    let query = |value: &str, position: &str| {
        format!("SELECT nth_value({value},{position}) {window} FROM source.bid")
    };
    let empty = input(0, false)?;
    let one = input(1, false)?;
    for position in ["0", "-1"] {
        let spec = bound(&query("price", position))?;
        assert_eq!(run(&spec, &empty, true)?.0, empty);
        assert!(matches!(
            run(&spec, &one, true),
            Err(error) if error.is::<crate::engine::execution::EvaluationFailed>()
                && error.root_cause().to_string() == "argument of nth_value must be greater than zero"
        ));
    }
    for position in ["NULL", "2147483647"] {
        let spec = bound(&query("1/id", position))?;
        let (result, _) = run(&spec, &one, true)?;
        assert!(result.iter().all(|(((), row), _)| row.get("@aggregate_0") == Some(&None)));
    }
    for sql in [
        query("NULL", "1"),
        query("price", "1::bigint"),
        format!("SELECT first_value(price) FILTER (WHERE true) {window} FROM source.bid"),
        "SELECT last_value(price) FROM source.bid".into(),
    ] {
        assert!(compile(&sql, &contract()).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn frame_values_do_not_scan_unselected_occurrences() -> Result<()> {
    let window = "OVER(ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
    let spec = bound(&format!(
        "SELECT first_value(price) {window}, last_value(price) {window}, nth_value(price,2) {window} FROM source.bid"
    ))?;
    let (result, work) = run(&spec, &input(400, false)?, true)?;
    assert_eq!(result.iter().count(), 400);
    assert!(work < 400 * 10, "{work}");
    Ok(())
}

#[test]
fn frame_value_keeps_collation_for_outer_ordering() -> Result<()> {
    compile(
        r#"SELECT first_value(price::text::varchar(12) COLLATE "C") OVER (ORDER BY id) AS v
           FROM source.bid ORDER BY v"#,
        &contract(),
    )?;
    Ok(())
}

#[test]
fn ntile_uses_first_non_null_bucket_count_and_reports_invalid_counts() -> Result<()> {
    let window = "OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)";
    let query = |count| format!("SELECT ntile({count}) {window} FROM source.bid");
    let spec = bound(&query("CASE WHEN id=0 THEN NULL WHEN id=1 THEN 2 ELSE -1 END"))?;
    let limits = Limits::default();
    let mut work = PartitionWork::new(BUDGET);
    let result = evaluate(&spec, &input(5, false)?, (limits, &mut work))?;
    let expected = [None, Some("1"), Some("1"), Some("1"), Some("2")];
    for (((), row), _) in result.iter() {
        let id = row["0:id"].as_deref().context("missing id")?.parse::<usize>()?;
        assert_eq!(row["@aggregate_0"].as_deref(), expected[id]);
    }
    for invalid in ["0", "-1"] {
        let spec = bound(&query(invalid))?;
        assert!(evaluate(&spec, &input(0, false)?, (limits, &mut work))?.iter().next().is_none());
        assert!(matches!(
            evaluate(&spec, &input(1, false)?, (limits, &mut work)),
            Err(error) if error.is::<crate::engine::execution::EvaluationFailed>()
                && error.root_cause().to_string() == "argument of ntile must be greater than zero"
        ));
    }
    for sql in [
        query("1::bigint"),
        "SELECT ntile() OVER (ORDER BY id) FROM source.bid".into(),
        "SELECT ntile(1) OVER (ORDER BY price) FROM source.bid".into(),
        "SELECT cume_dist(1) OVER () FROM source.bid".into(),
        "SELECT percent_rank() FILTER (WHERE true) OVER () FROM source.bid".into(),
    ] {
        assert!(compile(&sql, &contract()).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn fractional_ranks_count_weighted_null_peers_and_singletons() -> Result<()> {
    let spec = bound(
        "SELECT percent_rank() OVER w, cume_dist() OVER w FROM source.bid
         WINDOW w AS (ORDER BY price NULLS LAST)",
    )?;
    let bag = Bag::from_updates([
        (((), row(&[("0:id", Some("1")), ("0:price", Some("7"))])), 2),
        (((), row(&[("0:id", Some("2")), ("0:price", Some("7"))])), 1),
        (((), row(&[("0:id", Some("3")), ("0:price", None)])), 3),
    ])?;
    let mut work = PartitionWork::new(BUDGET);
    let limits = Limits::default();
    let output = evaluate(&spec, &bag, (limits, &mut work))?;
    assert_eq!(output.iter().count(), 3);
    for (((), row), weight) in output.iter() {
        let (rank, cumulative, weight_expected) = match row["0:id"].as_deref() {
            Some("1") => ("0", "0.5", 2),
            Some("2") => ("0", "0.5", 1),
            _ => ("0.6", "1", 3),
        };
        assert_eq!(row["@aggregate_0"].as_deref(), Some(rank));
        assert_eq!(row["@aggregate_1"].as_deref(), Some(cumulative));
        assert_eq!(*weight, weight_expected);
    }
    let singleton = evaluate(&spec, &input(1, false)?, (limits, &mut work))?;
    let (((), row), _) = singleton.iter().next().context("missing singleton")?;
    assert_eq!(row["@aggregate_0"].as_deref(), Some("0"));
    assert_eq!(row["@aggregate_1"].as_deref(), Some("1"));
    Ok(())
}
