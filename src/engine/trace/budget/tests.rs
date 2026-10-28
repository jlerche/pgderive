use super::*;
const fn candidate(cohort: usize, bytes: u64) -> Candidate {
    Candidate { cohort, cost: Cost { rows: 100, bytes, runs: 1 }, memory: false }
}
#[test]
fn encoded_width_excludes_dominant_roots_with_equal_row_counts() -> Result<()> {
    let mb = 1024 * 1024;
    let narrow = (0..8).map(|c| Some(candidate(c, mb))).collect::<Vec<_>>();
    assert_eq!(choose(&narrow, false)?, [true; 8]);
    let wide =
        (0..8).map(|c| Some(candidate(c, if c < 4 { 24 * mb } else { mb }))).collect::<Vec<_>>();
    let selected = choose(&wide, false)?;
    assert_eq!(selected, [false, false, false, false, true, true, true, true]);
    assert!(total(&wide, &selected)?.fits());
    Ok(())
}
#[test]
fn physical_roots_not_cohorts_define_fan_in_and_old_history_gets_admission() -> Result<()> {
    let roots = (0..40).map(|n| Some(candidate(n / 10, 1))).collect::<Vec<_>>();
    let flags = choose(&roots, false)?;
    assert_eq!(total(&roots, &flags)?.runs, FAN_IN);
    assert_eq!(participants(&roots, &flags), 4);
    assert!(flags[0] && flags[10] && flags[20] && flags[30]);
    assert!(!flags[39]);
    Ok(())
}
#[test]
fn indivisible_work_and_root_pressure_make_explicit_progress() -> Result<()> {
    let roots = (0..4).map(|c| Some(candidate(c, BYTE_BUDGET))).collect::<Vec<_>>();
    assert_eq!(choose(&roots, false)?, [true; 4]);
    let roots = (0..40).map(|n| Some(candidate(n / 10, 1))).collect::<Vec<_>>();
    assert_eq!(choose(&roots, true)?, [true; 40]);
    assert!(
        Cost { rows: usize::MAX, bytes: u64::MAX, runs: usize::MAX }
            .add(Cost { rows: 1, bytes: 1, runs: 1 })
            .is_err()
    );
    assert!(
        Cost { rows: 0, bytes: u64::MAX, runs: 0 }
            .add(Cost { rows: 0, bytes: 1, runs: 0 })
            .is_err()
    );
    Ok(())
}
#[test]
fn selected_memory_is_whole_and_kept_fused_with_bounded_peers() -> Result<()> {
    let mut roots = (0..32).map(|c| Some(candidate(c, 1))).collect::<Vec<_>>();
    roots.push(Some(Candidate {
        cohort: 32,
        cost: Cost { rows: 100, bytes: 1, runs: 1 },
        memory: true,
    }));
    let flags = choose(&roots, false)?;
    assert!(flags[32]);
    assert_eq!(total(&roots, &flags)?.runs, FAN_IN);
    Ok(())
}

#[test]
fn malformed_physical_membership_is_rejected() -> Result<()> {
    let runs = [Run::Memory(std::sync::Arc::new(crate::engine::Batch::from_updates([(
        (1_i64, 2_i64),
        1,
    )])?))];
    let mut parts = vec![Parts { retained: vec![], merging: Some(runs[0].clone()) }];
    assert!(restrict(&runs, &[1], &mut [], &mut parts).is_err());
    assert!(restrict(&runs, &[2], &mut [true], &mut parts).is_err());
    assert!(restrict(&runs, &[], &mut [true], &mut parts).is_err());
    assert!(
        Cost { rows: 0, bytes: 0, runs: usize::MAX }
            .add(Cost { rows: 0, bytes: 0, runs: 1 })
            .is_err()
    );
    Ok(())
}
