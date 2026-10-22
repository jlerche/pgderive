use crate::engine::search::advance;
use std::cell::Cell;
#[test]
fn forward_bounds_match_reference_and_dense_probes_do_not_restart() {
    for size in [0, 1, 2, 7, 64, 1024] {
        let rows = (0..size).map(|x| x / 3).collect::<Vec<_>>();
        for start in 0..=rows.len() {
            for target in [-1, 0, 1, 3, 19, 100, 1000] {
                assert_eq!(
                    advance(&rows, start, |x| *x < target),
                    start + rows[start..].partition_point(|x| *x < target)
                );
            }
        }
    }
    let rows = (0..1024).collect::<Vec<_>>();
    let calls = Cell::new(0);
    let mut position = 0;
    for target in 0..1024 {
        position = advance(&rows, position, |x| {
            calls.set(calls.get() + 1);
            *x < target
        });
        assert_eq!(position, usize::try_from(target).unwrap_or_default());
    }
    assert!(calls.get() < 4096);
}
