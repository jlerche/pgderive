//! Forward lower bounds without revisiting prefixes between sorted probes.
pub fn advance<T>(rows: &[T], start: usize, less: impl Fn(&T) -> bool) -> usize {
    let mut lower = start;
    let mut step = 1_usize;
    while lower < rows.len() && less(&rows[lower]) {
        let upper = lower.saturating_add(step).min(rows.len());
        if upper == rows.len() || !less(&rows[upper]) {
            return lower + rows[lower..upper].partition_point(less);
        }
        lower = upper;
        step = step.saturating_mul(2);
    }
    lower
}

#[cfg(test)]
#[path = "tests/search.rs"]
mod tests;
