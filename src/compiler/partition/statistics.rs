//! Shared names and positions of the durable additive statistics codec.
pub fn field(index: usize) -> String {
    format!("@statistic_{index}")
}
pub const fn count(index: usize) -> usize {
    1 + index * 2
}
pub const fn sum(index: usize) -> usize {
    2 + index * 2
}
pub const fn width(aggregates: usize) -> usize {
    1 + aggregates * 2
}
