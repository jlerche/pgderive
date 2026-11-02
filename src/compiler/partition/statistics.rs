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

const KEY_SCALE: &str = "@key_scale_";
pub fn key_scale(aggregate: &super::Aggregate) -> bool {
    aggregate.field.starts_with(KEY_SCALE)
}
pub fn fits(spec: &super::Partition) -> bool {
    let super::Mode::Grouped { keys } = &spec.mode else { return false };
    let scales = keys.iter().filter(|key| key.oid == 1700).count();
    width(spec.aggregates.len() + scales) <= crate::engine::dataflow::MAX_STATISTICS_WIDTH
}
pub fn with_key_scales(mut spec: super::Partition) -> super::Partition {
    let super::Mode::Grouped { keys } = &spec.mode else { return spec };
    let numeric = keys.iter().filter(|key| key.oid == 1700).cloned().collect::<Vec<_>>();
    for (index, key) in numeric.into_iter().enumerate() {
        spec.aggregates.push(super::Aggregate {
            function: super::Function::Sum,
            argument: Some(key),
            filter: None,
            expression: None,
            field: format!("{KEY_SCALE}{index}"),
            navigation: None,
        });
    }
    spec
}

/// Aggregate families with reversible exact sufficient statistics.
pub const fn supports(aggregate: &super::Aggregate) -> bool {
    matches!(
        aggregate.function,
        super::Function::Count
            | super::Function::CountDistinct
            | super::Function::Sum
            | super::Function::Average
    )
}
