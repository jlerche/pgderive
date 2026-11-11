//! Common result context for Boolean, UUID and all-unknown CASE and COALESCE.
use super::{Typed, Value};
use anyhow::Result;

pub(super) fn candidate(types: impl Iterator<Item = u32>) -> Option<u32> {
    let mut known = types.filter(|oid| !matches!(oid, 0 | 705));
    match known.next() {
        None => Some(25),
        Some(first @ (16 | 2950)) if known.all(|oid| oid == first) => Some(first),
        _ => None,
    }
}

pub(super) fn coerce(value: Typed, candidate: Option<u32>) -> Result<Typed> {
    if let Some(oid) = candidate {
        return value.coerce_common(oid);
    }
    // Other result families retain the ordinary text binding of explicit COLLATE.
    if matches!(value.oid, 0 | 705) && matches!(value.value, Value::Collate { .. }) {
        return value.coerce_common(25);
    }
    Ok(value)
}
