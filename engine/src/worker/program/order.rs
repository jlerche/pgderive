//! Shared integral/timestamp ordering and signed lexicographic tokens.
use anyhow::Result;
pub(super) fn value(text: &str, oid: u32) -> Result<i64> {
    match oid {
        1114 | 1184 => Ok(crate::temporal::Timestamp::parse(text, oid)?.sort_value()),
        _ => text.parse::<i64>().map_err(Into::into),
    }
}
pub(super) const fn biased(value: i64, descending: bool) -> u64 {
    let value = u64::from_be_bytes(value.to_be_bytes()) ^ (1_u64 << 63);
    if descending { !value } else { value }
}
