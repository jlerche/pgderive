//! Task-wide cooperative fuel for immutable maintenance and streaming output.
//! A byte charge supplements operation charges so wide tuples/fences cannot
//! consume the same allowance as tiny ones. Codec/hash operations remain bounded
//! indivisible units; this is a work bound, not a wall-clock deadline.
const BYTE_UNIT: usize = 4096;

pub async fn spend(bytes: usize) {
    for _ in 0..=bytes.div_ceil(BYTE_UNIT) {
        tokio::task::consume_budget().await;
    }
}
