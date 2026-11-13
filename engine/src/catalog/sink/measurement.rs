//! Publication feedback, independent of optional diagnostic emission.
#[derive(Debug, serde::Serialize)]
pub struct Feedback {
    pub timings: std::collections::BTreeMap<&'static str, u128>,
    pub mutations: super::work::Mutations,
    pub fixed: std::time::Duration,
    pub dml: std::time::Duration,
}
