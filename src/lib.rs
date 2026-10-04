//! `PostgreSQL` incremental view maintenance, built one verified slice at a time.

mod acknowledged;
pub mod catalog;
mod configuration;
pub mod engine;
mod harness;
mod listener;
mod outcome;
mod storage;
mod transaction;
mod weighted;

pub use configuration::Config;
pub use harness::{RecoveryReport, run_harness, run_recovery};
pub use listener::listen;
