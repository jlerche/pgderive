//! `PostgreSQL` incremental view maintenance, built one verified slice at a time.

mod configuration;
mod harness;
mod listener;
mod transaction;
mod weighted;

pub use configuration::Config;
pub use harness::run_harness;
pub use listener::listen;
