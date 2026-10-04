//! Verified source registration and slot-consistent bootstrap primitives.
mod contract;
mod copy;
pub use copy::copy;
mod control;
pub use contract::{Column, Contract, Relation};
pub use control::{Export, Identity};

#[cfg(test)]
mod tests;
