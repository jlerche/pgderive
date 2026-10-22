//! Verified source registration and slot-consistent bootstrap primitives.
mod contract;
mod layout;
pub use crate::semantics::Environment;
mod copy;
pub use copy::copy;
mod control;
pub use contract::{Column, Contract, Relation};
pub use control::{Export, Identity};

#[cfg(test)]
pub(crate) mod tests;
