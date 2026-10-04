use super::Plan;
use crate::engine::trace::Manifest;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// One named coarse arrangement membership at a complete query boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Membership {
    /// Registered arrangement name.
    pub id: String,
    /// Exact key/value codec and ordering identity.
    pub schema: String,
    /// Coarse immutable run references, logical time and physical generation.
    pub trace: Manifest,
}
impl Membership {
    /// Checksum of this coarse membership, including ordered repeated references.
    ///
    /// # Errors
    /// Returns a serialization failure.
    pub fn digest(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
    }
}
/// Metadata checkpoint; source progress and sink publication are separate work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    /// Checkpoint codec version; currently one.
    pub version: u32,
    /// Identity covering the full registered graph and callback semantic revision.
    pub plan_identity: String,
    /// Complete transaction-ordered query boundary.
    pub time: u64,
    /// Every registered arrangement, with no hidden or extra state.
    pub arrangements: Vec<Membership>,
}
impl Checkpoint {
    /// Verify plan identity, exact memberships, schemas, clocks and run codecs.
    ///
    /// # Errors
    /// Rejects incompatible, duplicate, incomplete, or inconsistent checkpoints.
    pub fn validate(&self, plan: &Plan) -> Result<()> {
        ensure!(
            self.version == 1 && self.plan_identity == plan.identity(),
            "checkpoint plan/format mismatch"
        );
        ensure!(
            self.arrangements.len() == plan.definition().arrangements.len(),
            "checkpoint membership count mismatch"
        );
        let mut seen = BTreeSet::new();
        for member in &self.arrangements {
            ensure!(
                plan.definition()
                    .arrangements
                    .iter()
                    .any(|expected| expected.id == member.id && expected.schema == member.schema)
                    && seen.insert(&member.id)
                    && member.trace.time == self.time,
                "checkpoint arrangement schema/clock/membership mismatch"
            );
            member.trace.validate(&member.schema)?;
        }
        Ok(())
    }
}
