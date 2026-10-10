//! Validated acyclic plan registration and stable identities for typed execution.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// Supported acyclic operator families; SQL lowering is a later concern.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Kind {
    /// One registered source.
    Source,
    /// Pure projection/filter of one edge.
    Project,
    /// Pure bounded relational expansion of one weighted edge.
    Expand,
    /// Inner equijoin of two edges.
    Join,
    /// Grouped COUNT/SUM statistics from one edge.
    Aggregate,
    /// Exact linear sufficient statistics retained per group.
    Statistics,
}
/// Source relation and exact supported column/type contract identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    /// Stable source identifier in this plan.
    pub id: String,
    /// Exact source schema identity; changing it requires a new query identity.
    pub schema: String,
}
/// Topologically ordered typed operator declaration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    /// Stable node identifier.
    pub id: String,
    /// Operator family.
    pub kind: Kind,
    /// Input node identifiers, or source identifier for Source.
    pub inputs: Vec<String>,
    /// Exact output type/ordering/codec identity.
    pub schema: String,
}
/// Named persisted state owned by one operator.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Arrangement {
    /// Stable arrangement identifier.
    pub id: String,
    /// Owning node.
    pub node: String,
    /// Exact state type/ordering/codec identity.
    pub schema: String,
}
/// Complete declared query contract; callbacks must implement these semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    /// User-provided semantic revision (includes callback implementation changes).
    pub revision: String,
    /// Registered source contracts.
    pub sources: Vec<Source>,
    /// Topologically ordered operators.
    pub nodes: Vec<Node>,
    /// Complete persisted state set.
    pub arrangements: Vec<Arrangement>,
    /// Visible output node identifiers.
    pub outputs: Vec<String>,
}
/// Immutable validated declaration and content-derived identity.
#[derive(Debug, Clone)]
pub struct Plan {
    definition: Definition,
    identity: String,
}
impl Plan {
    /// Validate graph/source/state registrations and bind their stable identity.
    ///
    /// # Errors
    /// Rejects unknown/duplicate/empty registrations, cycles/order, or wrong arity.
    pub fn new(definition: Definition) -> Result<Self> {
        ensure!(
            !definition.revision.is_empty() && !definition.sources.is_empty(),
            "empty plan revision/sources"
        );
        let mut sources = BTreeMap::new();
        for source in &definition.sources {
            ensure!(
                !source.id.is_empty()
                    && !source.schema.is_empty()
                    && sources.insert(source.id.clone(), source.schema.clone()).is_none(),
                "invalid source registration"
            );
        }
        let nodes = validate_nodes(&definition.nodes, &sources)?;
        let mut arrangements = BTreeSet::new();
        for arrangement in &definition.arrangements {
            ensure!(
                !arrangement.id.is_empty()
                    && !arrangement.schema.is_empty()
                    && nodes.contains(&arrangement.node)
                    && arrangements.insert(arrangement.id.clone()),
                "invalid arrangement registration"
            );
        }
        ensure!(
            !arrangements.is_empty() && !definition.outputs.is_empty(),
            "missing state or outputs"
        );
        let mut outputs = BTreeSet::new();
        for output in &definition.outputs {
            ensure!(
                nodes.contains(output) && outputs.insert(output),
                "invalid output registration"
            );
        }
        let mut pending = definition.outputs.clone();
        let mut reachable = BTreeSet::new();
        while let Some(id) = pending.pop() {
            if !reachable.insert(id.clone()) {
                continue;
            }
            let node = definition
                .nodes
                .iter()
                .find(|node| node.id == id)
                .context("unknown output node")?;
            if node.kind != Kind::Source {
                pending.extend(node.inputs.clone());
            }
        }
        ensure!(reachable.len() == definition.nodes.len(), "unused operator registration");
        let digest = Sha256::digest(serde_json::to_vec(&definition)?);
        let identity = format!("{digest:x}");
        Ok(Self { definition, identity })
    }
    /// Stable identity, including graph structure, schema contracts and revision.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }
    /// Immutable registered definition.
    #[must_use]
    pub const fn definition(&self) -> &Definition {
        &self.definition
    }
}
fn validate_nodes(nodes: &[Node], sources: &BTreeMap<String, String>) -> Result<BTreeSet<String>> {
    let mut visited = BTreeSet::new();
    let mut used_sources = BTreeSet::new();
    for node in nodes {
        ensure!(
            !node.id.is_empty() && !node.schema.is_empty() && !visited.contains(&node.id),
            "invalid node registration"
        );
        let arity = if node.kind == Kind::Join { 2 } else { 1 };
        ensure!(node.inputs.len() == arity, "wrong operator arity");
        if node.kind == Kind::Source {
            ensure!(
                sources.get(&node.inputs[0]) == Some(&node.schema)
                    && used_sources.insert(node.inputs[0].clone()),
                "unknown or duplicate source binding"
            );
        } else {
            ensure!(
                node.inputs.iter().all(|input| visited.contains(input)),
                "unknown, cyclic or unordered input"
            );
        }
        visited.insert(node.id.clone());
    }
    ensure!(used_sources == sources.keys().cloned().collect(), "unused source registration");
    Ok(visited)
}

/// Actual immutable arrangement metadata reported by a typed state.
#[derive(Debug, Clone)]
pub struct Binding {
    /// Registered arrangement identifier.
    pub id: String,
    /// Exact state schema/ordering identity.
    pub schema: String,
    /// Snapshot logical time.
    pub time: u64,
}
/// Immutable state must report every registered arrangement from actual snapshots.
pub trait State: Send + Sync + 'static {
    /// Complete snapshot bindings; no unregistered hidden mutable state is allowed.
    fn bindings(&self) -> Vec<Binding>;
}
impl Plan {
    /// Validate actual state membership/schema and its shared transaction boundary.
    ///
    /// # Errors
    /// Rejects missing/extra/duplicate state, schema mismatch or inconsistent ticks.
    pub fn validate_state(&self, state: &impl State, time: u64) -> Result<()> {
        let bindings = state.bindings();
        let mut seen = BTreeSet::new();
        ensure!(bindings.len() == self.definition.arrangements.len(), "state membership mismatch");
        for binding in bindings {
            let expected = self.definition.arrangements.iter().find(|item| item.id == binding.id);
            ensure!(
                expected.is_some_and(|item| item.schema == binding.schema)
                    && binding.time == time
                    && seen.insert(binding.id),
                "invalid state binding/schema/tick"
            );
        }
        Ok(())
    }
}
mod checkpoint;
pub use checkpoint::{Checkpoint, Membership};
pub mod projection;
pub mod query;
pub mod relational;
mod runtime;
pub use runtime::Engine;
#[cfg(test)]
#[path = "tests/contract.rs"]
mod tests;
