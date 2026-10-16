//! Report-only placement inventory. Operator-level liveness is deliberately conservative.
mod fields;
mod finalizers;
mod graph;
mod retention;
mod terminal;
use super::{Compiled, relational::Node};
use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
enum ConsumerKind {
    Predicate,
    Join,
    GroupOrPartition,
    AggregateOrWindow,
    Ordering,
    SetOperation,
    IntermediateComputation,
    Output,
    SinkPredicate,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct Consumer {
    node: String,
    kind: ConsumerKind,
}
#[derive(Serialize)]
struct Stage {
    id: String,
    inputs: Vec<String>,
    maintenance_consumers: Vec<Consumer>,
}
#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum Dependency {
    Expression { id: String },
    Operand { stage: String, column: super::ColumnRef },
}
#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Execution {
    Local,
    SinkDeferred,
}
#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Status {
    MaintenanceRequired,
    CandidateRequiresProof,
    QualifiedExistingTerminal,
    Unreferenced,
}
#[derive(Serialize)]
struct Expression {
    id: String,
    type_oid: u32,
    current_execution: Execution,
    status: Status,
    dependencies: Vec<Dependency>,
    consumers: Vec<Consumer>,
    field_consumers: Vec<Consumer>,
    definition: Value,
    obligations: Vec<&'static str>,
}
#[derive(Serialize)]
pub struct Report {
    revision: u32,
    #[serde(rename = "report_only")]
    analysis_only: bool,
    dependency_precision: &'static str,
    context: &'static str,
    network_fallback_stages: Vec<String>,
    stages: Vec<Stage>,
    expressions: Vec<Expression>,
    finalizers: Vec<finalizers::Finalizer>,
    retention: retention::Retention,
}

pub(super) fn analyze(compiled: &Compiled) -> Result<Report> {
    let stages = graph::stages(&compiled.relational.nodes)?;
    let field_consumers = fields::consumers(compiled, &stages);
    let mut expressions = Vec::new();
    for (node, stage) in compiled.relational.nodes.iter().zip(&stages) {
        if let Node::Map { input, computed, .. } = node {
            for computed in computed {
                let required = !stage.maintenance_consumers.is_empty();
                expressions.push(Expression {
                    id: format!("local:{}:{}", stage.id, computed.column.name),
                    type_oid: computed.column.oid,
                    current_execution: Execution::Local,
                    status: if required {
                        Status::MaintenanceRequired
                    } else {
                        Status::CandidateRequiresProof
                    },
                    dependencies: computed
                        .expression
                        .columns()
                        .into_iter()
                        .map(|column| Dependency::Operand { stage: input.clone(), column })
                        .collect(),
                    field_consumers: field_consumers
                        .get(&format!("local:{}:{}", stage.id, computed.column.name))
                        .map_or_else(Vec::new, |values| values.iter().cloned().collect()),
                    consumers: stage.maintenance_consumers.clone(),
                    definition: serde_json::to_value(&computed.expression)?,
                    obligations: if required {
                        vec![
                            "operator_level_dependency_is_conservative",
                            "full_tuple_state_identity",
                            "evaluation_domain_and_errors",
                        ]
                    } else {
                        vec![
                            "column_liveness",
                            "evaluation_domain_and_errors",
                            "lazy_branches",
                            "cardinality",
                            "contextual_determinism",
                            "retraction_reproducibility",
                            "qualified_target_runtime",
                        ]
                    },
                });
            }
        }
    }
    terminal::append(compiled, &mut expressions)?;
    Ok(Report {
        revision: 4,
        retention: retention::analyze(compiled)?,
        finalizers: finalizers::analyze(&compiled.relational.nodes)?,
        analysis_only: true,
        dependency_precision: "exact_scalar_operands_and_row_preserving_lineage_with_conservative_stateful_barriers",
        context: "registered_source_and_terminal_semantic_environment",
        network_fallback_stages: Vec::new(),
        stages,
        expressions,
    })
}

#[cfg(test)]
mod finalizers_tests;
#[cfg(test)]
mod tests;
