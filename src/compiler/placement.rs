//! Report-only placement inventory. Operator-level liveness is deliberately conservative.
mod graph;
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
    Stage { id: String },
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
}

pub(super) fn analyze(compiled: &Compiled) -> Result<Report> {
    let stages = graph::stages(&compiled.relational.nodes)?;
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
                    dependencies: vec![Dependency::Stage { id: input.clone() }],
                    consumers: stage.maintenance_consumers.clone(),
                    definition: serde_json::to_value(&computed.expression)?,
                    obligations: if required {
                        vec!["operator_level_dependency_is_conservative"]
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
        revision: 1,
        analysis_only: true,
        dependency_precision: "conservative_operator_edges_and_exact_terminal_expression_edges",
        context: "registered_source_and_terminal_semantic_environment",
        network_fallback_stages: Vec::new(),
        stages,
        expressions,
    })
}

#[cfg(test)]
mod tests;
