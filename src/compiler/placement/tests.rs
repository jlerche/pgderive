use super::{ConsumerKind, Execution, Status, graph};
use crate::{
    compiler::{compile, relational::Node},
    source::{Column, Contract, Identity, Relation},
};
use anyhow::{Context, Result};

fn contract() -> Contract {
    let column = |position, name: &str, oid| Column {
        position,
        name: name.into(),
        oid,
        modifier: -1,
        nullable: position != 1,
        primary: position == 1,
        collation: if oid == 25 { 100 } else { 0 },
    };
    Contract {
        environment: crate::source::Environment::fixture(),
        identity: Identity { system: "1".into(), timeline: 1, database: "db".into() },
        publication: "pub".into(),
        slot: "slot".into(),
        relations: vec![Relation {
            oid: 1,
            schema: "source".into(),
            table: "bid".into(),
            columns: vec![
                column(1, "id", 23),
                column(2, "price", 20),
                column(3, "date_time", 1114),
                column(4, "label", 25),
            ],
        }],
    }
}

#[test]
fn final_local_value_is_only_a_candidate_and_report_does_not_change_plan() -> Result<()> {
    let compiled = compile(
        "SELECT date_bin(interval '1 minute',date_time,timestamp '2000-01-01 00:00:00') AS bucket FROM source.bid",
        &contract(),
    )?;
    let before = serde_json::to_value(&compiled)?;
    let report = compiled.placement()?;
    let local = report
        .expressions
        .iter()
        .find(|entry| entry.current_execution == Execution::Local)
        .context("local date_bin absent")?;
    assert_eq!(local.type_oid, 1114);
    assert_eq!(local.status, Status::CandidateRequiresProof);
    assert!(local.obligations.contains(&"evaluation_domain_and_errors"));
    assert!(local.obligations.contains(&"lazy_branches"));
    assert!(local.obligations.contains(&"retraction_reproducibility"));
    assert!(report.analysis_only);
    assert!(report.network_fallback_stages.is_empty());
    assert_eq!(before, serde_json::to_value(&compiled)?);
    assert_eq!(serde_json::to_value(report)?, serde_json::to_value(compiled.placement()?)?);
    Ok(())
}

#[test]
fn grouping_and_intermediate_consumers_keep_computed_values_local() -> Result<()> {
    let compiled = compile(
        "SELECT date_bin(interval '1 minute',date_time,timestamp '2000-01-01 00:00:00') AS bucket,COUNT(*) FROM source.bid GROUP BY date_bin(interval '1 minute',date_time,timestamp '2000-01-01 00:00:00')",
        &contract(),
    )?;
    let report = compiled.placement()?;
    let local = report
        .expressions
        .iter()
        .find(|entry| entry.current_execution == Execution::Local)
        .context("grouping expression absent")?;
    assert_eq!(local.status, Status::MaintenanceRequired);
    assert!(local.consumers.iter().any(|consumer| consumer.kind == ConsumerKind::GroupOrPartition));
    let nodes = vec![
        Node::Source { id: "source".into(), source: 0 },
        Node::Map { id: "first".into(), input: "source".into(), computed: vec![] },
        Node::Rebind { id: "scope".into(), input: "first".into(), fields: vec![] },
        Node::Map { id: "second".into(), input: "scope".into(), computed: vec![] },
        Node::Output { id: "output".into(), input: "second".into() },
    ];
    let stages = graph::stages(&nodes)?;
    assert!(
        stages[1]
            .maintenance_consumers
            .iter()
            .any(|consumer| consumer.kind == ConsumerKind::IntermediateComputation
                && consumer.node == "second")
    );
    assert!(stages[3].maintenance_consumers.is_empty());
    Ok(())
}

#[test]
fn shared_terminal_expressions_have_typed_edges_and_all_output_consumers() -> Result<()> {
    let compiled =
        compile("SELECT length(label)+1 AS a,length(label)+2 AS b FROM source.bid", &contract())?;
    let report = compiled.placement()?;
    let call = report
        .expressions
        .iter()
        .find(|entry| entry.definition["Call"]["function"] == "Length")
        .context("terminal call absent")?;
    assert_eq!(call.current_execution, Execution::SinkDeferred);
    assert_eq!(call.type_oid, 23);
    assert_eq!(call.status, Status::QualifiedExistingTerminal);
    assert_eq!(
        call.consumers.iter().map(|consumer| consumer.node.as_str()).collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert!(!call.dependencies.is_empty());
    Ok(())
}

#[test]
fn lazy_terminal_branches_are_traversed_without_evaluating_them() -> Result<()> {
    let compiled = compile(
        "SELECT CASE WHEN length(label)>0 THEN length(label) ELSE coalesce(length(label),0) END AS value FROM source.bid",
        &contract(),
    )?;
    let report = compiled.placement()?;
    let case = report
        .expressions
        .iter()
        .find(|entry| entry.definition.get("Case").is_some())
        .context("CASE absent")?;
    assert_eq!(case.current_execution, Execution::SinkDeferred);
    assert_eq!(case.status, Status::QualifiedExistingTerminal);
    assert!(case.dependencies.len() >= 3);
    assert!(report.expressions.iter().any(|entry| entry.definition.get("Coalesce").is_some()));
    Ok(())
}

#[test]
fn graph_closure_includes_both_join_inputs_and_rejects_malformed_edges() -> Result<()> {
    let source = |id: &str| Node::Source { id: id.into(), source: 0 };
    let nodes = vec![
        source("left"),
        source("right"),
        Node::Join { id: "joined".into(), left: "left".into(), right: "right".into() },
        Node::Output { id: "output".into(), input: "joined".into() },
    ];
    let stages = graph::stages(&nodes)?;
    for stage in &stages[..2] {
        assert!(
            stage
                .maintenance_consumers
                .iter()
                .any(|consumer| consumer.node == "joined" && consumer.kind == ConsumerKind::Join)
        );
    }
    assert!(graph::stages(&[source("a"), source("a")]).is_err());
    assert!(graph::stages(&[Node::Output { id: "a".into(), input: "a".into() }]).is_err());
    assert!(graph::stages(&[Node::Output { id: "a".into(), input: "missing".into() }]).is_err());
    Ok(())
}

#[test]
fn lower_is_reported_only_in_the_existing_qualified_terminal_domain() -> Result<()> {
    let compiled = compile(
        "SELECT lower(label COLLATE pg_catalog.\"C\") AS value FROM source.bid",
        &contract(),
    )?;
    let report = compiled.placement()?;
    assert!(report.expressions.iter().any(|entry| entry.definition["Call"]["function"] == "Lower"
        && entry.status == Status::QualifiedExistingTerminal));
    assert!(compile("SELECT lower(label COLLATE pg_catalog.\"C\"),COUNT(*) FROM source.bid GROUP BY lower(label COLLATE pg_catalog.\"C\")",&contract()).is_err());
    Ok(())
}

#[test]
fn field_lineage_distinguishes_unused_siblings_and_follows_rebinding() -> Result<()> {
    use crate::compiler::{ColumnRef, relational::Binding};
    let mut compiled = compile(
        "SELECT date_bin(interval '1 minute',date_time,timestamp '2000-01-01 00:00:00') AS bucket FROM source.bid",
        &contract(),
    )?;
    let value = compiled
        .relational
        .nodes
        .iter()
        .find_map(|node| {
            if let Node::Map { computed, .. } = node { computed.first().cloned() } else { None }
        })
        .context("computed value absent")?;
    let mut unused = value.clone();
    unused.column.name = "unused".into();
    let alias = ColumnRef { name: "alias".into(), ..value.column };
    compiled.relational.nodes = vec![
        Node::Source { id: "source".into(), source: 0 },
        Node::Map {
            id: "map".into(),
            input: "source".into(),
            computed: vec![value.clone(), unused],
        },
        Node::Rebind {
            id: "scope".into(),
            input: "map".into(),
            fields: vec![Binding { input: value.column, output: alias.clone() }],
        },
        Node::PartitionBy { id: "key".into(), input: "scope".into(), keys: vec![alias] },
        Node::Output { id: "output".into(), input: "key".into() },
    ];
    let report = compiled.placement()?;
    let used = &report.expressions[0];
    let unused = &report.expressions[1];
    assert!(used.field_consumers.iter().any(|consumer| consumer.node == "key"));
    assert!(unused.field_consumers.is_empty());
    // Both still cross a conservative maintenance barrier: no movement is authorized.
    assert_eq!(used.status, Status::MaintenanceRequired);
    assert_eq!(unused.status, Status::MaintenanceRequired);
    assert!(unused.obligations.contains(&"full_tuple_state_identity"));
    assert!(
        matches!(used.dependencies.first(), Some(super::Dependency::Operand { column, .. }) if column.name.ends_with("date_time"))
    );
    Ok(())
}

#[test]
fn scalar_lineage_visits_lazy_conditions_and_values_without_evaluation() -> Result<()> {
    let compiled = compile(
        "SELECT CASE WHEN id > 0 THEN price + id ELSE coalesce(price, 9223372036854775807 + id) END AS value FROM source.bid",
        &contract(),
    )?;
    let report = compiled.placement()?;
    let columns = report
        .expressions
        .iter()
        .filter(|entry| entry.current_execution == Execution::Local)
        .flat_map(|entry| &entry.dependencies)
        .filter_map(|dependency| {
            if let super::Dependency::Operand { column, .. } = dependency {
                Some(column.name.as_str())
            } else {
                None
            }
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert!(columns.iter().any(|name| name.ends_with("price")));
    assert!(columns.iter().any(|name| name.ends_with("id")));
    Ok(())
}
