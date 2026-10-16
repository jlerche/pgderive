use super::{finalizers, tests::contract};
use crate::compiler::{
    compile,
    partition::{Function, Mode, statistics},
    relational::Node,
};
use anyhow::{Context, Result};

#[test]
fn additive_finalizer_distinguishes_value_fields_from_whole_row_validation() -> Result<()> {
    let compiled = compile(
        "SELECT id,COUNT(*) AS rows,COUNT(price) AS present,SUM(price) AS total,AVG(price) AS mean FROM source.bid GROUP BY id",
        &contract(),
    )?;
    let report = serde_json::to_value(compiled.placement()?)?;
    let finalizer = &report["finalizers"][0];
    assert_eq!(finalizer["state_encoding"], "signed_arbitrary_precision_decimal");
    assert_eq!(
        finalizer["validation_fields"].as_array().context("validation fields absent")?.len(),
        9
    );
    assert_eq!(finalizer["outputs"][0]["value_fields"], serde_json::json!(["@statistic_1"]));
    assert_eq!(
        finalizer["outputs"][2]["value_fields"],
        serde_json::json!(["@statistic_5", "@statistic_6"])
    );
    assert_eq!(finalizer["outputs"][2]["sql_type_oid"], 1700);
    assert_eq!(finalizer["outputs"][3]["encoding"], "sum_count_ratio");
    assert_eq!(finalizer["outputs"][3]["sql_type_oid"], 1700);
    let spec = compiled
        .relational
        .nodes
        .iter()
        .find_map(|node| if let Node::Finalize { spec, .. } = node { Some(spec) } else { None })
        .context("finalizer absent")?;
    let aggregate = &spec.aggregates[2];
    assert_eq!(
        finalizers::operands(spec, &aggregate.field),
        [statistics::field(5), statistics::field(6)]
    );
    assert!(
        finalizers::measurements(spec, "@statistic_6")
            .iter()
            .any(|column| column.name.ends_with("price"))
    );
    assert_eq!(finalizers::operands(spec, "0:id"), ["0:id"]);
    assert_eq!(finalizers::measurements(spec, "0:id").len(), 1);
    assert!(finalizers::measurements(spec, "@statistic_0").is_empty());
    assert!(finalizers::operands(spec, "missing").is_empty());
    Ok(())
}

#[test]
fn finalizer_report_rejects_unmodeled_codecs_and_reports_distinct_count() -> Result<()> {
    let compiled =
        compile("SELECT id,COUNT(DISTINCT price) AS n FROM source.bid GROUP BY id", &contract())?;
    let report = serde_json::to_value(compiled.placement()?)?;
    assert_eq!(report["finalizers"][0]["outputs"][0]["sql_type_oid"], 20);
    let mut nodes = compiled.relational.nodes;
    let spec = nodes
        .iter_mut()
        .find_map(|node| if let Node::Finalize { spec, .. } = node { Some(spec) } else { None })
        .context("finalizer absent")?;
    spec.aggregates[0].function = Function::Min;
    assert!(finalizers::analyze(&nodes).is_err());
    let spec = nodes
        .iter_mut()
        .find_map(|node| if let Node::Finalize { spec, .. } = node { Some(spec) } else { None })
        .context("finalizer absent")?;
    spec.mode = Mode::Ranking { order: vec![] };
    assert!(finalizers::analyze(&nodes).is_err());
    Ok(())
}

#[test]
fn final_output_lineage_reaches_shared_local_aggregate_operands() -> Result<()> {
    let compiled = compile(
        "SELECT q.value,SUM(q.value) AS total FROM (SELECT price+id AS value FROM source.bid) q GROUP BY q.value",
        &contract(),
    )?;
    let report = compiled.placement()?;
    let local = report
        .expressions
        .iter()
        .find(|entry| entry.current_execution == super::Execution::Local)
        .context("local operand absent")?;
    assert!(
        local.field_consumers.iter().any(|consumer| consumer.kind == super::ConsumerKind::Output)
    );
    assert!(
        local
            .field_consumers
            .iter()
            .any(|consumer| consumer.kind == super::ConsumerKind::AggregateOrWindow)
    );
    assert_eq!(local.status, super::Status::MaintenanceRequired);
    Ok(())
}

#[test]
fn mixed_distinct_numeric_sum_preserves_declared_numeric_type() -> Result<()> {
    let mut contract = contract();
    contract.relations[0].columns[1].oid = 1700;
    let compiled = compile(
        "SELECT id,COUNT(DISTINCT id) AS n,SUM(price) AS total FROM source.bid GROUP BY id",
        &contract,
    )?;
    let report = serde_json::to_value(compiled.placement()?)?;
    assert_eq!(report["finalizers"][0]["outputs"][1]["sql_type_oid"], 1700);
    Ok(())
}

#[test]
fn error_domain_fixtures_force_lazy_expressions_into_local_grouping() -> Result<()> {
    for expression in [
        "CASE WHEN id > 0 THEN id ELSE price + 1 END",
        "CASE WHEN price + 1 > 0 THEN id ELSE 0 END",
        "COALESCE(id, price + 1)",
        "CASE WHEN id > 0 THEN id ELSE price::smallint END",
    ] {
        let compiled = compile(
            &format!(
                "SELECT q.value,COUNT(*) AS n FROM (SELECT {expression} AS value FROM source.bid) q GROUP BY q.value"
            ),
            &contract(),
        )?;
        assert!(
            compiled
                .relational
                .nodes
                .iter()
                .any(|node| matches!(node, Node::Map { computed, .. } if computed.len() == 1)),
            "{expression}"
        );
    }
    Ok(())
}
