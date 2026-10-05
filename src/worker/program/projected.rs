use crate::{
    compiler::{Cell, Compiled, Projected},
    engine::{
        Batch,
        dataflow::{Project, TimedBatch},
        plan::{self, Plan, projection::Projection, query::Settings},
    },
    source::Contract,
    transaction::Row,
    weighted,
};
use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
pub(in crate::worker) type Output = Vec<Option<Cell>>;
pub(in crate::worker) type Query = Projection<Row, Output>;
pub(in crate::worker) fn build(
    contract: &Contract,
    compiled: &Compiled,
    settings: Settings,
) -> Result<Query> {
    let projection = compiled.projection().context("expected projection program")?.clone();
    let filter = compiled.clone();
    let operator = Project::new(move |(): &(), row: &Row| {
        if !filter.qualifies((row, row))? {
            return Ok(None);
        }
        Ok(Some(((), projection.row(row)?)))
    });
    Projection::new(super::plan(contract, compiled)?, operator, settings)
}
pub(in crate::worker) fn plan(
    contract: &Contract,
    compiled: &Compiled,
    projection: &Projected,
) -> Result<Plan> {
    contract.validate()?;
    anyhow::ensure!(
        contract
            .relations
            .iter()
            .any(|relation| relation.schema == projection.schema
                && relation.table == projection.table),
        "projection source absent"
    );
    let identity = format!("{:x}", Sha256::digest(serde_json::to_vec(&(contract, compiled))?));
    let schema = |id: &str| format!("worker-projection-v1:{identity}:{id}");
    Plan::new(plan::Definition {
        revision: format!("worker-projection-v1:{identity}"),
        sources: vec![plan::Source { id: "rows".into(), schema: schema("source") }],
        nodes: vec![
            plan::Node {
                id: "source".into(),
                kind: plan::Kind::Source,
                inputs: vec!["rows".into()],
                schema: schema("source"),
            },
            plan::Node {
                id: "project".into(),
                kind: plan::Kind::Project,
                inputs: vec!["source".into()],
                schema: schema("output"),
            },
        ],
        arrangements: vec![plan::Arrangement {
            id: "output".into(),
            node: "project".into(),
            schema: schema("output"),
        }],
        outputs: vec!["project".into()],
    })
}
pub(in crate::worker) fn inputs(
    batch: &weighted::Batch,
    projection: &Projected,
    time: u64,
) -> Result<TimedBatch<Batch<(), Row>>> {
    Ok(TimedBatch {
        time,
        batch: Batch::from_updates(
            batch
                .updates
                .iter()
                .filter(|update| {
                    update.tuple.schema == projection.schema
                        && update.tuple.table == projection.table
                })
                .map(|update| (((), update.tuple.row.clone()), update.weight)),
        )?,
    })
}
