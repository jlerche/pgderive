use super::spec::Query as Spec;
use crate::{
    engine::{
        Batch,
        dataflow::{GroupSum, Project, Stream},
        plan::{
            self, Kind, Node, Plan,
            query::{GroupedJoin, Operators},
        },
    },
    source::Contract,
    transaction::Row,
    weighted,
};
use anyhow::{Context, Result};
type Group = Option<String>;
type Joined = (Row, Row);
type Inputs = Stream<plan::query::Inputs<String, Row, Row>>;
pub(super) type Query = GroupedJoin<String, Row, Row, Row, Row, Group, Joined>;

pub(super) fn build(
    contract: &Contract,
    spec: &Spec,
    settings: plan::query::Settings,
) -> Result<Query> {
    spec.validate(contract)?;
    let group = spec.group.clone();
    let sum = spec.sum.clone();
    let operators = Operators {
        left: Project::new(|key: &String, row: &Row| Ok(Some((key.clone(), row.clone())))),
        right: Project::new(|key: &String, row: &Row| Ok(Some((key.clone(), row.clone())))),
        group: Project::new(move |_: &String, value: &Joined| {
            Ok(Some((value.0.get(&group).context("missing group column")?.clone(), value.clone())))
        }),
        sum: GroupSum::new(move |_: &Group, rows: &Joined| {
            rows.1
                .get(&sum)
                .context("missing SUM column")?
                .as_ref()
                .map(|value| value.parse::<i64>().map_err(Into::into))
                .transpose()
        }),
    };
    Query::new_with_limits(
        plan(contract, spec)?,
        operators,
        settings.store,
        settings.block_rows,
        settings.limits,
    )
}
pub(super) fn plan(contract: &Contract, spec: &Spec) -> Result<Plan> {
    use sha2::{Digest, Sha256};
    let identity = format!("{:x}", Sha256::digest(serde_json::to_vec(&(contract, spec))?));
    let schema = |id: &str| format!("worker-grouped-v1:{identity}:{id}");
    let nodes = [
        ("left_source", Kind::Source, vec!["left_source_rows"]),
        ("right_source", Kind::Source, vec!["right_source_rows"]),
        ("left", Kind::Project, vec!["left_source"]),
        ("right", Kind::Project, vec!["right_source"]),
        ("join", Kind::Join, vec!["left", "right"]),
        ("group", Kind::Project, vec!["join"]),
        ("aggregate", Kind::Aggregate, vec!["group"]),
    ]
    .into_iter()
    .map(|(id, kind, inputs)| Node {
        id: id.into(),
        kind,
        inputs: inputs.into_iter().map(Into::into).collect(),
        schema: schema(id),
    })
    .collect();
    Plan::new(plan::Definition {
        revision: format!("worker-grouped-v1:{identity}"),
        sources: [("left_source_rows", "left_source"), ("right_source_rows", "right_source")]
            .into_iter()
            .map(|(id, node)| plan::Source { id: id.into(), schema: schema(node) })
            .collect(),
        nodes,
        arrangements: [
            ("left", "left"),
            ("right", "right"),
            ("sums", "aggregate"),
            ("output", "aggregate"),
        ]
        .into_iter()
        .map(|(id, node)| plan::Arrangement {
            id: id.into(),
            node: node.into(),
            schema: schema(id),
        })
        .collect(),
        outputs: vec!["aggregate".into()],
    })
}
pub(super) fn inputs(batch: &weighted::Batch, spec: &Spec, time: u64) -> Result<Inputs> {
    Ok(Stream {
        time,
        batch: (
            side(batch, (&spec.left_schema, &spec.left_table), &spec.left_key)?,
            side(batch, (&spec.right_schema, &spec.right_table), &spec.right_key)?,
        ),
    })
}
fn side(batch: &weighted::Batch, relation: (&str, &str), key: &str) -> Result<Batch<String, Row>> {
    let rows = batch
        .updates
        .iter()
        .filter(|update| update.tuple.schema == relation.0 && update.tuple.table == relation.1)
        .map(|update| {
            Ok(update
                .tuple
                .row
                .get(key)
                .context("source join key absent")?
                .as_ref()
                .map(|value| ((value.clone(), update.tuple.row.clone()), update.weight)))
        })
        .collect::<Result<Vec<_>>>()?;
    Batch::from_updates(rows.into_iter().flatten())
}
