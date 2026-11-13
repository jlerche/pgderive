pub mod retire;
pub mod store;

use crate::engine::plan::{Arrangement, Definition, Kind, Node, Source};

/// Uniform-schema positive fixtures keep their graph topology explicit.
pub fn definition<'a>(
    identity: (&str, &str),
    sources: &[&str],
    nodes: impl IntoIterator<Item = (&'a str, Kind, Vec<&'a str>)>,
    arrangements: &[(&str, &str)],
    outputs: &[&str],
) -> Definition {
    let (revision, schema) = identity;
    Definition {
        revision: revision.into(),
        sources: sources
            .iter()
            .map(|id| Source { id: (*id).into(), schema: schema.into() })
            .collect(),
        nodes: nodes
            .into_iter()
            .map(|(id, kind, inputs)| Node {
                id: id.into(),
                kind,
                inputs: inputs.into_iter().map(str::to_owned).collect(),
                schema: schema.into(),
            })
            .collect(),
        arrangements: arrangements
            .iter()
            .map(|(id, node)| Arrangement {
                id: (*id).into(),
                node: (*node).into(),
                schema: schema.into(),
            })
            .collect(),
        outputs: outputs.iter().map(|id| (*id).into()).collect(),
    }
}
