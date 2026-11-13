//! Bind projected membership while retaining complete source identities.
use super::{Compiled, partition};
use crate::{
    compiler::{parser::partition::Parsed, relational::Node},
    source::Contract,
};
use anyhow::{Result, ensure};
pub(super) fn bind(parsed: Parsed, distinct: bool, contract: &Contract) -> Result<Compiled> {
    let mut compiled = partition::bind(parsed, contract)?;
    let relational = &mut compiled.relational;
    if distinct {
        ensure!(
            relational.output.columns.iter().all(|column| matches!(
                column.column.oid,
                16 | 20 | 21 | 23 | 25 | 1043 | 1082 | 2950 | 1114 | 1184
            )),
            "unsupported DISTINCT native type"
        );
        let Some(Node::Output { id, input }) = relational.nodes.pop() else {
            anyhow::bail!("missing membership output")
        };
        let columns = relational
            .output
            .columns
            .iter()
            .map(|output| output.column.clone())
            .collect::<Vec<_>>();
        let position = relational.nodes.len();
        let (key, distinct) = (format!("distinct_key_{position}"), format!("distinct_{position}"));
        relational.nodes.push(Node::PartitionBy { id: key.clone(), input, keys: columns.clone() });
        relational.nodes.push(Node::SetDistinct { id: distinct.clone(), input: key, columns });
        relational.nodes.push(Node::Output { id, input: distinct });
    }
    Ok(compiled)
}
