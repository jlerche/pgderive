//! Bind projected membership while retaining complete source identities.
use super::{Compiled, native_join};
use crate::{
    compiler::{parser::native_join::Parsed, relational::Node},
    source::Contract,
};
use anyhow::{Result, ensure};
pub(super) fn bind(parsed: Parsed, distinct: bool, contract: &Contract) -> Result<Compiled> {
    let mut compiled = native_join::bind(parsed, contract)?;
    let relational = &mut compiled.relational;
    if distinct {
        ensure!(
            relational.output.columns.iter().all(|column| matches!(
                column.column.oid,
                16 | 20 | 21 | 23 | 25 | 1043 | 2950 | 1114 | 1184
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
        relational.nodes.push(Node::PartitionBy {
            id: "distinct_key".into(),
            input,
            keys: columns.clone(),
        });
        relational.nodes.push(Node::SetDistinct {
            id: "distinct".into(),
            input: "distinct_key".into(),
            columns,
        });
        relational.nodes.push(Node::Output { id, input: "distinct".into() });
    }
    Ok(compiled)
}
