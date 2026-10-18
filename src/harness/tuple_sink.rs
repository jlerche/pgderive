//! Exercise generic weighted tuples through the ordinary native jsonb row sink.
use crate::{
    catalog::{Deltas, ResultColumn, Sink},
    engine::{Batch, reader::BatchData},
};
use anyhow::Result;
pub fn sink(table: &str) -> Sink {
    Sink {
        table: table.into(),
        columns: vec![ResultColumn {
            name: "tuple".into(),
            oid: 3802,
            modifier: -1,
            collation: None,
        }],
    }
}
pub fn deltas<K: BatchData, V: BatchData>(batch: &Batch<K, V>) -> Result<Deltas> {
    let rows = batch
        .iter()
        .map(|(tuple, weight)| Ok((((), vec![serde_json::to_string(tuple)?]), *weight)))
        .collect::<Result<Vec<_>>>()?;
    Deltas::bag(&Batch::from_updates(rows)?)
}
