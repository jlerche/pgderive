//! Ordered occurrence top-k for deterministic `PostgreSQL` `ROW_NUMBER` cutoffs.
use super::relational::Key;
use crate::{
    compiler::topk::Spec,
    engine::dataflow::{OrderedSelection, Project},
    transaction::Row,
};
use anyhow::ensure;
#[path = "topk_encoding.rs"]
mod codec;
use codec::encoding;
const ORDER: &str = "\0ordered";
pub(super) fn input(spec: &Spec) -> Project<Key, Row, Key, Row> {
    let spec = spec.clone();
    Project::new(move |key: &Key, row: &Row| {
        ensure!(row.keys().all(|field| !field.starts_with('\0')), "top-k internal field collision");
        let mut indexed = row.clone();
        indexed.insert(ORDER.into(), Some(encoding(&spec, row)?));
        Ok(Some((key.clone(), indexed)))
    })
}
pub(super) fn operator(spec: &Spec) -> OrderedSelection<Key, Row> {
    let offset = spec.offset;
    let spec = spec.clone();
    let peer_spec = spec.with_ties.then(|| spec.clone());
    let selection = OrderedSelection::new(spec.limit, move |key: &Key, row: &Row, position| {
        ensure!(
            row.get(ORDER) == Some(&Some(encoding(&spec, row)?)),
            "invalid top-k identity codec"
        );
        let mut output = row.clone();
        output.remove(ORDER);
        if let Some(field) = &spec.field {
            ensure!(
                output.insert(field.clone(), Some(i64::try_from(position)?.to_string())).is_none(),
                "top-k result field collision"
            );
        }
        Ok((key.clone(), output))
    })
    .skip(offset);
    if let Some(spec) = peer_spec {
        selection.with_ties(move |row| codec::peer_key(&spec, row))
    } else {
        selection
    }
}

#[cfg(test)]
#[path = "topk_codec_tests.rs"]
mod tests;
