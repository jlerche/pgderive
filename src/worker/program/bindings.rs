//! Typed scope-boundary projection and native SQL equijoin keys.
use super::relational::Key;
use crate::{
    compiler::{ColumnRef, relational::Binding},
    engine::dataflow::Project,
    transaction::Row,
};
use anyhow::{Context, ensure};

pub(super) fn rebind(fields: &[Binding]) -> Project<Key, Row, Key, Row> {
    let fields = fields.to_vec();
    Project::new(move |_: &Key, row: &Row| {
        let mut output = Row::new();
        for field in &fields {
            let value = row.get(&field.input.name).context("missing scoped field")?.clone();
            ensure!(
                output.insert(field.output.name.clone(), value).is_none(),
                "duplicate scoped output field"
            );
        }
        Ok(Some((Key::new(), output)))
    })
}
pub(super) fn key_by(keys: &[ColumnRef]) -> Project<Key, Row, Key, Row> {
    let keys = keys.to_vec();
    Project::new(move |_: &Key, row: &Row| {
        let key = keys
            .iter()
            .map(|column| crate::compiler::native_key(column, row))
            .collect::<anyhow::Result<Option<Vec<_>>>>()?;
        Ok(key.map(|key| (key.into(), row.clone())))
    })
}

/// A Cartesian product uses one constant navigation key, including NULL tuples.
pub(super) fn cross_key() -> Project<Key, Row, Key, Row> {
    Project::new(|_: &Key, row: &Row| Ok(Some((Key::new(), row.clone()))))
}
