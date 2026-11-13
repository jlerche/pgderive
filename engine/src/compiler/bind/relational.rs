use super::{ColumnRef, Name};
use anyhow::Result;
pub(super) fn qualify(mut column: ColumnRef) -> ColumnRef {
    column.name = crate::compiler::relational::field(usize::from(column.right), &column.name);
    column.right = false;
    column
}
pub(super) fn output(
    columns: crate::compiler::parser::Columns,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<Vec<crate::compiler::projection::OutputColumn>> {
    columns
        .into_iter()
        .map(|(name, label)| {
            Ok(crate::compiler::projection::OutputColumn { column: resolve(&name)?, label })
        })
        .collect()
}
