use super::{ColumnRef, Name};
use anyhow::Result;
pub(super) fn qualify(mut column: ColumnRef) -> ColumnRef {
    column.name = crate::compiler::relational::field(usize::from(column.right), &column.name);
    column.right = false;
    column
}
type Output =
    (Vec<crate::compiler::projection::OutputColumn>, Option<crate::compiler::output::Output>);
pub(super) fn output(
    columns: crate::compiler::parser::Columns,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<Output> {
    let transforms = columns.iter().map(|(_, _, transform)| transform.clone()).collect::<Vec<_>>();
    let columns = columns
        .into_iter()
        .map(|(name, label, _)| {
            Ok(crate::compiler::projection::OutputColumn { column: resolve(&name)?, label })
        })
        .collect::<Result<Vec<_>>>()?;
    let mapping = super::output_mapping(transforms, &columns)?;
    Ok((columns, mapping))
}
