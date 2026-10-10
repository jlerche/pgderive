//! `PostgreSQL` LAG/LEAD over complete ordered row occurrences, respecting NULLs.
use crate::compiler::{navigation::Navigation, partition::Aggregate};
use anyhow::{Context, Result};
pub(super) fn evaluate(
    aggregate: &Aggregate,
    navigation: &Navigation,
    rows: &[super::partition::Ordered],
    index: usize,
    lead: bool,
) -> Result<Option<String>> {
    let row = &rows[index].1;
    let Some(offset) = navigation.offset.value(row)? else {
        return Ok(None);
    };
    let position = i64::try_from(index)?;
    let offset = i64::from(offset);
    let target = if lead { position.checked_add(offset) } else { position.checked_sub(offset) }
        .context("navigation position overflow")?;
    let Some((_, previous)) = usize::try_from(target).ok().and_then(|target| rows.get(target))
    else {
        return navigation.default.value(row);
    };
    let argument = aggregate.argument.as_ref().context("missing navigation argument")?;
    Ok(previous.get(&argument.name).context("missing navigation value")?.clone())
}
