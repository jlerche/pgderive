//! SQL GROUP BY checks the input namespace before the visible select list.
use super::{Body, bind_key, syntax};
use crate::compiler::{
    ColumnRef,
    bind::UnknownColumn,
    parser::{Name, scalar::Key},
};
use anyhow::{Result, ensure};

pub(super) fn resolve(
    body: &Body,
    input: &impl Fn(&Name) -> Result<ColumnRef>,
    scope: Option<usize>,
) -> Result<Vec<Key>> {
    body.groups
        .iter()
        .map(|key| {
            let Key::Column(name) = key else { return Ok(key.clone()) };
            let [label] = name.0.as_slice() else { return Ok(key.clone()) };
            match input(name) {
                Ok(_) => Ok(key.clone()),
                Err(error) if error.is::<UnknownColumn>() => {
                    alias(label, &body.targets[..body.visible], input, scope)?.ok_or(error)
                }
                Err(error) => Err(error),
            }
        })
        .collect()
}

fn alias(
    label: &str,
    visible: &[syntax::Target],
    input: &impl Fn(&Name) -> Result<ColumnRef>,
    scope: Option<usize>,
) -> Result<Option<Key>> {
    let mut matches = visible.iter().filter(|target| target.label == label).peekable();
    let Some(first) = matches.next() else { return Ok(None) };
    let key = syntax::group_target(first)?;
    if matches.peek().is_some() {
        let mut computed = Vec::new();
        let bound = bind_key(key.clone(), input, &mut computed, scope)?;
        for target in matches {
            let candidate = bind_key(syntax::group_target(target)?, input, &mut computed, scope)?;
            ensure!(bound == candidate, "ambiguous GROUP BY output label {label}");
        }
    }
    Ok(Some(key))
}
