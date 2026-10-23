//! Bind side-local integral ON expressions without losing source tuple identity.
use super::{Column, resolve};
use crate::compiler::{
    ColumnRef,
    parser::scalar::Key,
    scalar::{self, Computed},
    typed,
};
use anyhow::{Context, Result, ensure};
use std::cell::Cell;
pub(super) struct Bound {
    pub keys: Vec<(ColumnRef, ColumnRef)>,
    pub computed: [Vec<Computed>; 2],
}
pub(super) fn bind(
    names: Vec<(Key, Key)>,
    left: &[Column],
    right: &[Column],
    prefix: &str,
) -> Result<Bound> {
    let mut all = left.to_vec();
    all.extend_from_slice(right);
    let mut bound = Bound { keys: Vec::new(), computed: [Vec::new(), Vec::new()] };
    for (a, b) in names {
        let a = operand(a, (&all, left), &mut bound.computed, prefix)?;
        let b = operand(b, (&all, left), &mut bound.computed, prefix)?;
        ensure!(a.0 != b.0, "ON equality must connect both inputs");
        let (a, b) = if a.0 == 0 { (a.1, b.1) } else { (b.1, a.1) };
        ensure!(
            a.value.oid == b.value.oid
                && a.modifier == b.modifier
                && a.collation == b.collation
                && matches!(a.value.oid, 16 | 20 | 21 | 23 | 1700 | 2950 | 1114 | 1184),
            "unsupported or incompatible join-tree key types"
        );
        bound.keys.push((a.value, b.value));
    }
    bound.keys.sort_unstable();
    bound.keys.dedup();
    Ok(bound)
}
fn operand(
    key: Key,
    columns: (&[Column], &[Column]),
    computed: &mut [Vec<Computed>; 2],
    prefix: &str,
) -> Result<(usize, Column)> {
    let side = |value: &Column| {
        usize::from(!columns.1.iter().any(|column| column.value.name == value.value.name))
    };
    let Key::Typed(ast) = key else {
        let Key::Column(name) = key else {
            anyhow::bail!("computed join keys require locally supported integral expressions");
        };
        let column = resolve(&name, columns.0)?;
        return Ok((side(column), column.clone()));
    };
    let input = Cell::new(None);
    let resolve = |name: &crate::compiler::parser::Name| {
        let column = resolve(name, columns.0)?;
        let side = side(column);
        ensure!(
            input.get().is_none_or(|prior| prior == side),
            "computed ON operand must be local to one input"
        );
        input.set(Some(side));
        Ok(column.value.clone())
    };
    let typed = typed::bind(&ast, &resolve, 0)?;
    ensure!(
        matches!(typed.oid(), 20 | 21 | 23),
        "computed join keys require integral result types"
    );
    let side = input.get().context("computed ON operand must reference its input")?;
    let column = scalar::register(typed, &mut computed[side], &format!("@{prefix}_input_{side}_"))?;
    Ok((
        side,
        Column {
            label: column.name.clone(),
            qualifiers: Vec::new(),
            value: column,
            modifier: -1,
            collation: 0,
        },
    ))
}
