//! Evaluate each bound specification over the same row occurrences.
use crate::compiler::{
    ColumnRef,
    partition::{Aggregate, Mode, Partition, PreviousWindows},
    relational::Node,
    scalar::Computed,
};
use anyhow::{Result, ensure};
use std::collections::BTreeSet;

struct Window {
    keys: Vec<ColumnRef>,
    spec: Partition,
}

#[derive(Default)]
pub(super) struct Windows(Vec<Window>);

impl Windows {
    pub(super) const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(super) fn aggregates(&self) -> impl Iterator<Item = &Aggregate> {
        self.0.iter().flat_map(|window| &window.spec.aggregates)
    }

    pub(super) fn add(&mut self, mode: Mode, keys: Vec<ColumnRef>, value: Aggregate) -> Result<()> {
        super::validate_keys(&keys)?;
        if let Some(window) =
            self.0.iter_mut().find(|window| window.keys == keys && window.spec.mode == mode)
        {
            window.spec.aggregates.push(value);
        } else {
            self.0.push(Window {
                keys,
                spec: Partition { mode, aggregates: vec![value], previous_windows: None },
            });
        }
        Ok(())
    }

    pub(super) fn validate_occurrences(&self, expanded: bool) -> Result<()> {
        if !expanded {
            return Ok(());
        }
        for window in &self.0 {
            let spec = &window.spec;
            if let Mode::Rows { order, .. } | Mode::Ranking { order } = &spec.mode
                && (matches!(spec.mode, Mode::Rows { .. })
                    || spec.aggregates.iter().any(|value| value.function.occurrence_order()))
            {
                ensure!(
                    order
                        .iter()
                        .any(|order| order.column.name == crate::compiler::expansion::FIELD),
                    "expanded positional windows must order by generated ordinal"
                );
            }
        }
        Ok(())
    }

    pub(super) fn nodes(self, computed: Vec<Computed>) -> Result<Vec<Node>> {
        let mut windows = self.0.into_iter();
        let Some(first) = windows.next() else { anyhow::bail!("missing window specification") };
        let mut fields: BTreeSet<_> =
            first.spec.aggregates.iter().map(|value| value.field.clone()).collect();
        let mut common_keys: BTreeSet<_> = first.keys.iter().map(|key| key.name.clone()).collect();
        let mut nodes = super::nodes(first.keys, first.spec, computed);
        for Window { keys, mut spec } in windows {
            common_keys.retain(|name| keys.iter().any(|key| &key.name == name));
            spec.previous_windows =
                Some(PreviousWindows { fields: fields.clone(), common_keys: common_keys.clone() });
            fields.extend(spec.aggregates.iter().map(|value| value.field.clone()));
            let Some(Node::Output { input, .. }) = nodes.pop() else {
                anyhow::bail!("missing prior window output");
            };
            let next = super::nodes(keys, spec, Vec::new());
            super::composition::append(&mut nodes, next, &input);
        }
        Ok(nodes)
    }
}
