//! Resolve SELECT-local WINDOW definitions before ordinary window lowering.
use super::node;
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
use std::{cell::Cell, collections::BTreeMap};

// PostgreSQL 17 FRAMEOPTION_DEFAULTS.
pub(super) const DEFAULT_FRAME: i32 = 1058;

#[derive(Clone, Copy)]
pub(super) struct Spec<'a> {
    pub(super) frame: &'a pg::WindowDef,
    pub(super) partition: &'a [Node],
    pub(super) order: &'a [Node],
}

struct Entry<'a> {
    spec: Spec<'a>,
    used: Cell<bool>,
}

#[derive(Default)]
pub(super) struct Definitions<'a>(BTreeMap<&'a str, Entry<'a>>);

impl<'a> Definitions<'a> {
    pub(super) fn new(clause: &'a [Node]) -> Result<Self> {
        let mut windows = Self::default();
        for definition in clause {
            let NodeEnum::WindowDef(value) = node(definition)? else {
                anyhow::bail!("invalid WINDOW definition");
            };
            ensure!(!value.name.is_empty(), "missing WINDOW name");
            ensure!(
                !windows.0.contains_key(value.name.as_str()),
                "duplicate WINDOW name {:?}",
                value.name
            );
            let spec = windows.inherit(value)?;
            windows.0.insert(&value.name, Entry { spec, used: Cell::new(false) });
        }
        Ok(windows)
    }

    pub(super) fn resolve(&self, value: &'a pg::WindowDef) -> Result<Spec<'a>> {
        if value.name.is_empty() {
            let spec = self.inherit(value)?;
            self.mark_used(&value.refname);
            Ok(spec)
        } else {
            // OVER name uses the definition verbatim, including its frame.
            let spec = self.get(&value.name)?;
            self.mark_used(&value.name);
            Ok(spec)
        }
    }

    pub(super) fn require_used(&self) -> Result<()> {
        // PostgreSQL analyzes even unused definitions. Avoid discarding their
        // expressions, which may contain errors or change aggregate semantics.
        for (name, entry) in &self.0 {
            ensure!(entry.used.get(), "unused WINDOW definition {name:?} unsupported");
        }
        Ok(())
    }

    fn mark_used(&self, name: &str) {
        let mut next = self.0.get(name);
        while let Some(entry) = next {
            if entry.used.replace(true) {
                break;
            }
            next = self.0.get(entry.spec.frame.refname.as_str());
        }
    }

    fn get(&self, name: &str) -> Result<Spec<'a>> {
        self.0
            .get(name)
            .map(|entry| entry.spec)
            .with_context(|| format!("window {name:?} does not exist"))
    }

    fn inherit(&self, value: &'a pg::WindowDef) -> Result<Spec<'a>> {
        let mut spec =
            Spec { frame: value, partition: &value.partition_clause, order: &value.order_clause };
        if !value.refname.is_empty() {
            let prior = self.get(&value.refname)?;
            ensure!(spec.partition.is_empty(), "cannot override a window's PARTITION BY");
            ensure!(
                spec.order.is_empty() || prior.order.is_empty(),
                "cannot override a window's ORDER BY"
            );
            ensure!(
                prior.frame.frame_options == DEFAULT_FRAME,
                "cannot copy a window with an explicit frame; use OVER name"
            );
            spec.partition = prior.partition;
            if spec.order.is_empty() {
                spec.order = prior.order;
            }
        }
        Ok(spec)
    }
}
