use anyhow::{Context, Result, bail, ensure};
use postgres_replication::protocol::{LogicalReplicationMessage, RelationBody, Tuple, TupleData};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};

pub type Row = BTreeMap<String, Option<String>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Change {
    pub(super) schema: String,
    pub(super) table: String,
    pub(super) operation: Operation,
    pub(super) old: Option<Row>,
    pub(super) new: Option<Row>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Transaction {
    pub(super) xid: u32,
    pub(super) commit_lsn: String,
    pub(super) end_lsn: String,
    pub(super) changes: Vec<Change>,
    pub(super) batch: crate::weighted::Batch,
}

struct Relation {
    schema: String,
    table: String,
    columns: Vec<String>,
}

impl Relation {
    fn decode(&self, tuple: &Tuple, limit: usize) -> Result<Row> {
        let bytes = tuple.tuple_data().iter().try_fold(0_usize, |total, value| {
            let length = match value {
                TupleData::Text(bytes) | TupleData::Binary(bytes) => bytes.len(),
                _ => 0,
            };
            total.checked_add(length).context("row byte count overflow")
        })?;
        ensure!(bytes <= limit, "source row byte limit exceeded");
        ensure!(self.columns.len() == tuple.tuple_data().len(), "tuple column count mismatch");
        self.columns.iter().cloned().zip(tuple.tuple_data()).map(|(name, value)| {
            let text = match value {
                TupleData::Null => None,
                TupleData::Text(bytes) => Some(std::str::from_utf8(bytes)?.to_owned()),
                TupleData::UnchangedToast => bail!("unchanged TOAST requires stored row reconstruction; unsupported in this listener"),
                TupleData::Binary(_) => bail!("binary pgoutput tuples are unsupported"),
            };
            Ok((name, text))
        }).collect()
    }
}

#[derive(Default)]
pub struct Decoder {
    relations: HashMap<u32, Relation>,
    pending: Option<(u32, Vec<Change>)>,
    pending_bytes: u64,
    limits: crate::engine::execution::Limits,
    failed: bool,
}

impl Decoder {
    pub(super) fn new(limits: crate::engine::execution::Limits) -> Self {
        Self { limits, ..Self::default() }
    }
    pub(super) fn begin(&mut self, xid: u32) -> Result<()> {
        ensure!(!self.failed, "failed source transaction requires a fresh decoder");
        ensure!(self.pending.is_none(), "nested transaction BEGIN");
        self.pending = Some((xid, Vec::new()));
        self.pending_bytes = 0;
        Ok(())
    }

    pub(super) fn commit(&mut self, commit_lsn: String, end_lsn: String) -> Result<Transaction> {
        ensure!(!self.failed, "failed source transaction cannot commit");
        let (xid, changes) = self.pending.take().context("COMMIT without BEGIN")?;
        let batch = crate::weighted::Batch::from_changes_with_limits(&changes, self.limits)?;
        Ok(Transaction { xid, commit_lsn, end_lsn, changes, batch })
    }

    fn relation(&mut self, message: &RelationBody) -> Result<()> {
        let columns = message
            .columns()
            .iter()
            .map(|column| column.name().map(str::to_owned))
            .collect::<std::io::Result<Vec<_>>>()?;
        self.relations.insert(
            message.rel_id(),
            Relation {
                schema: message.namespace()?.to_owned(),
                table: message.name()?.to_owned(),
                columns,
            },
        );
        Ok(())
    }

    pub(super) fn message(
        &mut self,
        message: &LogicalReplicationMessage,
        limit: usize,
    ) -> Result<()> {
        ensure!(!self.failed, "failed source transaction cannot continue");
        let result = self.message_inner(message, limit);
        self.failed = result.is_err();
        result
    }
    fn message_inner(&mut self, message: &LogicalReplicationMessage, limit: usize) -> Result<()> {
        let (id, operation, old, new) = match message {
            LogicalReplicationMessage::Relation(message) => return self.relation(message),
            LogicalReplicationMessage::Type(_) | LogicalReplicationMessage::Origin(_) => {
                return Ok(());
            }
            LogicalReplicationMessage::Insert(row) => {
                (row.rel_id(), Operation::Insert, None, Some(row.tuple()))
            }
            LogicalReplicationMessage::Update(row) => {
                ensure!(
                    row.key_tuple().is_none(),
                    "key-only old images need stored state; use REPLICA IDENTITY FULL"
                );
                (row.rel_id(), Operation::Update, row.old_tuple(), Some(row.new_tuple()))
            }
            LogicalReplicationMessage::Delete(row) => {
                ensure!(
                    row.key_tuple().is_none(),
                    "key-only delete images need stored state; use REPLICA IDENTITY FULL"
                );
                (row.rel_id(), Operation::Delete, row.old_tuple(), None)
            }
            _ => bail!("unsupported logical replication message: {message:?}"),
        };
        let relation = self.relations.get(&id).context("row arrived before relation metadata")?;
        let change = Change {
            schema: relation.schema.clone(),
            table: relation.table.clone(),
            operation,
            old: old.map(|tuple| relation.decode(tuple, self.limits.record_bytes)).transpose()?,
            new: new.map(|tuple| relation.decode(tuple, self.limits.record_bytes)).transpose()?,
        };
        let bytes = crate::engine::execution::record_size(&change, self.limits.record_bytes)?;
        let next_bytes = self
            .pending_bytes
            .checked_add(u64::try_from(bytes)?)
            .context("transaction byte count overflow")?;
        ensure!(next_bytes <= self.limits.output_bytes, "source transaction byte limit exceeded");
        let (_, changes) = self.pending.as_mut().context("row outside BEGIN/COMMIT")?;
        ensure!(changes.len() < limit, "transaction change limit exceeded; no rows acknowledged");
        changes.push(change);
        self.pending_bytes = next_bytes;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
