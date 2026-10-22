use anyhow::{Context, Result, bail, ensure};
use postgres_replication::protocol::{LogicalReplicationMessage, RelationBody, Tuple, TupleData};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};

pub type Row = crate::engine::value::Value<BTreeMap<String, Option<String>>>;

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
    fn decode(&self, tuple: &Tuple, prior: Option<&Row>, limit: usize) -> Result<Row> {
        ensure!(self.columns.len() == tuple.tuple_data().len(), "tuple column count mismatch");
        // Measure the reconstructed values before cloning. Wire length alone omits TOAST.
        let values = self
            .columns
            .iter()
            .zip(tuple.tuple_data())
            .map(|(name, value)| Self::value(name, value, prior))
            .collect::<Result<Vec<_>>>()?;
        let bytes = values.iter().try_fold(0_usize, |total, value| {
            total.checked_add(value.map_or(0, str::len)).context("row byte count overflow")
        })?;
        ensure!(bytes <= limit, "source row byte limit exceeded");
        Ok(self
            .columns
            .iter()
            .cloned()
            .zip(values.into_iter().map(|value| value.map(str::to_owned)))
            .collect())
    }
    fn value<'a>(
        name: &str,
        value: &'a TupleData,
        prior: Option<&'a Row>,
    ) -> Result<Option<&'a str>> {
        match value {
            TupleData::Null => Ok(None),
            TupleData::Text(bytes) => Ok(Some(std::str::from_utf8(bytes)?)),
            TupleData::UnchangedToast => {
                let prior =
                    prior.context("unchanged TOAST requires a complete same-update old image")?;
                let value =
                    prior.get(name).context("unchanged TOAST column missing in old image")?;
                ensure!(value.is_some(), "unchanged TOAST cannot refer to a NULL old value");
                Ok(value.as_deref())
            }
            TupleData::Binary(_) => bail!("binary pgoutput tuples are unsupported"),
        }
    }
}

#[derive(Default)]
pub struct Decoder {
    relations: HashMap<u32, Relation>,
    pending: Option<(u32, Vec<Change>)>,
    pending_bytes: u64,
    limits: crate::engine::execution::Limits,
    failed: bool,
    contract: Option<crate::source::Contract>,
}

impl Decoder {
    pub(super) fn new(limits: crate::engine::execution::Limits) -> Self {
        Self { limits, ..Self::default() }
    }
    pub(super) fn registered(
        limits: crate::engine::execution::Limits,
        contract: crate::source::Contract,
    ) -> Self {
        Self { limits, contract: Some(contract), ..Self::default() }
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
        if let Some(contract) = &self.contract {
            contract.check_relation(message)?;
        }
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
        // FULL old images must decode independently. Only the new side of the same
        // UPDATE may inherit omitted TOAST; never read a later SQL source snapshot.
        let old =
            old.map(|tuple| relation.decode(tuple, None, self.limits.record_bytes)).transpose()?;
        let prior = if operation == Operation::Update { old.as_ref() } else { None };
        let new =
            new.map(|tuple| relation.decode(tuple, prior, self.limits.record_bytes)).transpose()?;
        let change = Change {
            schema: relation.schema.clone(),
            table: relation.table.clone(),
            operation,
            old,
            new,
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

#[cfg(test)]
mod toast_tests;
