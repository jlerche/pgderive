//! Encoded publication comparisons: immutable binding, epoch-scoped checkpoint.
use super::{Durable, checkpoint_digest};
use crate::engine::plan::Plan;
use anyhow::{Result, ensure};
use bytes::BytesMut;
use serde::Serialize;
use std::error::Error;
use tokio_postgres::types::{IsNull, ToSql, Type};

/// The same JSONB wire encoding as postgres-types, serialized once rather than
/// reconstructing and traversing JSON trees on every parameter binding.
#[derive(Debug)]
pub(super) struct Jsonb(Vec<u8>);
impl Jsonb {
    pub fn new(value: &(impl Serialize + ?Sized)) -> Result<Self> {
        let mut bytes = vec![1];
        serde_json::to_writer(&mut bytes, value)?;
        Ok(Self(bytes))
    }
}
impl ToSql for Jsonb {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }
    fn accepts(kind: &Type) -> bool {
        *kind == Type::JSONB
    }
    tokio_postgres::types::to_sql_checked!();
}

pub(super) struct Metadata {
    identity: String,
    epoch: u64,
    pub binding: Jsonb,
    pub source_key: String,
    pub query: Jsonb,
}
impl Metadata {
    pub fn new(plan: &Plan, boundary: &Durable) -> Result<Self> {
        Ok(Self {
            identity: plan.identity().into(),
            epoch: boundary.stored.epoch,
            binding: Jsonb::new(&boundary.binding)?,
            source_key: boundary.binding.source.ownership_key()?,
            query: query(plan, boundary)?,
        })
    }
    pub fn refresh(&mut self, plan: &Plan, boundary: &Durable) -> Result<()> {
        ensure!(self.identity == plan.identity(), "publication plan differs from claimed plan");
        // Every physical membership installation advances the authoritative epoch,
        // including maintenance at the same logical time and committed recovery.
        if self.epoch != boundary.stored.epoch {
            self.query = query(plan, boundary)?;
            self.epoch = boundary.stored.epoch;
        }
        Ok(())
    }
}
fn query(plan: &Plan, boundary: &Durable) -> Result<Jsonb> {
    let checkpoint = &boundary.stored.checkpoint;
    Jsonb::new(&(
        plan.identity(),
        plan.definition(),
        checkpoint.time,
        boundary.stored.epoch,
        checkpoint,
        checkpoint_digest(checkpoint)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::recovery::tests::fixture;
    use serde_json::{Value, json};

    #[test]
    fn encoded_jsonb_matches_native_parameter_bytes_and_rejects_other_types() -> Result<()> {
        for value in [
            json!(null),
            json!(["quote\"\\\n", "雪", null, true, u64::MAX]),
            json!({"nested": [1, "1.00"]}),
        ] {
            let cached = Jsonb::new(&value)?;
            let mut actual = BytesMut::new();
            let mut expected = BytesMut::new();
            cached.to_sql_checked(&Type::JSONB, &mut actual).map_err(anyhow::Error::msg)?;
            value.to_sql_checked(&Type::JSONB, &mut expected).map_err(anyhow::Error::msg)?;
            assert_eq!(actual, expected);
            assert!(cached.to_sql_checked(&Type::TEXT, &mut BytesMut::new()).is_err());
        }
        Ok(())
    }
    #[test]
    fn progress_reuses_query_bytes_but_checkpoint_epoch_refreshes_them() -> Result<()> {
        let (plan, mut writer) = fixture()?;
        let original = writer.metadata.query.0.as_ptr();
        writer.boundary.published_time += 1;
        writer.boundary.end = "0/10".parse()?;
        writer.metadata.refresh(&plan, &writer.boundary)?;
        assert!(std::ptr::eq(original, writer.metadata.query.0.as_ptr()));
        // Maintenance can advance the physical epoch without changing logical time.
        writer.boundary.stored.epoch += 1;
        writer.boundary.stored.checkpoint.arrangements[0].trace.generation += 1;
        writer.metadata.refresh(&plan, &writer.boundary)?;
        let query: Value = serde_json::from_slice(&writer.metadata.query.0[1..])?;
        assert_eq!(query[2], 1);
        assert_eq!(query[3], writer.boundary.stored.epoch);
        assert_eq!(query[4], serde_json::to_value(&writer.boundary.stored.checkpoint)?);
        assert_eq!(query[5], checkpoint_digest(&writer.boundary.stored.checkpoint)?);
        // The next checkpoint/recovery can change both logical time and epoch.
        writer.boundary.stored.epoch += 1;
        writer.boundary.stored.checkpoint.time += 1;
        writer.boundary.stored.checkpoint.arrangements[0].trace.time += 1;
        writer.metadata.refresh(&plan, &writer.boundary)?;
        let query: Value = serde_json::from_slice(&writer.metadata.query.0[1..])?;
        assert_eq!(query[2], 2);
        assert_eq!(query[3], writer.boundary.stored.epoch);
        let mut definition = plan.definition().clone();
        definition.revision.push_str("-different");
        assert!(writer.metadata.refresh(&Plan::new(definition)?, &writer.boundary).is_err());
        Ok(())
    }
}
