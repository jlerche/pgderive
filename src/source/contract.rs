use super::Identity;
use anyhow::{Context, Result, ensure};
use postgres_replication::protocol::{RelationBody, ReplicaIdentity};
use serde::{Deserialize, Serialize};
use tokio_postgres::GenericClient;

/// Frozen full-row column contract, separate from weighted operator identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    /// Physical attribute number; dropped attributes may leave gaps.
    pub position: i16,
    /// Exact case-sensitive source column name.
    pub name: String,
    /// Native built-in `PostgreSQL` type OID.
    pub oid: u32,
    /// Exact type modifier, including declared varchar length.
    pub modifier: i32,
    /// Whether the source permits SQL NULL.
    pub nullable: bool,
    /// Source-specific primary-key membership, never a generic DBSP key constraint.
    pub primary: bool,
    /// Deterministic collation identity, or zero for noncollatable types.
    pub collation: u32,
}
/// Frozen ordinary persistent source table with full replication identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Relation {
    /// Exact source relation OID.
    pub oid: u32,
    /// Case-sensitive namespace.
    pub schema: String,
    /// Case-sensitive relation name.
    pub table: String,
    /// Full visible source row layout in physical attribute order.
    pub columns: Vec<Column>,
}
/// Explicit full-table publication contract captured from the source database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Contract {
    /// Bound server, codecs and catalog semantics; incompatible changes require rebuild.
    pub environment: super::Environment,
    /// Replication protocol cluster/database identity.
    pub identity: Identity,
    /// Exact publication name.
    pub publication: String,
    /// Exact slot name; WAL addresses are meaningful only within this binding.
    pub slot: String,
    /// Full source relation contracts, ordered by relation OID.
    pub relations: Vec<Relation>,
}
impl Contract {
    /// Check a persisted contract before it can bind durable source progress.
    ///
    /// # Errors
    /// Rejects malformed identity, empty/duplicate layouts or unsupported source types.
    pub fn validate(&self) -> Result<()> {
        self.environment.validate()?;
        ensure!(
            self.identity.system.parse::<u64>()? > 0
                && self.identity.timeline > 0
                && !self.identity.database.is_empty(),
            "invalid source identity"
        );
        ensure!(
            crate::configuration::identifier(&self.publication)
                && crate::configuration::identifier(&self.slot),
            "invalid source identifiers"
        );
        ensure!(!self.relations.is_empty(), "empty source contract");
        let mut previous = 0;
        for relation in &self.relations {
            ensure!(
                relation.oid > previous
                    && !relation.schema.is_empty()
                    && !relation.table.is_empty(),
                "invalid source relation ordering/name"
            );
            previous = relation.oid;
            validate_columns(&relation.columns)?;
        }
        Ok(())
    }
    /// Slot ownership identity independent of schema or timeline changes.
    ///
    /// # Errors
    /// Rejects invalid contracts or serialization failure.
    pub fn ownership_key(&self) -> Result<String> {
        self.validate()?;
        Ok(serde_json::to_string(&(&self.identity.system, &self.identity.database, &self.slot))?)
    }
    /// Hold source DDL exclusion and verify layout through publication.
    /// Semantic catalogs are verified during initialization, never in this path.
    ///
    /// # Errors
    /// Rejects schema drift or missing relations before destination/progress publication.
    pub async fn lock_and_verify_layout(
        &self,
        sql: &tokio_postgres::Transaction<'_>,
    ) -> Result<()> {
        self.validate()?;
        let tables = self
            .relations
            .iter()
            .map(|relation| format!("{}.{}", quote(&relation.schema), quote(&relation.table)))
            .collect::<Vec<_>>()
            .join(",");
        let lock = format!("LOCK TABLE {tables} IN ACCESS SHARE MODE");
        let current = inspect_locked_layout(sql, &lock, self).await?;
        ensure!(current == self.relations, "source schema/publication contract changed");
        Ok(())
    }
    /// Inspect an explicit full-table publication in the caller's source snapshot.
    /// Requires primary keys and REPLICA IDENTITY FULL on ordinary persistent tables.
    ///
    /// # Errors
    /// Rejects unsupported publication/table/type/collation contracts and SQL errors.
    pub async fn inspect(
        sql: &(impl GenericClient + Sync),
        identity: Identity,
        publication: &str,
        slot: &str,
    ) -> Result<Self> {
        super::Environment::configure(sql).await?;
        let relations = inspect_layout(sql, &identity, publication, slot).await?;
        let collations = relations
            .iter()
            .flat_map(|relation| relation.columns.iter().map(|column| column.collation))
            .collect::<Vec<_>>();
        let environment = super::Environment::inspect(sql, &collations).await?;
        let contract = Self {
            environment,
            identity,
            publication: publication.into(),
            slot: slot.into(),
            relations,
        };
        contract.validate()?;
        Ok(contract)
    }
    /// Validate each pgoutput relation announcement against the frozen row codec.
    ///
    /// # Errors
    /// Rejects unknown relations, renamed/retyped/reordered columns or changed identity.
    pub fn check_relation(&self, message: &RelationBody) -> Result<()> {
        let relation = self
            .relations
            .iter()
            .find(|relation| relation.oid == message.rel_id())
            .context("unregistered source relation")?;
        ensure!(
            relation.schema == message.namespace()?
                && relation.table == message.name()?
                && matches!(message.replica_identity(), ReplicaIdentity::Full)
                && relation.columns.len() == message.columns().len(),
            "source relation identity/layout changed"
        );
        for (expected, actual) in relation.columns.iter().zip(message.columns()) {
            ensure!(
                expected.name == actual.name()?
                    && expected.oid == u32::try_from(actual.type_id())?
                    && expected.modifier == actual.type_modifier()
                    && actual.flags() == 1,
                "source column contract changed"
            );
        }
        Ok(())
    }
    /// Reinspect the complete source and semantic environment during initialization.
    /// Use layout-only locking for steady-state publication.
    ///
    /// # Errors
    /// Rejects publication, PK, nullability, collation or other frozen contract changes.
    pub async fn verify(&self, sql: &(impl GenericClient + Sync)) -> Result<()> {
        let current =
            Self::inspect(sql, self.identity.clone(), &self.publication, &self.slot).await?;
        ensure!(current.environment == self.environment, "source semantic environment changed");
        ensure!(current == *self, "source schema/publication contract changed");
        Ok(())
    }
    /// Stable identity covering the complete source contract.
    ///
    /// # Errors
    /// Returns a serialization failure.
    pub fn digest(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
    }
}
// Keep the pipelining macro outside the deserialized contract's implementation:
// its internal unsafe expansion is unrelated to validation of persisted fields.
async fn inspect_locked_layout(
    sql: &tokio_postgres::Transaction<'_>,
    lock: &str,
    contract: &Contract,
) -> Result<Vec<Relation>> {
    // Queue the lock first, then inspect in a separate statement snapshot.
    // A DDL commit during the lock wait must be visible to inspection.
    let ((), current) = tokio::try_join!(
        biased;
        async { sql.batch_execute(lock).await.map_err(anyhow::Error::from) },
        inspect_layout(sql, &contract.identity, &contract.publication, &contract.slot),
    )?;
    Ok(current)
}

async fn inspect_layout(
    sql: &(impl GenericClient + Sync),
    identity: &Identity,
    publication: &str,
    slot: &str,
) -> Result<Vec<Relation>> {
    ensure!(
        crate::configuration::identifier(publication) && crate::configuration::identifier(slot),
        "invalid source publication/slot"
    );
    super::layout::inspect(sql, identity, publication).await
}

pub(super) fn quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
const SOURCE_TYPE_OIDS: &[u32] = &[16, 20, 21, 23, 25, 1043, 2950, 1114, 1184, 1700];

pub(super) fn validate_columns(columns: &[Column]) -> Result<()> {
    ensure!(
        !columns.is_empty() && columns.iter().any(|column| column.primary),
        "source requires a primary key"
    );
    let mut previous = 0;
    let mut names = std::collections::BTreeSet::new();
    for column in columns {
        ensure!(
            column.position > previous
                && !column.name.is_empty()
                && !column.name.contains('\0')
                && names.insert(&column.name),
            "invalid source column layout"
        );
        previous = column.position;
        ensure!(
            SOURCE_TYPE_OIDS.contains(&column.oid) && !(column.primary && column.nullable),
            "unsupported source column contract"
        );
    }
    Ok(())
}

#[cfg(test)]
mod matrix_tests {
    use super::SOURCE_TYPE_OIDS;
    use anyhow::Result;
    use serde::Deserialize;
    use std::collections::BTreeSet;

    #[derive(Deserialize)]
    struct Matrix {
        source_type_oids: Vec<u32>,
        unsupported_source_type_oids: Vec<u32>,
    }
    #[test]
    fn source_matrix_exactly_matches_admitted_types() -> Result<()> {
        let matrix: Matrix =
            serde_json::from_str(include_str!("../../docs/postgres-support-matrix.json"))?;
        let declared = matrix.source_type_oids.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(declared.len(), matrix.source_type_oids.len());
        assert_eq!(declared, SOURCE_TYPE_OIDS.iter().copied().collect::<BTreeSet<_>>());
        assert!(matrix.unsupported_source_type_oids.iter().all(|oid| !declared.contains(oid)));
        Ok(())
    }
}
