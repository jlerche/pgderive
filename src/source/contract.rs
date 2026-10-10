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
    /// Canonical source binding covering cluster, slot and native row layout.
    ///
    /// # Errors
    /// Rejects invalid contracts or serialization failure.
    pub fn encode(&self) -> Result<String> {
        self.validate()?;
        Ok(format!("pgderive-source-v1:{}", serde_json::to_string(self)?))
    }
    /// Slot ownership identity independent of schema or timeline changes.
    ///
    /// # Errors
    /// Rejects invalid contracts or serialization failure.
    pub fn ownership_key(&self) -> Result<String> {
        self.validate()?;
        Ok(serde_json::to_string(&(&self.identity.system, &self.identity.database, &self.slot))?)
    }
    /// Hold source DDL exclusion through the caller's publication transaction.
    ///
    /// # Errors
    /// Rejects schema drift or missing relations before destination/progress publication.
    pub async fn lock_and_verify(&self, sql: &tokio_postgres::Transaction<'_>) -> Result<()> {
        self.validate()?;
        for relation in &self.relations {
            sql.batch_execute(&format!(
                "LOCK TABLE {}.{} IN ACCESS SHARE MODE",
                quote(&relation.schema),
                quote(&relation.table)
            ))
            .await?;
        }
        self.verify(sql).await
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
        ensure!(
            crate::configuration::identifier(publication) && crate::configuration::identifier(slot),
            "invalid source publication/slot"
        );
        identity.verify_sql(sql).await?;
        let row = sql.query_one("SELECT current_database(),puballtables,pubinsert,pubupdate,pubdelete,EXISTS(SELECT 1 FROM pg_publication_namespace pn WHERE pn.pnpubid=p.oid),pubtruncate FROM pg_publication p WHERE pubname=$1", &[&publication]).await?;
        ensure!(
            row.try_get::<_, String>(0)? == identity.database
                && !row.try_get::<_, bool>(1)?
                && row.try_get::<_, bool>(2)?
                && row.try_get::<_, bool>(3)?
                && row.try_get::<_, bool>(4)?
                && !row.try_get::<_, bool>(5)?
                && row.try_get::<_, bool>(6)?,
            "unsupported source database/publication scope"
        );
        let rows = sql.query("SELECT c.oid,n.nspname,c.relname,c.relkind::text,c.relpersistence::text,c.relreplident::text,pr.prattrs IS NULL AND pr.prqual IS NULL,EXISTS(SELECT 1 FROM pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) FROM pg_publication_rel pr JOIN pg_publication p ON p.oid=pr.prpubid JOIN pg_class c ON c.oid=pr.prrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE p.pubname=$1 ORDER BY c.oid", &[&publication]).await?;
        let mut relations = Vec::new();
        for row in rows {
            ensure!(
                row.try_get::<_, String>(3)? == "r"
                    && row.try_get::<_, String>(4)? == "p"
                    && row.try_get::<_, String>(5)? == "f"
                    && row.try_get::<_, bool>(6)?
                    && !row.try_get::<_, bool>(7)?,
                "unsupported source table or replica identity"
            );
            let oid = row.try_get(0)?;
            relations.push(Relation {
                oid,
                schema: row.try_get(1)?,
                table: row.try_get(2)?,
                columns: columns(sql, oid).await?,
            });
        }
        ensure!(!relations.is_empty(), "source publication is empty");
        let contract =
            Self { identity, publication: publication.into(), slot: slot.into(), relations };
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
    /// Reinspect native metadata to catch changes absent from pgoutput row metadata.
    ///
    /// # Errors
    /// Rejects publication, PK, nullability, collation or other frozen contract changes.
    pub async fn verify(&self, sql: &(impl GenericClient + Sync)) -> Result<()> {
        let current =
            Self::inspect(sql, self.identity.clone(), &self.publication, &self.slot).await?;
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
async fn columns(sql: &(impl GenericClient + Sync), oid: u32) -> Result<Vec<Column>> {
    let rows = sql.query("SELECT a.attnum,a.attname,a.atttypid,a.atttypmod,a.attnotnull,a.attcollation,a.attgenerated::text,EXISTS(SELECT 1 FROM pg_index i WHERE i.indrelid=a.attrelid AND i.indisprimary AND a.attnum=ANY(i.indkey)),COALESCE(co.collisdeterministic,true) FROM pg_attribute a LEFT JOIN pg_collation co ON co.oid=a.attcollation WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum", &[&oid]).await?;
    let mut columns = Vec::new();
    for row in rows {
        let oid = row.try_get(2)?;
        ensure!(
            matches!(oid, 16 | 20 | 21 | 23 | 25 | 1043 | 2950 | 1114 | 1184 | 1700)
                && row.try_get::<_, String>(6)?.is_empty()
                && row.try_get::<_, bool>(8)?,
            "unsupported source type/generated column/nondeterministic collation"
        );
        columns.push(Column {
            position: row.try_get(0)?,
            name: row.try_get(1)?,
            oid,
            modifier: row.try_get(3)?,
            nullable: !row.try_get::<_, bool>(4)?,
            primary: row.try_get(7)?,
            collation: row.try_get(5)?,
        });
    }
    ensure!(
        !columns.is_empty()
            && columns.iter().any(|column| column.primary)
            && columns.iter().filter(|column| column.primary).all(|column| !column.nullable),
        "source requires nonnullable primary key columns"
    );
    Ok(columns)
}

pub(super) fn quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
fn validate_columns(columns: &[Column]) -> Result<()> {
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
            matches!(column.oid, 16 | 20 | 21 | 23 | 25 | 1043 | 2950 | 1114 | 1184 | 1700)
                && !(column.primary && column.nullable),
            "unsupported source column contract"
        );
    }
    Ok(())
}
