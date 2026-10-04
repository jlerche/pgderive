use super::{Catalog, lock, shared};
use crate::{
    catalog::Writer,
    engine::{
        plan::{Checkpoint, Plan},
        reader::ObjectRef,
    },
};
use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};
use tokio_postgres::Client;

/// Durable coarse protection, retained after disconnect or process failure.
///
/// Upload protection blocks collection; reader/recovery protection retains exact
/// immutable roots and their transitive blocks. Tokens never expire on a timer.
pub struct Protection {
    catalog: Catalog,
    token: String,
    uploading: bool,
    roots: Vec<ObjectRef>,
    namespace: Option<String>,
    stored: Option<crate::catalog::Stored>,
}
impl Catalog {
    /// Reserve initial bootstrap storage before progress/writer ownership exists.
    /// Active queries must use `Writer::protect_upload`.
    /// Keep this protection until publication is authoritatively confirmed. Errors
    /// retain uncertain reservations; never infer release from connection loss.
    ///
    /// # Errors
    /// Rejects reused/invalid tokens or database failures.
    pub async fn protect_upload(&self, sql: &mut Client, token: &str) -> Result<Protection> {
        insert(sql, self, token, 0).await
    }
    /// Pin an authoritative checkpoint before opening its objects for recovery/read.
    /// Retain the protection for every reader using the returned root membership.
    ///
    /// # Errors
    /// Rejects corrupt/missing checkpoints, reused tokens or database failures.
    pub async fn protect_checkpoint(
        &self,
        sql: &mut Client,
        plan: &Plan,
        token: &str,
    ) -> Result<Protection> {
        self.pin_checkpoint(sql, plan, token, None).await
    }
    async fn pin_checkpoint(
        &self,
        sql: &mut Client,
        plan: &Plan,
        token: &str,
        recovery_fence: Option<i64>,
    ) -> Result<Protection> {
        valid_token(token)?;
        let tx = sql.transaction().await?;
        tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
        shared(&tx, self).await?;
        tx.query_one(
            &format!(
                "SELECT query_id FROM {}.pgderive_queries WHERE query_id=$1 FOR SHARE",
                self.schema
            ),
            &[&self.query],
        )
        .await?;
        let checkpoint = crate::catalog::read::boundary(&tx, self, plan)
            .await?
            .context("no checkpoint to protect")?;
        let roots = checkpoint
            .checkpoint
            .arrangements
            .iter()
            .flat_map(|member| member.trace.objects.clone())
            .collect::<Vec<_>>();
        let fence = tx
            .query_opt(
                &format!("SELECT fence FROM {}.pgderive_progress WHERE query_id=$1", self.schema),
                &[&self.query],
            )
            .await?
            .map(|row| row.try_get::<_, i64>(0))
            .transpose()?;
        if let Some(expected) = recovery_fence {
            ensure!(fence == Some(expected), "recovery protection writer fenced");
        }
        let fence = fence.unwrap_or(0);
        let recovering = recovery_fence.is_some();
        tx.execute(&format!("INSERT INTO {0}.pgderive_protections(token,query_id,owner_fence,uploading,recovering,roots) VALUES($1,$2,$3,false,$4,$5)",self.schema),&[&token,&self.query,&fence,&recovering,&serde_json::to_value(&roots)?]).await?;
        tx.commit()
            .await
            .context("protection COMMIT requires authoritative inspection on failure")?;
        Ok(Protection {
            catalog: self.clone(),
            token: token.into(),
            uploading: false,
            roots,
            namespace: None,
            stored: Some(checkpoint),
        })
    }
}
async fn insert(
    sql: &mut Client,
    catalog: &Catalog,
    token: &str,
    fence: i64,
) -> Result<Protection> {
    valid_token(token)?;
    let tx = sql.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
    lock(&tx, catalog, true).await?;
    let current = tx
        .query_opt(
            &format!(
                "SELECT fence FROM {}.pgderive_progress WHERE query_id=$1 FOR SHARE",
                catalog.schema
            ),
            &[&catalog.query],
        )
        .await?
        .map(|row| row.try_get::<_, i64>(0))
        .transpose()?;
    ensure!(current.unwrap_or(0) == fence, "upload reservation writer fenced");
    let roots = Vec::<ObjectRef>::new();
    let row = tx.query_one(&format!("INSERT INTO {0}.pgderive_protections(token,query_id,owner_fence,uploading,roots) VALUES($1,$2,$3,true,$4) RETURNING id",catalog.schema),&[&token,&catalog.query,&fence,&serde_json::to_value(&roots)?]).await?;
    let id: i64 = row.try_get(0)?;
    let identity = tx.query_one("SELECT s.system_identifier::text,c.timeline_id::bigint,(SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=current_database()),pg_catalog.gen_random_uuid()::text FROM pg_catalog.pg_control_system() s CROSS JOIN pg_catalog.pg_control_checkpoint() c",&[]).await?;
    let scope = format!("{:x}", Sha256::digest(catalog.schema.as_bytes()));
    let namespace = format!(
        "pgderive/upload-v1/{}-{}-{}-{}-{id}-{}",
        identity.try_get::<_, String>(0)?,
        identity.try_get::<_, i64>(1)?,
        identity.try_get::<_, i64>(2)?,
        &scope[..16],
        identity.try_get::<_, String>(3)?
    );
    tx.execute(
        &format!("UPDATE {}.pgderive_protections SET namespace=$2 WHERE token=$1", catalog.schema),
        &[&token, &namespace],
    )
    .await?;
    tx.commit()
        .await
        .context("upload reservation COMMIT requires authoritative inspection on failure")?;
    Ok(Protection {
        catalog: catalog.clone(),
        token: token.into(),
        uploading: true,
        roots,
        namespace: Some(namespace),
        stored: None,
    })
}
fn valid_token(token: &str) -> Result<()> {
    ensure!(!token.is_empty() && token.len() <= 200, "invalid storage protection token");
    Ok(())
}
impl Protection {
    /// Unique physical namespace writable only while this upload reservation is active.
    /// Never retain it for another upload after closing the reservation.
    ///
    /// # Errors
    /// Rejects reader protections, which do not authorize object writes.
    pub fn namespace(&self) -> Result<&str> {
        self.namespace.as_deref().context("reader protection has no writable namespace")
    }
    /// Exact authoritative boundary captured under this reader protection.
    /// Restore from this checkpoint rather than loading another generation.
    ///
    /// # Errors
    /// Rejects upload reservations, which do not pin a readable checkpoint.
    pub fn stored(&self) -> Result<&crate::catalog::Stored> {
        self.stored.as_ref().context("upload reservation has no pinned checkpoint")
    }
    /// Exact coarse root membership pinned by a reader/recovery protection.
    #[must_use]
    pub fn roots(&self) -> &[ObjectRef] {
        &self.roots
    }
    /// Release an upload only after exact candidate publication is confirmed.
    ///
    /// # Errors
    /// Rejects a different query, uncertain/stale writer or nonmatching checkpoint.
    pub async fn published(
        self,
        sql: &mut Client,
        writer: &Writer,
        plan: &Plan,
        checkpoint: &Checkpoint,
    ) -> Result<()> {
        ensure!(
            self.uploading
                && self.catalog.schema == writer.catalog.schema
                && self.catalog.query == writer.catalog.query,
            "upload publication owner mismatch"
        );
        let durable = writer.confirmed(sql, plan).await?;
        ensure!(durable.stored.checkpoint == *checkpoint, "upload publication candidate mismatch");
        self.remove(sql).await
    }
    /// Release reader protection only after every dependent reader has finished.
    ///
    /// # Errors
    /// Rejects an upload reservation or changed/missing protection.
    pub async fn reader_finished(self, sql: &mut Client) -> Result<()> {
        ensure!(!self.uploading, "unresolved upload cannot be released as a reader");
        self.remove(sql).await
    }
    async fn remove(self, sql: &mut Client) -> Result<()> {
        let tx = sql.transaction().await?;
        tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
        shared(&tx, &self.catalog).await?;
        let affected = tx.execute(&format!("UPDATE {}.pgderive_protections SET active=false WHERE token=$1 AND query_id=$2 AND uploading=$3 AND roots=$4 AND active",self.catalog.schema),&[&self.token,&self.catalog.query,&self.uploading,&serde_json::to_value(&self.roots)?]).await?;
        ensure!(affected == 1, "storage protection changed or disappeared");
        tx.commit().await.context("protection release COMMIT outcome uncertain")?;
        Ok(())
    }
}

impl Writer {
    /// Reserve an upload using this exact writer's fence before any PUT.
    /// Keep the reservation active through authoritative publication resolution.
    ///
    /// # Errors
    /// Rejects uncertain/stale writers, reused tokens or database failures.
    pub async fn protect_upload(&self, sql: &mut Client, token: &str) -> Result<Protection> {
        ensure!(!self.uncertain, "uncertain writer cannot reserve uploads");
        insert(sql, &self.catalog, token, self.fence).await
    }
    // Private recovery pins may be invalidated by fencing. They never authorize
    // externally visible reads: the worker must re-confirm ownership/membership
    // after restoring, before opening CDC or exposing any recovered result.
    pub(crate) async fn protect_recovery(
        &self,
        sql: &mut Client,
        plan: &Plan,
        token: &str,
    ) -> Result<Protection> {
        self.confirmed(sql, plan).await?;
        self.catalog.pin_checkpoint(sql, plan, token, Some(self.fence)).await
    }
    pub(crate) async fn seal_fenced_recovery(&self, sql: &mut Client, plan: &Plan) -> Result<u64> {
        self.seal(sql, plan, true).await
    }
    /// Seal abandoned uploads belonging to earlier fenced writers of this query.
    /// The new writer must first confirm its authoritative boundary. Older writers
    /// cannot publish these namespaces again; late PUTs remain collectible orphans.
    /// Reader/recovery protections never expire here.
    ///
    /// # Errors
    /// Rejects uncertain/stale ownership or database failures.
    pub async fn seal_fenced_uploads(&self, sql: &mut Client, plan: &Plan) -> Result<u64> {
        self.seal(sql, plan, false).await
    }
    async fn seal(&self, sql: &mut Client, plan: &Plan, recovery: bool) -> Result<u64> {
        self.confirmed(sql, plan).await?;
        let tx = sql.transaction().await?;
        tx.batch_execute("SET LOCAL synchronous_commit=on").await?;
        shared(&tx, &self.catalog).await?;
        let row = tx
            .query_one(
                &format!(
                    "SELECT fence FROM {}.pgderive_progress WHERE query_id=$1 FOR SHARE",
                    self.catalog.schema
                ),
                &[&self.catalog.query],
            )
            .await?;
        ensure!(row.try_get::<_, i64>(0)? == self.fence, "upload recovery writer fenced");
        let sealed = tx.execute(
            &format!("UPDATE {}.pgderive_protections SET active=false WHERE query_id=$1 AND ((uploading AND NOT $3) OR (recovering AND $3)) AND active AND owner_fence<$2", self.catalog.schema),
            &[&self.catalog.query, &self.fence, &recovery],
        ).await?;
        tx.commit().await.context("upload recovery COMMIT outcome uncertain")?;
        Ok(sealed)
    }
}
