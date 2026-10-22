//! Bounded occurrence DML chunks share the caller's atomic publication transaction.
use super::{Sink, layout::Layout};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use tokio_postgres::Transaction;
const IDENTITIES: usize = 512;
const BYTES: usize = 4 * 1024 * 1024;
#[derive(Default)]
pub(super) struct Chunk {
    values: Vec<Value>,
    bytes: usize,
    occurrences: u64,
}
impl Chunk {
    pub(super) const fn full(&self, bytes: usize) -> bool {
        !self.values.is_empty()
            && (self.values.len() >= IDENTITIES || self.bytes.saturating_add(bytes) > BYTES)
    }
    pub(super) fn push(&mut self, tuple: &Value, count: i64, bytes: usize) -> Result<()> {
        self.values.push(json!([tuple, count]));
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| anyhow::anyhow!("result batch byte overflow"))?;
        self.occurrences += u64::try_from(count)?;
        Ok(())
    }
}
pub(super) async fn apply(
    tx: &Transaction<'_>,
    table: &str,
    sink: &Sink,
    rows: &[(Value, i64)],
    context: &Value,
) -> Result<()> {
    let work =
        rows.iter().try_fold(0_u64, |total, (_, delta)| total.checked_add(delta.unsigned_abs()));
    ensure!(
        work.is_some_and(|count| count <= 1_000_000),
        "result occurrence publication limit exceeded (1000000)"
    );
    if !sink.primary_key.is_empty() {
        return super::keyed::apply(tx, table, sink, rows).await;
    }
    apply_rows(tx, table, sink, rows, context).await
}
pub(super) async fn apply_rows(
    tx: &Transaction<'_>,
    table: &str,
    sink: &Sink,
    rows: &[(Value, i64)],
    context: &Value,
) -> Result<()> {
    let layout = Layout::new(sink)?;
    for positive in [false, true] {
        let statement = statement(table, &layout, positive);
        let mut chunk = Chunk::default();
        for (tuple, delta) in
            rows.iter().filter(|(_, delta)| *delta != 0 && (*delta > 0) == positive)
        {
            let count =
                delta.checked_abs().ok_or_else(|| anyhow::anyhow!("result occurrence overflow"))?;
            let bytes = serde_json::to_vec(tuple)?.len().saturating_add(32);
            if chunk.full(bytes) {
                execute(tx, &statement, &mut chunk, sink.output, context).await?;
            }
            chunk.push(tuple, count, bytes)?;
        }
        execute(tx, &statement, &mut chunk, sink.output, context).await?;
    }
    Ok(())
}
pub(super) async fn execute(
    tx: &Transaction<'_>,
    statement: &str,
    chunk: &mut Chunk,
    output: super::SinkOutput,
    context: &Value,
) -> Result<()> {
    let chunk = std::mem::take(chunk);
    if chunk.values.is_empty() {
        return Ok(());
    }
    if output == super::SinkOutput::Stdout {
        return super::stdout::emit(
            context,
            statement,
            &Value::Array(chunk.values),
            chunk.occurrences,
        );
    }
    let affected = tx.execute(statement, &[&Value::Array(chunk.values)]).await?;
    ensure!(
        affected == chunk.occurrences,
        "result occurrence missing or destination DML suppressed"
    );
    Ok(())
}
fn statement(table: &str, layout: &Layout, positive: bool) -> String {
    let decoded = format!(
        "WITH decoded AS MATERIALIZED (SELECT {} FROM pg_catalog.jsonb_array_elements($1::jsonb) AS entry(value))",
        layout.decoded
    );
    if positive {
        return format!(
            "/* pgderive:sink-insert-v1 */ {decoded} INSERT INTO {table}({}) SELECT {} FROM decoded CROSS JOIN LATERAL pg_catalog.generate_series(1::bigint,decoded.n)",
            layout.names, layout.values
        );
    }
    format!(
        "/* pgderive:sink-delete-v1 */ {decoded}, keyed AS MATERIALIZED (SELECT {} AS representation,{} AS bucket,n FROM decoded), requested AS MATERIALIZED (SELECT representation,pg_catalog.min(bucket) AS bucket,pg_catalog.sum(n)::bigint AS n FROM keyed GROUP BY representation), selected AS MATERIALIZED (SELECT hit.ctid FROM requested r CROSS JOIN LATERAL (SELECT d.ctid FROM {table} d WHERE {}=r.bucket AND {}=r.representation LIMIT r.n FOR UPDATE) hit) DELETE FROM {table} d USING selected WHERE d.ctid=selected.ctid",
        layout.identity, layout.bucket, layout.target_bucket, layout.target_identity
    )
}

#[cfg(test)]
mod tests {
    use super::{Chunk, Layout, Sink, statement};
    use crate::catalog::ResultColumn;
    use anyhow::Result;
    #[test]
    fn batches_keep_weights_out_of_public_layout_and_bound_candidate_identity() -> Result<()> {
        let sink = Sink {
            primary_key: vec![],
            output: crate::catalog::SinkOutput::Postgres,
            table: "results".into(),
            columns: vec![ResultColumn {
                name: "x\" label".into(),
                oid: 1700,
                modifier: -1,
                collation: None,
            }],
        };
        let layout = Layout::new(&sink)?;
        let deletion = statement("owned.results", &layout, false);
        assert!(deletion.contains("GROUP BY representation"));
        assert!(deletion.contains("d.ctid=selected.ctid"));
        assert!(deletion.contains("numeric_send(d.\"x\"\" label\")"));
        assert!(deletion.contains("LIMIT r.n FOR UPDATE"));
        assert!(!statement("owned.results", &layout, true).contains("INSERT INTO owned.results(n"));
        let mut chunk = Chunk::default();
        chunk.push(&serde_json::json!([null, ["1.00"]]), 3, 32)?;
        assert_eq!(chunk.occurrences, 3);
        assert_eq!(chunk.values, vec![serde_json::json!([[null, ["1.00"]], 3])]);
        Ok(())
    }
}
