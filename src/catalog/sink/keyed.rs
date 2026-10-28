//! Same-key replacements preserve key identity and emit native UPDATEs.
use super::{Sink, batching, key, layout, rows::quote};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tokio_postgres::Transaction;

#[derive(Default)]
struct Change<'a> {
    old: Option<&'a Value>,
    new: Option<&'a Value>,
}
fn changes<'a>(positions: &[usize], rows: &'a [(Value, i64)]) -> Result<Vec<Change<'a>>> {
    let mut keyed = BTreeMap::<String, Change<'a>>::new();
    for (tuple, weight) in rows.iter().filter(|(_, weight)| *weight != 0) {
        ensure!(matches!(*weight, -1 | 1), "keyed result requires unit multiplicity");
        let fields =
            tuple.get(1).and_then(Value::as_array).context("invalid keyed result tuple")?;
        let key = positions
            .iter()
            .map(|position| {
                let value = fields.get(*position).context("keyed result column missing")?;
                ensure!(!value.is_null(), "keyed result key is NULL");
                Ok(value)
            })
            .collect::<Result<Vec<_>>>()?;
        // The compiler key proof accepts only types whose canonical engine codec
        // agrees with PostgreSQL key equality (no collated text or numeric keys).
        let change = keyed.entry(serde_json::to_string(&key)?).or_default();
        let entry = if *weight < 0 { &mut change.old } else { &mut change.new };
        ensure!(entry.replace(tuple).is_none(), "multiple result tuples share a sink key");
    }
    Ok(keyed.into_values().collect())
}
pub(super) async fn apply(
    tx: &Transaction<'_>,
    program: &super::program::Program,
    sink: &Sink,
    rows: &[(Value, i64)],
) -> Result<super::work::Mutations> {
    let changes = changes(&program.positions, rows)?;
    let mut unpaired = Vec::new();
    let mut replacements = Vec::new();
    for change in changes {
        match (change.old, change.new) {
            (Some(old), Some(new)) => replacements.push(json!([old, new])),
            (Some(old), None) => unpaired.push((old.clone(), -1)),
            (None, Some(new)) => unpaired.push((new.clone(), 1)),
            (None, None) => {}
        }
    }
    // Unpaired keys are disjoint from replacement keys. Preserve delete-before-insert
    // bag publication and the caller's single progress/publication transaction.
    let mut counts = batching::apply_rows(tx, program, sink, &unpaired, &Value::Null).await?;
    counts.updates = u64::try_from(replacements.len())?;
    let statement = program.update.as_ref().context("missing keyed UPDATE program")?;
    let mut chunk = batching::Chunk::default();
    for pair in replacements {
        let size = serde_json::to_vec(&pair)?.len().saturating_add(32);
        if chunk.full(size) {
            batching::execute(tx, statement, &mut chunk, sink.output, &Value::Null).await?;
        }
        chunk.push(&pair, 1, size)?;
    }
    batching::execute(tx, statement, &mut chunk, sink.output, &Value::Null).await?;
    Ok(counts)
}

pub(super) fn update_statement(table: &str, sink: &Sink) -> Result<String> {
    let old = layout::decode(sink, "(entry.value->0->0)")?;
    let new = layout::decode(sink, "(entry.value->0->1)")?;
    let layout = layout::Layout::new(sink)?;
    let assignments = sink
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| format!("{}=replacement.c{index}", quote(&column.name)))
        .collect::<Vec<_>>()
        .join(",");
    Ok(format!(
        "/* pgderive:sink-update-v1 */ WITH replacements AS MATERIALIZED (SELECT previous.representation,replacement.* FROM pg_catalog.jsonb_array_elements($1::jsonb) AS entry(value) CROSS JOIN LATERAL (SELECT {old}) original CROSS JOIN LATERAL (SELECT {} AS representation FROM (SELECT original.*) decoded) previous CROSS JOIN LATERAL (SELECT {new}) replacement), {} UPDATE ONLY {table} d SET {assignments} FROM selected replacement WHERE {}",
        layout.identity,
        selected(table, sink, ("replacements", "replacement"))?,
        selected_predicate(&layout, "replacement")
    ))
}

fn predicate(sink: &Sink, source: &str) -> Result<String> {
    key::positions(sink)?
        .into_iter()
        .map(|index| Ok(format!("d.{}={source}.c{index}", quote(&sink.columns[index].name))))
        .collect::<Result<Vec<_>>>()
        .map(|values| values.join(" AND "))
}

pub(super) fn delete_statement(
    table: &str,
    sink: &Sink,
    layout: &layout::Layout,
) -> Result<String> {
    Ok(format!(
        "/* pgderive:sink-delete-v1 */ WITH decoded AS MATERIALIZED (SELECT {} FROM pg_catalog.jsonb_array_elements($1::jsonb) AS entry(value)), requested AS MATERIALIZED (SELECT decoded.*,{} AS representation FROM decoded), {} DELETE FROM ONLY {table} d USING selected r WHERE {}",
        layout.decoded,
        layout.identity,
        selected(table, sink, ("requested", "r"))?,
        selected_predicate(layout, "r")
    ))
}

// OFFSET prevents flattening the key lookup into a whole-target hash scan.
// The TID array also bounds the UPDATE/DELETE target scan; joining selected
// retains O(batch) replacement lookup instead of a per-target CTE rescan.
fn selected(table: &str, sink: &Sink, source: (&str, &str)) -> Result<String> {
    let (relation, alias) = source;
    Ok(format!(
        "selected AS MATERIALIZED (SELECT hit.ctid,{alias}.* FROM {relation} {alias} CROSS JOIN LATERAL (SELECT d.ctid FROM ONLY {table} d WHERE {} OFFSET 0) hit)",
        predicate(sink, alias)?
    ))
}

fn selected_predicate(layout: &layout::Layout, source: &str) -> String {
    // Keep the exact old image at the modifying node for EvalPlanQual. A TID
    // identifies a candidate in this statement, never a durable row identity.
    // COALESCE preserves WHERE's NULL rejection without hashing every row image.
    format!(
        "d.ctid={source}.ctid AND d.ctid=ANY(ARRAY(SELECT ctid FROM selected)) AND COALESCE({}={source}.representation,false)",
        layout.target_identity
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ResultColumn, SinkOutput};
    fn sink() -> Sink {
        Sink {
            output: SinkOutput::Postgres,
            table: "result".into(),
            primary_key: vec!["key".into()],
            columns: ["key", "value"]
                .into_iter()
                .map(|name| ResultColumn {
                    name: name.into(),
                    oid: 23,
                    modifier: -1,
                    collation: None,
                })
                .collect(),
        }
    }
    #[test]
    fn pairs_across_input_order_and_rejects_invalid_multiplicity() -> Result<()> {
        let rows = vec![
            (json!([null, [1, 20]]), 1),
            (json!([null, [2, 30]]), 1),
            (json!([null, [1, 10]]), -1),
            (json!([null, [3, 40]]), -1),
        ];
        let pairs = changes(&key::positions(&sink())?, &rows)?;
        assert_eq!(pairs.len(), 3);
        assert!(pairs[0].old.is_some() && pairs[0].new.is_some());
        for invalid in [
            vec![(json!([null, [1, 20]]), 2)],
            vec![(json!([null, [null, 20]]), 1)],
            vec![(json!([null, [1, 20]]), 1), (json!([null, [1, 30]]), 1)],
        ] {
            assert!(changes(&key::positions(&sink())?, &invalid).is_err());
        }
        let sql = update_statement("owned.result", &sink())?;
        assert!(sql.contains("pgderive:sink-update-v1"));
        assert!(sql.contains("int4send(d.\"value\")"));
        assert!(sql.contains("replacement.representation"));
        assert!(sql.contains("d.\"key\"=replacement.c0"));
        assert!(!sql.contains("hash_record_extended"));
        let deletion = delete_statement("owned.result", &sink(), &layout::Layout::new(&sink())?)?;
        assert!(deletion.contains("d.\"key\"=r.c0"));
        assert!(deletion.contains("int4send(d.\"value\")"));
        assert!(deletion.contains("=r.representation"));
        assert!(!deletion.contains("hash_record_extended"));
        Ok(())
    }
    #[test]
    fn composite_keys_use_all_columns_and_validate_shape() -> Result<()> {
        let mut composite = sink();
        composite.primary_key = vec!["value".into(), "key".into()];
        let rows = vec![(json!([null, [1, 20]]), -1), (json!([null, [1, 30]]), 1)];
        let pairs = changes(&key::positions(&composite)?, &rows)?;
        assert_eq!(pairs.len(), 2);
        assert!(pairs.iter().all(|pair| pair.old.is_none() || pair.new.is_none()));
        let deletion =
            delete_statement("owned.result", &composite, &layout::Layout::new(&composite)?)?;
        assert!(deletion.contains("d.\"value\"=r.c1 AND d.\"key\"=r.c0"));
        assert!(changes(&key::positions(&composite)?, &[(json!([null, [1]]), 1)]).is_err());
        assert!(changes(&key::positions(&composite)?, &[(json!(null), 1)]).is_err());
        composite.primary_key = vec!["missing".into()];
        assert!(key::positions(&composite).is_err());
        assert!(changes(&key::positions(&sink())?, &[(json!(null), 0)])?.is_empty());
        Ok(())
    }
}
