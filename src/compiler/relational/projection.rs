//! Initialization-only projection over the canonical resolved expression schema.
use super::Relational;
use anyhow::Result;
use std::collections::BTreeSet;
impl Relational {
    pub(crate) fn bind_projection(&mut self, contract: &crate::source::Contract) -> Result<()> {
        let mut names = BTreeSet::new();
        collect(&serde_json::to_value(&*self)?, &mut names);
        self.projection = self
            .sources
            .iter()
            .map(|source| {
                let relation = contract
                    .relations
                    .iter()
                    .find(|r| r.schema == source.schema && r.table == source.table)
                    .ok_or_else(|| anyhow::anyhow!("missing projected source"))?;
                Ok(relation
                    .columns
                    .iter()
                    .filter(|c| c.primary || names.contains(&c.name))
                    .map(|c| c.name.clone())
                    .collect())
            })
            .collect::<Result<_>>()?;
        Ok(())
    }
    pub(crate) fn source_columns(
        &self,
        contract: &crate::source::Contract,
    ) -> Vec<BTreeSet<String>> {
        contract
            .relations
            .iter()
            .map(|relation| {
                self.sources
                    .iter()
                    .zip(&self.projection)
                    .filter(|(s, _)| s.schema == relation.schema && s.table == relation.table)
                    .flat_map(|(_, columns)| columns.iter().cloned())
                    .collect()
            })
            .collect()
    }
}
fn collect(value: &serde_json::Value, names: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::Object(fields) => {
            // ColumnRef is shared by every expression family. Reading its
            // canonical schema avoids a second, incomplete expression visitor.
            if fields.get("right").is_some_and(serde_json::Value::is_boolean)
                && fields.get("oid").is_some_and(serde_json::Value::is_number)
                && let Some(name) = fields.get("name").and_then(serde_json::Value::as_str)
            {
                let name = name
                    .split_once(':')
                    .filter(|(prefix, _)| prefix.parse::<usize>().is_ok())
                    .map_or(name, |(_, name)| name);
                names.insert(name.to_owned());
            }
            for value in fields.values() {
                collect(value, names);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect(value, names);
            }
        }
        _ => {}
    }
}
