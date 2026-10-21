//! Conservative output-key proof: complete non-NULL GROUP BY keys survive projection.
use super::{ColumnRef, Compiled, partition::Mode, relational::Node};
use anyhow::{Context, Result, ensure};

pub(super) fn prove(compiled: &Compiled, names: &[String]) -> Result<()> {
    if names.is_empty() {
        return Ok(());
    }
    let selected =
        names
            .iter()
            .map(|name| {
                let column =
                    compiled.output().columns.iter().find(|c| &c.label == name).with_context(
                        || format!("sink key column {name:?} is not an output column"),
                    )?;
                ensure!(!column.column.nullable, "sink key column {name:?} may be NULL");
                // SQL equality must agree with the grouping codec; collated/text and numeric
                // keys need a separate proof before accepting them as primary-key identity.
                ensure!(
                    matches!(column.column.oid, 16 | 20 | 21 | 23 | 2950 | 1114 | 1184),
                    "sink key column {name:?} has an unsupported key type"
                );
                Ok(column.column.clone())
            })
            .collect::<Result<Vec<_>>>()?;
    let mut edge = match compiled.relational.nodes.last() {
        Some(Node::Output { input, .. }) => input.as_str(),
        _ => anyhow::bail!("sink key uniqueness cannot be proven for this output"),
    };
    for node in compiled.relational.nodes.iter().rev() {
        match node {
            Node::Filter { id, input, .. } | Node::Output { id, input } if id == edge => {
                edge = input;
            }
            Node::Map { id, input, computed } if id == edge => {
                ensure!(
                    computed.iter().all(|value| !selected.contains(&value.column)),
                    "sink key uniqueness is not proven through a transformed key"
                );
                edge = input;
            }
            Node::Finalize { id, spec, .. }
            | Node::OrderedFinalize { id, spec, .. }
            | Node::Partition { id, spec, .. }
                if id == edge =>
            {
                if let Mode::Grouped { keys } = &spec.mode {
                    return complete(keys, &selected);
                }
                break;
            }
            _ => {}
        }
    }
    anyhow::bail!(
        "sink key uniqueness is not proven: requires projected complete non-NULL GROUP BY keys"
    )
}
fn complete(keys: &[ColumnRef], selected: &[ColumnRef]) -> Result<()> {
    ensure!(
        !keys.is_empty() && keys.iter().all(|key| selected.contains(key)),
        "sink key must include every GROUP BY column"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{Column, Contract, Identity, Relation};
    fn contract() -> Contract {
        Contract {
            environment: crate::source::Environment::fixture(),
            identity: Identity { system: "1".into(), timeline: 1, database: "db".into() },
            publication: "pub".into(),
            slot: "slot".into(),
            relations: vec![Relation {
                oid: 1,
                schema: "public".into(),
                table: "items".into(),
                columns: [("id", false), ("category", false), ("optional", true)]
                    .into_iter()
                    .zip(1_i16..)
                    .map(|((name, nullable), position)| Column {
                        position,
                        name: name.into(),
                        oid: 23,
                        modifier: -1,
                        nullable,
                        primary: position == 1,
                        collation: 0,
                    })
                    .collect(),
            }],
        }
    }
    #[test]
    fn proves_grouped_keys_and_rejects_incomplete_nullable_or_unproven_keys() -> Result<()> {
        let native = contract();
        let compiled = crate::compiler::compile(
            "SELECT id AS key, count(*) AS n FROM public.items GROUP BY id",
            &native,
        )?;
        prove(&compiled, &["key".into()])?;
        prove(&compiled, &[])?;
        let average = crate::compiler::compile(
            "SELECT id,AVG(category) FROM public.items GROUP BY id",
            &native,
        )?;
        prove(&average, &["id".into()])?;
        assert!(prove(&compiled, &["missing".into()]).is_err());
        assert!(prove(&compiled, &["n".into()]).is_err());
        for (sql, key) in [
            ("SELECT optional, count(*) FROM public.items GROUP BY optional", "optional"),
            ("SELECT id FROM public.items", "id"),
            ("SELECT id,category,count(*) FROM public.items GROUP BY id,category", "id"),
        ] {
            let compiled = crate::compiler::compile(sql, &native)?;
            assert!(prove(&compiled, &[key.into()]).is_err(), "{sql}");
        }
        let composite = crate::compiler::compile(
            "SELECT id,category,count(*) FROM public.items GROUP BY id,category",
            &native,
        )?;
        prove(&composite, &["category".into(), "id".into()])?;
        Ok(())
    }
}
