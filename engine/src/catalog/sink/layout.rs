//! Native storage decoding and exact representation keys for destination DML.
use super::{
    Sink,
    rows::{native, quote},
};
use anyhow::Result;
pub(super) struct Layout {
    pub decoded: String,
    pub values: String,
    pub names: String,
    pub identity: String,
    pub bucket: String,
    pub target_identity: String,
    pub target_bucket: String,
}
pub(super) fn names(sink: &Sink) -> Vec<String> {
    sink.columns.iter().map(|column| column.name.clone()).collect()
}
pub(super) fn hash(fields: &[String]) -> String {
    format!("pg_catalog.hash_record_extended(ROW({}),0)", fields.join(","))
}
impl Layout {
    pub(super) fn new(sink: &Sink) -> Result<Self> {
        let names = names(sink).iter().map(|name| quote(name)).collect::<Vec<_>>();
        let fields = (0..names.len()).map(|index| format!("c{index}")).collect::<Vec<_>>();
        let target = names.iter().map(|name| format!("d.{name}")).collect::<Vec<_>>();
        let decoded = decode(sink, "(entry.value->0)")?;
        Ok(Self {
            decoded: format!("{decoded},(entry.value->>1)::bigint AS n"),
            values: fields.join(","),
            names: names.join(","),
            identity: identity(sink, &fields)?,
            bucket: hash(&fields),
            target_identity: identity(sink, &target)?,
            target_bucket: hash(&target),
        })
    }
}
fn identity(sink: &Sink, fields: &[String]) -> Result<String> {
    let values = sink
        .columns
        .iter()
        .zip(fields)
        .map(|(column, field)| {
            let (_, send) = native(column.oid)?;
            Ok(format!("pg_catalog.{send}({field})"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(format!("pg_catalog.array_send(ARRAY[{}])", values.join(",")))
}

pub(super) fn decode(sink: &Sink, tuple: &str) -> Result<String> {
    sink.columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let mut value = column.value(tuple, index)?;
            if let Some((schema, name)) = &column.collation {
                value = format!("({value}) COLLATE {}.{}", quote(schema), quote(name));
            }
            Ok(format!("{value} AS c{index}"))
        })
        .collect::<Result<Vec<_>>>()
        .map(|values| values.join(","))
}
