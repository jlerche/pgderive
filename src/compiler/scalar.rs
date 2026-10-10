//! Typed pure native scalar expressions evaluated before relational state.
use super::ColumnRef;
use anyhow::{Context, Result, ensure};
use serde::Serialize;
#[derive(Clone, Serialize)]
pub struct DateBin {
    pub(crate) input: ColumnRef,
    pub(crate) stride: i64,
    pub(crate) origin: crate::temporal::Timestamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) shift: Option<(ColumnRef, i64)>,
}
#[derive(Clone, Serialize)]
pub struct Computed {
    pub(crate) column: ColumnRef,
    pub(crate) expression: Native,
}
#[derive(Clone, Serialize)]
#[serde(untagged)]
pub enum Native {
    Bin(DateBin),
    Case { case: super::case::Case },
    Offset { offset: Offset },
}
impl Native {
    pub(crate) fn evaluate(&self, row: &crate::transaction::Row) -> Result<Option<String>> {
        match self {
            Self::Bin(value) => value.evaluate(row),
            Self::Case { case } => case.evaluate(row),
            Self::Offset { offset } => offset.evaluate(row),
        }
    }
    pub(super) const fn shift(&self) -> Option<&(ColumnRef, i64)> {
        match self {
            Self::Bin(value) => value.shift.as_ref(),
            Self::Case { .. } | Self::Offset { .. } => None,
        }
    }
}
impl DateBin {
    pub(crate) fn evaluate(&self, row: &crate::transaction::Row) -> Result<Option<String>> {
        row.get(&self.input.name)
            .context("missing date_bin input")?
            .as_deref()
            .map(|value| {
                let value = crate::temporal::Timestamp::parse(value, self.input.oid)?;
                let value = value.bin(self.stride, self.origin)?;
                let value = if let Some((index, duration)) = &self.shift {
                    let index = row
                        .get(&index.name)
                        .and_then(Option::as_deref)
                        .context("missing hopping ordinal")?
                        .parse::<i64>()?;
                    value.subtract_duration(
                        index.checked_mul(*duration).context("hopping interval overflow")?,
                    )?
                } else {
                    value
                };
                value.text(self.input.oid)
            })
            .transpose()
    }
}
pub(super) fn bind(
    parsed: &super::parser::scalar::Bin,
    resolve: &impl Fn(&super::parser::Name) -> Result<ColumnRef>,
    computed: &mut Vec<Computed>,
    scope: Option<usize>,
) -> Result<ColumnRef> {
    use sha2::{Digest, Sha256};
    let input = resolve(&parsed.input)?;
    ensure!(input.oid == parsed.oid, "date_bin origin must match native timestamp type");
    let origin = crate::temporal::Timestamp::parse(&parsed.origin, parsed.oid)?;
    ensure!(
        origin.sort_value() != i64::MIN && origin.sort_value() != i64::MAX,
        "date_bin origin must be finite"
    );
    let shift = parsed
        .shift
        .as_ref()
        .map(|(name, duration)| -> Result<_> {
            let index = resolve(name)?;
            ensure!(
                index.name == super::expansion::FIELD,
                "hopping offset requires bounded generated ordinal"
            );
            Ok((index, *duration))
        })
        .transpose()?;
    let expression = DateBin { input: input.clone(), stride: parsed.stride, origin, shift };
    let prefix = scope.map_or_else(|| "@scalar_".into(), |scope| format!("@scalar_stage_{scope}_"));
    let name = format!("{prefix}{:x}", Sha256::digest(serde_json::to_vec(&expression)?));
    let column = ColumnRef { name, right: false, oid: input.oid, nullable: input.nullable };
    if !computed.iter().any(|prior| prior.column == column) {
        computed.push(Computed { column: column.clone(), expression: Native::Bin(expression) });
    }
    Ok(column)
}

pub(super) fn bind_case(
    parsed: &super::parser::case::Case,
    resolve: &impl Fn(&super::parser::Name) -> Result<ColumnRef>,
    computed: &mut Vec<Computed>,
    scope: Option<usize>,
) -> Result<ColumnRef> {
    use sha2::{Digest, Sha256};
    let (case, oid, nullable) = super::case::bind(parsed, resolve)?;
    let expression = Native::Case { case };
    let prefix = scope.map_or_else(|| "@case_".into(), |scope| format!("@case_stage_{scope}_"));
    let name = format!("{prefix}{:x}", Sha256::digest(serde_json::to_vec(&expression)?));
    let column = ColumnRef { name, right: false, oid, nullable };
    if !computed.iter().any(|prior| prior.column == column) {
        computed.push(Computed { column: column.clone(), expression });
    }
    Ok(column)
}

#[derive(Clone, Serialize)]
pub struct Offset {
    input: ColumnRef,
    subtract: i64,
}
impl Offset {
    fn evaluate(&self, row: &crate::transaction::Row) -> Result<Option<String>> {
        row.get(&self.input.name)
            .context("missing timestamp offset input")?
            .as_deref()
            .map(|text| {
                crate::temporal::Timestamp::parse(text, self.input.oid)?
                    .subtract_duration(self.subtract)?
                    .text(self.input.oid)
            })
            .transpose()
    }
}
pub(super) fn bind_offset(
    parsed: &super::parser::offset::Offset,
    resolve: &impl Fn(&super::parser::Name) -> Result<ColumnRef>,
    computed: &mut Vec<Computed>,
    scope: Option<usize>,
) -> Result<ColumnRef> {
    use sha2::{Digest, Sha256};
    let input = resolve(&parsed.input)?;
    ensure!(matches!(input.oid, 1114 | 1184), "offset requires native timestamp input");
    let expression =
        Native::Offset { offset: Offset { input: input.clone(), subtract: parsed.subtract } };
    let prefix = scope.map_or_else(|| "@offset_".into(), |scope| format!("@offset_stage_{scope}_"));
    let name = format!("{prefix}{:x}", Sha256::digest(serde_json::to_vec(&expression)?));
    let column = ColumnRef { name, right: false, oid: input.oid, nullable: input.nullable };
    if !computed.iter().any(|prior| prior.column == column) {
        computed.push(Computed { column: column.clone(), expression });
    }
    Ok(column)
}
