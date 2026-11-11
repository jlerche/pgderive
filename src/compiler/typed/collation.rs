//! Collation derivation for collatable scalar expressions.
use super::{ColumnRef, Name, Typed, Value};
use anyhow::{Result, ensure};
use pg_query::protobuf as pg;

pub(super) fn bind(
    clause: &pg::CollateClause,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    use crate::compiler::parser::expressions::{builtin_name, names, optional};
    let name = names(&clause.collname)?;
    let collation = match builtin_name(&name.0) {
        Some("C") => 950,
        Some("POSIX") => 951,
        _ => anyhow::bail!("explicit text collation must be pg_catalog.C or pg_catalog.POSIX"),
    };
    let mut input = super::bind(optional(clause.arg.as_deref())?, resolve, depth + 1)?;
    if input.oid == 0 {
        input = Typed::null_text();
    }
    let input = input.unknown_text();
    ensure!(matches!(input.oid, 25 | 1043), "COLLATE requires a text or varchar operand");
    Ok(Typed { oid: input.oid, value: Value::Collate { input: Box::new(input), collation } })
}

#[derive(Clone, Copy)]
pub(in crate::compiler) enum Derivation {
    Implicit(u32),
    Explicit(u32),
    Indeterminate,
}

impl Derivation {
    pub(in crate::compiler) fn merge(self, other: Self) -> Result<Self> {
        Ok(match (self, other) {
            (Self::Explicit(left), Self::Explicit(right)) => {
                ensure!(left == right, "conflicting explicit text collations");
                Self::Explicit(left)
            }
            (Self::Explicit(oid), _) | (_, Self::Explicit(oid)) => Self::Explicit(oid),
            (Self::Implicit(left), Self::Implicit(right)) => {
                common(left, right).map_or(Self::Indeterminate, Self::Implicit)
            }
            _ => Self::Indeterminate,
        })
    }

    pub(in crate::compiler) fn oid(self) -> Result<u32> {
        match self {
            Self::Implicit(oid) | Self::Explicit(oid) => Ok(oid),
            Self::Indeterminate => anyhow::bail!("unresolved or conflicting text collations"),
        }
    }
}

impl Typed {
    pub(in crate::compiler) fn collation(
        &self,
        resolve: &impl Fn(&ColumnRef) -> Result<u32>,
    ) -> Result<u32> {
        self.derive_collation(resolve)?.oid()
    }

    pub(in crate::compiler) fn join_collation(
        &self,
        resolve: &impl Fn(&ColumnRef) -> Result<u32>,
    ) -> Result<u32> {
        let collation = self.derive_collation(resolve)?;
        ensure!(
            !matches!(collation, Derivation::Explicit(_)),
            "explicit COLLATE in ordinary join comparisons is unsupported"
        );
        collation.oid()
    }

    pub(in crate::compiler) fn derive_collation(
        &self,
        resolve: &impl Fn(&ColumnRef) -> Result<u32>,
    ) -> Result<Derivation> {
        use Derivation::{Explicit, Implicit, Indeterminate};
        let collatable = matches!(self.oid, 25 | 1043 | 705);
        if let Value::Call(call) = &self.value {
            let derived = collations(call.arguments.iter(), resolve)?;
            return Ok(if collatable { derived } else { Implicit(0) });
        }
        if !collatable {
            return Ok(Implicit(0));
        }
        match &self.value {
            Value::Column(column) => match resolve(column)? {
                0 => Ok(Indeterminate),
                collation => Ok(Implicit(collation)),
            },
            Value::Collate { input, collation } => {
                input.derive_collation(resolve)?;
                Ok(Explicit(*collation))
            }
            Value::Cast { input, .. } => {
                let derived = input.derive_collation(resolve)?;
                Ok(if matches!(input.oid, 25 | 1043 | 705) { derived } else { Implicit(100) })
            }
            Value::Coalesce(values) => collations(values.iter(), resolve),
            Value::NullIf { left, right } | Value::Concat { left, right } => {
                collations([left.as_ref(), right.as_ref()], resolve)
            }
            Value::SimpleCase(case) => collations(case.results(), resolve),
            Value::Case { branches, otherwise } => collations(
                std::iter::once(otherwise.as_ref()).chain(branches.iter().map(|(_, value)| value)),
                resolve,
            ),
            _ => Ok(Implicit(100)),
        }
    }
}
fn collations<'a>(
    values: impl IntoIterator<Item = &'a Typed>,
    resolve: &impl Fn(&ColumnRef) -> Result<u32>,
) -> Result<Derivation> {
    values.into_iter().try_fold(Derivation::Implicit(100), |result, value| {
        result.merge(value.derive_collation(resolve)?)
    })
}
pub(in crate::compiler) fn common(left: u32, right: u32) -> Result<u32> {
    ensure!(
        left == right || matches!(left, 0 | 100) || matches!(right, 0 | 100),
        "conflicting join text collations"
    );
    Ok(if right == 0 {
        left
    } else if matches!(left, 0 | 100) {
        right
    } else {
        left
    })
}
