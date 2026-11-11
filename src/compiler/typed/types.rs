//! Deliberately bounded native coercions; unsupported overloads never guess.
use super::{ColumnRef, Name, Typed, Value};
use anyhow::{Context, Result, ensure};
use pg_query::protobuf as pg;

use crate::{numeric::Typmod, temporal::Precision};

const VARCHAR_HEADER: i32 = 4;
const MAX_VARCHAR_LENGTH: i32 = 10 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(untagged)]
pub(super) enum Modifier {
    Numeric(Typmod),
    Varchar(i32),
    Timestamp(Precision),
    Interval { precision: Precision, relabel: bool },
}

impl Modifier {
    pub(super) fn requires_coercion(self, input: &Typed, output: u32) -> bool {
        let prior = if input.oid == output { input.bound_modifier() } else { -1 };
        match self {
            Self::Numeric(_) => true,
            Self::Varchar(_) => prior <= VARCHAR_HEADER || prior > self.encoded(),
            Self::Timestamp(precision) => precision.needs_rounding(prior),
            Self::Interval { precision, relabel } => {
                // An exact input typmod omits the cast during binding. For
                // other constant inputs PostgreSQL evaluates scale before its
                // planner support function can eliminate redundant rounding.
                !relabel && (input.is_constant() || precision.needs_interval_rounding(prior))
            }
        }
    }

    pub(super) fn encoded(self) -> i32 {
        match self {
            Self::Numeric(value) => value.encoded(),
            Self::Varchar(length) => VARCHAR_HEADER + length,
            Self::Timestamp(value) => value.encoded(),
            Self::Interval { precision, .. } => precision.interval_modifier(),
        }
    }
}

pub(in crate::compiler) fn common(types: &[u32]) -> Result<u32> {
    let known = types.iter().copied().filter(|oid| !matches!(oid, 0 | 705)).collect::<Vec<_>>();
    if let Some(first) = known.first()
        && matches!(first, 16 | 2950 | 1082 | 1114 | 1184 | 1186)
        && known.iter().all(|oid| oid == first)
    {
        ensure!(
            types.iter().all(|oid| *oid == 0 || oid == first),
            "native CASE/COALESCE requires typed values instead of unknown string results"
        );
        return Ok(*first);
    }
    if known.is_empty() || known.iter().all(|oid| matches!(oid, 25 | 1043)) {
        // Unknown literals do not change a resolved varchar candidate.
        return Ok(if known.iter().all(|oid| *oid == 1043) && !known.is_empty() {
            1043
        } else {
            25
        });
    }
    ensure!(
        types.iter().all(|oid| matches!(oid, 0 | 20 | 21 | 23 | 1700)),
        "mixed scalar result types are not available in-engine"
    );
    Ok(numeric_common(types))
}

pub(super) fn cast(
    cast: &pg::TypeCast,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    depth: usize,
) -> Result<Typed> {
    use crate::compiler::parser::expressions::{builtin_name, names, optional};
    let kind = cast.type_name.as_ref().context("missing scalar cast type")?;
    ensure!(
        kind.array_bounds.is_empty() && !kind.setof && !kind.pct_type,
        "unsupported scalar cast modifier"
    );
    let name = names(&kind.names)?.0;
    let oid = match builtin_name(&name).context("unsupported scalar cast namespace")? {
        "int2" => 21,
        "int4" => 23,
        "int8" => 20,
        "bool" => 16,
        "uuid" => 2950,
        "numeric" => 1700,
        "text" => 25,
        "varchar" => 1043,
        "date" => 1082,
        "timestamp" => 1114,
        "timestamptz" => 1184,
        "interval" => 1186,
        _ => anyhow::bail!("unsupported scalar cast"),
    };
    let typmod = modifier(kind, oid)?;
    let argument = optional(cast.arg.as_deref())?;
    let input = if oid == 1700 {
        super::numeric_cast_input(argument, resolve, depth + 1)?
    } else {
        super::bind(argument, resolve, depth + 1)?
    };
    // PostgreSQL keeps the original expression when both its type and modifier match.
    if input.oid == oid && unchanged_modifier(&input, typmod) {
        return Ok(input);
    }
    if matches!(oid, 1082 | 1114 | 1184 | 1186) {
        return temporal_cast(input, oid, typmod);
    }
    if matches!(oid, 20 | 21 | 23) && input.oid == 705 {
        return Typed::contextual_literal(input, oid);
    }
    if matches!(oid, 16 | 2950) && matches!(input.oid, 0 | 705) {
        return Typed::contextual_literal(input, oid);
    }
    let valid = match oid {
        16 | 2950 => input.oid == oid,
        25 => matches!(input.oid, 0 | 16 | 20 | 21 | 23 | 25 | 705 | 1043 | 2950),
        1043 => matches!(input.oid, 0 | 25 | 1043 | 705),
        _ => matches!(input.oid, 0 | 20 | 21 | 23 | 1700),
    };
    ensure!(valid, "scalar cast from OID {} to OID {oid} is not available in-engine", input.oid);
    Ok(Typed { oid, value: Value::Cast { input: Box::new(input), typmod } })
}

fn unchanged_modifier(input: &Typed, typmod: Option<Modifier>) -> bool {
    match &input.value {
        Value::Column(column) => column.modifier == typmod.map(Modifier::encoded),
        _ => typmod.is_none() && input.bound_modifier() == -1,
    }
}

fn temporal_cast(input: Typed, oid: u32, typmod: Option<Modifier>) -> Result<Typed> {
    let literal = matches!(input.oid, 0 | 705);
    let input = if literal {
        let input = Typed::contextual_literal(input, oid)?;
        if typmod.is_none() {
            return Ok(input);
        }
        input
    } else {
        ensure!(
            input.oid == oid
                || (matches!(input.oid, 1082 | 1114 | 1184) && matches!(oid, 1082 | 1114 | 1184)),
            "unsupported temporal cast"
        );
        input
    };
    let typmod = typmod.map(|modifier| match modifier {
        Modifier::Interval { precision, .. } => Modifier::Interval {
            precision,
            relabel: input.bound_modifier() == precision.interval_modifier(),
        },
        modifier => modifier,
    });
    let value = Typed { oid, value: Value::Cast { input: Box::new(input), typmod } };
    if literal && oid == 1186 {
        // PostgreSQL passes the modifier directly to interval input during binding.
        value.evaluate(&crate::transaction::Row::new())?;
    }
    Ok(value)
}

pub(super) fn numeric_common(types: &[u32]) -> u32 {
    [1700, 20, 23, 21].into_iter().find(|oid| types.contains(oid)).unwrap_or(23)
}

fn modifier(kind: &pg::TypeName, oid: u32) -> Result<Option<Modifier>> {
    if kind.typmods.is_empty() {
        return Ok(None);
    }
    ensure!(kind.typmods.len() <= 2, "unsupported scalar cast modifier");
    let values = kind
        .typmods
        .iter()
        .map(|node| {
            super::native_literal(node)?
                .as_integer_literal()
                .context("cast modifier requires integer constants")
        })
        .collect::<Result<Vec<_>>>()?;
    let modifier = match (oid, values.as_slice()) {
        (1700, [precision]) => Modifier::Numeric(Typmod::new(*precision, 0)?),
        (1700, [precision, scale]) => Modifier::Numeric(Typmod::new(*precision, *scale)?),
        (1043, [length]) => {
            let length = i32::try_from(*length)?;
            ensure!((1..=MAX_VARCHAR_LENGTH).contains(&length), "varchar length out of range");
            Modifier::Varchar(length)
        }
        (1114 | 1184, [digits]) => Modifier::Timestamp(Precision::new(*digits)?),
        (1186, [range, digits]) if *range == i64::from(Precision::INTERVAL_FULL_RANGE) => {
            Modifier::Interval { precision: Precision::new(*digits)?, relabel: false }
        }
        _ => anyhow::bail!("unsupported scalar cast modifier"),
    };
    Ok(Some(modifier))
}
