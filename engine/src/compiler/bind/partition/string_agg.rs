//! Bind the text overload and its aggregate-local sort expressions.
use crate::compiler::{
    ColumnRef,
    parser::{Name, partition::StringAgg as Parsed},
    partition::{AggregateOrder, StringAgg},
    typed::Typed,
};
use anyhow::{Result, ensure};
use pg_query::Node;

pub(super) fn bind(
    parsed: &Parsed,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<StringAgg> {
    let value_input = text(&parsed.value, resolve)?;
    let delimiter_input = text(&parsed.delimiter, resolve)?;
    let value = value_input.clone().string_agg_text()?;
    let delimiter = delimiter_input.clone().string_agg_text()?;
    let column_collation = |column: &ColumnRef| Ok(column.collation);
    let derivation = value
        .derive_collation(&column_collation)?
        .merge(delimiter.derive_collation(&column_collation)?)?;
    let order = parsed
        .order
        .iter()
        .map(|(node, descending, nulls_first)| {
            let expression = Typed::bind_contextual(node, resolve, 0)?;
            if parsed.distinct {
                ensure!(
                    matches_distinct(&expression, &value, &value_input)
                        || matches_distinct(&expression, &delimiter, &delimiter_input),
                    "STRING_AGG DISTINCT ORDER BY expressions must appear in its arguments"
                );
            }
            let expression = if matches!(expression.oid(), 0 | 705) {
                expression.coerce_common(25)?
            } else {
                expression
            };
            validate_order(&expression)?;
            Ok(AggregateOrder { expression, descending: *descending, nulls_first: *nulls_first })
        })
        .collect::<Result<_>>()?;
    let collation = derivation.oid().unwrap_or(0);
    Ok(StringAgg { value, delimiter, order, distinct: parsed.distinct, collation })
}

fn text(node: &Node, resolve: &impl Fn(&Name) -> Result<ColumnRef>) -> Result<Typed> {
    let expression = Typed::bind_contextual(node, resolve, 0)?;
    ensure!(matches!(expression.oid(), 0 | 25 | 705 | 1043), "STRING_AGG requires text inputs");
    Ok(expression)
}

fn matches_distinct(order: &Typed, cast: &Typed, raw: &Typed) -> bool {
    if raw.oid() == 1043 && !raw.explicit_collation() { order == raw } else { order == cast }
}

fn validate_order(expression: &Typed) -> Result<()> {
    ensure!(
        matches!(
            expression.oid(),
            16 | 20 | 21 | 23 | 25 | 1043 | 1082 | 1114 | 1184 | 1186 | 1700 | 2950
        ),
        "unsupported STRING_AGG ORDER BY type"
    );
    if matches!(expression.oid(), 25 | 1043) && !expression.is_constant() {
        ensure!(
            matches!(expression.collation(&|column| Ok(column.collation))?, 950 | 951),
            "STRING_AGG text ordering requires C/POSIX collation"
        );
    }
    Ok(())
}
