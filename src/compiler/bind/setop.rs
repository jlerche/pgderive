//! Positional native reconciliation and weighted SQL set multiplicities.
use super::derived;
use crate::{
    compiler::{
        ColumnRef, Compiled,
        parser::{
            Parsed,
            setop::{Reference, SetOperation},
        },
        projection::OutputColumn,
        relational::{Binding, Node, Relational},
        scalar,
        typed::Typed,
    },
    source::Contract,
};
use anyhow::{Context, Result, ensure};
use pg_query::{Node as Ast, NodeEnum, protobuf as pg};

fn unknown(parsed: &Parsed) -> Vec<Option<Ast>> {
    match parsed {
        Parsed::Ordered(value) => {
            let mut literals = unknown(&value.inner);
            literals.truncate(value.visible);
            for order in &value.order {
                if let Some(value) = literals.get_mut(order.index) {
                    *value = None;
                }
            }
            literals
        }
        Parsed::Renamed { inner, .. } | Parsed::With { inner, .. } => unknown(inner),
        Parsed::Partition(value) | Parsed::Membership { parsed: value, distinct: false } => value
            .targets
            .iter()
            .map(|target| {
                let crate::compiler::parser::partition::Value::Typed(value) = &target.value else {
                    return None;
                };
                unknown_literal(value).then(|| (**value).clone())
            })
            .collect(),
        _ => Vec::new(),
    }
}
fn unknown_literal(value: &Ast) -> bool {
    match &value.node {
        Some(NodeEnum::AConst(value)) => {
            value.isnull || matches!(value.val, Some(pg::a_const::Val::Sval(_)))
        }
        Some(NodeEnum::CollateClause(value)) => value.arg.as_deref().is_some_and(unknown_literal),
        _ => false,
    }
}

fn common(left: &ColumnRef, right: &ColumnRef, unknown: (bool, bool)) -> Result<u32> {
    let (a, b) = (if unknown.0 { 0 } else { left.oid }, if unknown.1 { 0 } else { right.oid });
    common_type(a, b).map(|oid| if oid == 0 { 25 } else { oid })
}

pub(super) fn common_type(a: u32, b: u32) -> Result<u32> {
    Ok(match (a, b) {
        (0, value) | (value, 0) => value,
        (a, b) if a == b => a,
        (20 | 21 | 23 | 1700, 20 | 21 | 23 | 1700) => [1700, 20, 23, 21]
            .into_iter()
            .find(|oid| *oid == a || *oid == b)
            .context("missing numeric set operation type")?,
        (25 | 1043, 25 | 1043) => a,
        _ => crate::compiler::typed::control::temporal_common(a, b)
            .context("set operation operands have incompatible native types")?,
    })
}
pub(super) fn bind(parsed: SetOperation, contract: &Contract) -> Result<Compiled> {
    let left_unknown = unknown(&parsed.left);
    let right_unknown = unknown(&parsed.right);
    let mut left = derived::scope((parsed.left, "left".into()), "union_left_", 0, contract)?.ir;
    let mut right = derived::scope(
        (parsed.right, "right".into()),
        "union_right_",
        left.sources.len(),
        contract,
    )?
    .ir;
    ensure!(
        left.output.columns.len() == right.output.columns.len(),
        "set operation operands have different widths"
    );
    let columns = left
        .output
        .columns
        .iter()
        .zip(&right.output.columns)
        .enumerate()
        .map(|(index, (a, b))| {
            output_column(
                index,
                a,
                b,
                (
                    left_unknown.get(index).is_some_and(Option::is_some),
                    right_unknown.get(index).is_some_and(Option::is_some),
                ),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let facts = properties(&left, &right, contract)?;
    if parsed.kind != crate::compiler::setop::Kind::Union || !parsed.all {
        domains(&left, &right, (&left_unknown, &right_unknown), &columns, &facts)?;
    }
    let left_edge = align(&mut left, &columns, &left_unknown, "union_left", &facts[0])?;
    let right_edge = align(&mut right, &columns, &right_unknown, "union_right", &facts[1])?;
    let width = columns.len();
    let mut ir = Relational {
        sources: left.sources,
        projection: Vec::new(),
        nodes: left.nodes,
        output: crate::compiler::Projected { columns },
    };
    ir.sources.extend(right.sources);
    ensure!(ir.sources.len() <= 16, "set operation source occurrence limit exceeded");
    ir.nodes.extend(right.nodes);
    let input = lower(&mut ir, (left_edge, right_edge), parsed.kind, parsed.all);
    ir.nodes.push(Node::Output { id: "project".into(), input });
    let order = ordering(parsed.order, &ir)?;
    super::ordering::apply(
        Compiled { relational: ir, result_columns: Vec::new() },
        super::ordering::Clause { order, visible: width, distinct: false, bounds: parsed.bounds },
        contract,
    )
}
fn output_column(
    index: usize,
    a: &OutputColumn,
    b: &OutputColumn,
    unknown: (bool, bool),
) -> Result<OutputColumn> {
    let oid = common(&a.column, &b.column, unknown)?;
    ensure!(
        matches!(oid, 16 | 20 | 21 | 23 | 25 | 1043 | 1082 | 1700 | 2950 | 1114 | 1184 | 1186),
        "unsupported set operation native type"
    );
    Ok(OutputColumn {
        label: a.label.clone(),
        column: ColumnRef {
            name: format!("@union_{index}"),
            oid,
            right: false,
            nullable: a.column.nullable || b.column.nullable,
            collation: if matches!(oid, 25 | 1043) {
                super::super::typed::collation::common(a.column.collation, b.column.collation)
                    .unwrap_or(0)
            } else {
                0
            },
            modifier: a.column.modifier.filter(|_| {
                a.column.oid == oid && b.column.oid == oid && a.column.modifier == b.column.modifier
            }),
        },
    })
}

fn align(
    ir: &mut Relational,
    columns: &[OutputColumn],
    unknown: &[Option<Ast>],
    prefix: &str,
    facts: &crate::compiler::properties::Properties,
) -> Result<String> {
    let input =
        ir.nodes.last().context("set operation branch has no visible edge")?.id().to_owned();
    let mut computed = Vec::new();
    let mut fields = Vec::new();
    for (index, (source, target)) in ir.output.columns.iter().zip(columns).enumerate() {
        let input = if let Some(Some(value)) = unknown.get(index) {
            scalar::register(
                Typed::union_literal(value, target.column.oid)?,
                &mut computed,
                &format!("@{prefix}_cast_"),
            )?
        } else if source.column.oid != target.column.oid
            || (source.column.oid == 1700
                && facts.modifier(&source.column).is_some_and(|modifier| modifier >= 4))
        {
            scalar::register(
                Typed::set_cast(
                    source.column.clone(),
                    target.column.oid,
                    facts.modifier(&source.column),
                )?,
                &mut computed,
                &format!("@{prefix}_cast_"),
            )?
        } else {
            source.column.clone()
        };
        fields.push(Binding { input, output: target.column.clone() });
    }
    let mapped = if computed.is_empty() {
        input
    } else {
        let id = format!("{prefix}_casts");
        ir.nodes.push(Node::Map { id: id.clone(), input, computed });
        id
    };
    let id = format!("{prefix}_aligned");
    ir.nodes.push(Node::Rebind { id: id.clone(), input: mapped, fields });
    Ok(id)
}

fn ordering(
    order: Vec<crate::compiler::parser::setop::Order>,
    ir: &Relational,
) -> Result<Vec<crate::compiler::parser::ordering::Order>> {
    let order = order
        .into_iter()
        .map(|value| {
            let index = match value.reference {
                Reference::Ordinal(index) => {
                    ensure!(index < ir.output.columns.len(), "set ORDER BY ordinal exceeds width");
                    index
                }
                Reference::Label(label) => {
                    let mut matches =
                        ir.output.columns.iter().enumerate().filter(|(_, c)| c.label == label);
                    let first = matches.next().context("unknown set ORDER BY label")?.0;
                    ensure!(matches.next().is_none(), "ambiguous set ORDER BY label");
                    first
                }
            };
            Ok(crate::compiler::parser::ordering::Order {
                index,
                descending: value.descending,
                nulls_first: value.nulls_first,
                bytewise: false,
            })
        })
        .collect::<Result<_>>()?;
    Ok(order)
}

fn lower(
    ir: &mut Relational,
    edges: (String, String),
    kind: crate::compiler::setop::Kind,
    all: bool,
) -> String {
    let columns = ir.output.columns.iter().map(|column| column.column.clone()).collect::<Vec<_>>();
    let (mut left, mut right) = edges;
    if kind != crate::compiler::setop::Kind::Union {
        for (edge, side) in [(&mut left, true), (&mut right, false)] {
            let input = format!("{edge}_set_key");
            ir.nodes.push(Node::PartitionBy {
                id: input.clone(),
                input: edge.clone(),
                keys: columns.clone(),
            });
            let tagged = format!("{edge}_set_tag");
            ir.nodes.push(Node::SetTag { id: tagged.clone(), input, left: side });
            *edge = tagged;
        }
    }
    ir.nodes.push(Node::Union { id: "set_union".into(), left, right });
    if kind == crate::compiler::setop::Kind::Union {
        if all {
            return "set_union".into();
        }
        ir.nodes.push(Node::PartitionBy {
            id: "set_key".into(),
            input: "set_union".into(),
            keys: columns.clone(),
        });
        ir.nodes.push(Node::SetDistinct {
            id: "set_result".into(),
            input: "set_key".into(),
            columns,
        });
    } else {
        ir.nodes.push(Node::SetOperation {
            id: "set_result".into(),
            input: "set_union".into(),
            spec: crate::compiler::setop::Spec { kind, all, columns },
        });
    }
    "set_result".into()
}

fn properties(
    left: &Relational,
    right: &Relational,
    contract: &Contract,
) -> Result<[crate::compiler::properties::Properties; 2]> {
    let a = crate::compiler::properties::occurrence(left, contract)?;
    let mut context = right.clone();
    context.sources.splice(..0, left.sources.iter().cloned());
    context.nodes.splice(..0, left.nodes.iter().cloned());
    let b = crate::compiler::properties::occurrence(&context, contract)?;
    Ok([a, b])
}

type Literals = [Option<Ast>];
fn domains(
    left: &Relational,
    right: &Relational,
    unknown: (&Literals, &Literals),
    columns: &[OutputColumn],
    facts: &[crate::compiler::properties::Properties; 2],
) -> Result<()> {
    let [a, b] = facts;
    for (index, ((left, right), output)) in
        left.output.columns.iter().zip(&right.output.columns).zip(columns).enumerate()
    {
        ensure!(output.column.oid != 1186, "interval set operations require UNION ALL");
        if output.column.oid == 1700 {
            let left_null = null_literal(unknown.0.get(index).and_then(Option::as_ref));
            let right_null = null_literal(unknown.1.get(index).and_then(Option::as_ref));
            let scales = (
                numeric_scale(&left.column, a.modifier(&left.column)),
                numeric_scale(&right.column, b.modifier(&right.column)),
            );
            ensure!(
                (left_null || scales.0.is_some())
                    && (right_null || scales.1.is_some())
                    && (left_null || right_null || scales.0 == scales.1),
                "set numeric equality requires uniform display scales; cast each operand to numeric(p,s) with the same effective scale"
            );
        }
        if matches!(output.column.oid, 25 | 1043) {
            let (left, right) =
                (a.collation(&left.column).unwrap_or(0), b.collation(&right.column).unwrap_or(0));
            ensure!(left != 0 && right != 0, "set equality requires resolved text collations");
            crate::compiler::typed::collation::common(left, right)?;
        }
    }
    Ok(())
}
fn null_literal(value: Option<&Ast>) -> bool {
    matches!(value.and_then(|value| value.node.as_ref()), Some(NodeEnum::AConst(value)) if value.isnull)
}
fn numeric_scale(column: &ColumnRef, modifier: Option<i32>) -> Option<i32> {
    if matches!(column.oid, 20 | 21 | 23) {
        return Some(0);
    }
    let modifier = modifier.filter(|value| *value >= 4)? - 4;
    let scale = ((modifier & 0x7ff) ^ 0x400) - 0x400;
    Some(scale.max(0))
}
