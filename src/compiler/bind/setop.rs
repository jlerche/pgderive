//! Positional native reconciliation followed by the existing weighted Union edge.
use super::derived;
use crate::{
    compiler::{
        ColumnRef, Compiled,
        parser::{
            Parsed,
            setop::{Reference, Union},
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
            for order in &value.order { if let Some(value) = literals.get_mut(order.index) { *value = None; } }
            literals
        },
        Parsed::Renamed { inner, .. } | Parsed::With { inner, .. } => unknown(inner),
        Parsed::Partition(value) | Parsed::Membership { parsed: value, distinct:false } => value.targets.iter().map(|target| {
            let crate::compiler::parser::partition::Value::Typed(value) = &target.value else { return None };
            matches!(&value.node, Some(NodeEnum::AConst(value)) if value.isnull || matches!(value.val,Some(pg::a_const::Val::Sval(_)))).then(|| (**value).clone())
        }).collect(),
        _ => Vec::new(),
    }
}
fn common(left: &ColumnRef, right: &ColumnRef, unknown: (bool, bool)) -> Result<u32> {
    let (a, b) = (if unknown.0 { 0 } else { left.oid }, if unknown.1 { 0 } else { right.oid });
    Ok(match (a, b) {
        (0, 0) => 25,
        (0, value) | (value, 0) => value,
        (a, b) if a == b => a,
        (20 | 21 | 23 | 1700, 20 | 21 | 23 | 1700) => [1700, 20, 23, 21]
            .into_iter()
            .find(|oid| *oid == a || *oid == b)
            .context("missing numeric UNION type")?,
        (25 | 1043, 25 | 1043) => a,
        (1114 | 1184, 1114 | 1184) => 1184,
        _ => anyhow::bail!("UNION operands have incompatible native types"),
    })
}
pub(super) fn bind(parsed: Union, contract: &Contract) -> Result<Compiled> {
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
        "UNION operands have different widths"
    );
    let columns = left
        .output
        .columns
        .iter()
        .zip(&right.output.columns)
        .enumerate()
        .map(|(index, (a, b))| {
            let oid = common(
                &a.column,
                &b.column,
                (
                    left_unknown.get(index).is_some_and(Option::is_some),
                    right_unknown.get(index).is_some_and(Option::is_some),
                ),
            )?;
            ensure!(
                matches!(oid, 16 | 20 | 21 | 23 | 25 | 1043 | 1700 | 2950 | 1114 | 1184),
                "unsupported UNION native type"
            );
            Ok(OutputColumn {
                label: a.label.clone(),
                column: ColumnRef {
                    name: format!("@union_{index}"),
                    oid,
                    right: false,
                    nullable: a.column.nullable || b.column.nullable,
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let left_edge = align(&mut left, &columns, &left_unknown, "union_left")?;
    let right_edge = align(&mut right, &columns, &right_unknown, "union_right")?;
    let width = columns.len();
    let mut ir = Relational {
        sources: left.sources,
        projection: Vec::new(),
        nodes: left.nodes,
        output: crate::compiler::Projected { columns },
    };
    ir.sources.extend(right.sources);
    ensure!(ir.sources.len() <= 16, "UNION source occurrence limit exceeded");
    ir.nodes.extend(right.nodes);
    ir.nodes.push(Node::Union { id: "union".into(), left: left_edge, right: right_edge });
    ir.nodes.push(Node::Output { id: "project".into(), input: "union".into() });
    let order = ordering(parsed.order, &ir)?;
    super::ordering::apply(
        Compiled { relational: ir, result_columns: Vec::new() },
        super::ordering::Clause {
            order,
            visible: width,
            distinct: false,
            limit: parsed.limit,
            offset: parsed.offset,
        },
        contract,
    )
}
fn align(
    ir: &mut Relational,
    columns: &[OutputColumn],
    unknown: &[Option<Ast>],
    prefix: &str,
) -> Result<String> {
    let input = ir.nodes.last().context("UNION branch has no visible edge")?.id().to_owned();
    let mut computed = Vec::new();
    let mut fields = Vec::new();
    for (index, (source, target)) in ir.output.columns.iter().zip(columns).enumerate() {
        let input = if let Some(Some(value)) = unknown.get(index) {
            scalar::register(
                Typed::union_literal(value, target.column.oid)?,
                &mut computed,
                &format!("@{prefix}_cast_"),
            )?
        } else if source.column.oid != target.column.oid {
            scalar::register(
                Typed::union_cast(source.column.clone(), target.column.oid)?,
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
