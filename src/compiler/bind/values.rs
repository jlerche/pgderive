//! Bind constant expressions by column and emit one bootstrap bag.
use super::setop;
use crate::{
    compiler::{
        ColumnRef, Compiled, Projected,
        parser::values::label,
        projection::OutputColumn,
        relational::{ConstantColumn, Node, Relational, Source},
        scalar::Computed,
        typed::{Typed, collation::Derivation},
    },
    source::Contract,
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use pg_query::Node as Ast;

struct BoundColumn {
    layout: ConstantColumn,
    expressions: Vec<Typed>,
}

pub(super) fn bind(rows: Vec<Vec<Ast>>, contract: &Contract) -> Result<Compiled> {
    let count = rows.len();
    let columns = bind_rows(rows)?;
    let output = Projected {
        columns: columns
            .iter()
            .enumerate()
            .map(|(index, column)| OutputColumn {
                column: column.layout.reference.clone(),
                label: label(index),
            })
            .collect(),
    };
    let driver = contract.relations.first().context("VALUES requires a publication driver")?;
    let mut nodes = vec![Node::Source { id: "source".into(), source: 0 }];
    let input = if count == 1 {
        single_row(columns, &mut nodes)?
    } else {
        nodes.push(multirow(count, columns)?);
        "values"
    };
    nodes.push(Node::Output { id: "project".into(), input: input.into() });
    let relational = Relational {
        sources: vec![Source { schema: driver.schema.clone(), table: driver.table.clone() }],
        projection: Vec::new(),
        nodes,
        output,
    };
    Ok(Compiled { relational, result_columns: Vec::new() })
}

// PostgreSQL pulls up a singleton VALUES scan like a constant SELECT.
// Keep its expressions in the existing map so unused cells can be pruned.
fn single_row(columns: Vec<BoundColumn>, nodes: &mut Vec<Node>) -> Result<&'static str> {
    let computed = columns
        .into_iter()
        .map(|column| {
            Ok(Computed {
                column: column.layout.reference,
                expression: column.expressions.into_iter().next().context("missing VALUES cell")?,
            })
        })
        .collect::<Result<_>>()?;
    nodes.push(Node::Constant {
        id: "unit".into(),
        input: "source".into(),
        rows: vec![Row::new()],
        columns: Vec::new(),
    });
    nodes.push(Node::Map { id: "mapped".into(), input: "unit".into(), computed });
    Ok("mapped")
}

fn multirow(count: usize, columns: Vec<BoundColumn>) -> Result<Node> {
    let (columns, mut expressions): (Vec<_>, Vec<_>) =
        columns.into_iter().map(|column| (column.layout, column.expressions.into_iter())).unzip();
    let rows = (0..count)
        .map(|_| {
            expressions
                .iter_mut()
                .map(|values| values.next().context("missing VALUES cell"))
                .collect()
        })
        .collect::<Result<_>>()?;
    Ok(Node::Values { id: "values".into(), input: "source".into(), rows, columns })
}

fn bind_rows(input: Vec<Vec<Ast>>) -> Result<Vec<BoundColumn>> {
    let width = input.first().context("missing VALUES rows")?.len();
    let mut columns = vec![Vec::new(); width];
    for input in input {
        for (column, expression) in columns.iter_mut().zip(input) {
            let value = Typed::bind_contextual(
                &expression,
                &|_| anyhow::bail!("VALUES requires expressions independent of input rows"),
                0,
            )?;
            ensure!(value.is_constant(), "VALUES requires foldable constant expressions");
            column.push(value);
        }
    }
    columns.into_iter().enumerate().map(|(index, values)| bind_column(index, values)).collect()
}

fn bind_column(index: usize, values: Vec<Typed>) -> Result<BoundColumn> {
    let candidate = values.iter().try_fold(0, |prior, value| {
        let oid = value.oid();
        setop::common_type(prior, if oid == 705 { 0 } else { oid })
    })?;
    let oid = if candidate == 0 { 25 } else { candidate };
    let expressions =
        values.into_iter().map(|value| value.constant_coerce(oid)).collect::<Result<Vec<_>>>()?;
    let modifier = expressions.first().map_or(-1, Typed::bound_modifier);
    let modifier = if expressions.iter().all(|value| value.bound_modifier() == modifier) {
        modifier
    } else {
        -1
    };
    let collation = expressions
        .iter()
        .try_fold(Derivation::Implicit(0), |prior, value| {
            prior.merge(value.derive_collation(&|column| Ok(column.collation))?)
        })?
        .oid()
        .unwrap_or(0);
    let layout = ConstantColumn {
        reference: ColumnRef {
            name: format!("@values_{index}"),
            oid,
            nullable: expressions.iter().any(Typed::nullable),
            right: false,
            collation,
            modifier: Some(modifier).filter(|value| *value >= 0),
        },
        modifier,
        collation,
    };
    Ok(BoundColumn { layout, expressions })
}
