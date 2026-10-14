//! Connect independent relational scopes through typed boundary maps and joins.
use super::{Compiled, bind as bind_inner};
use crate::{
    compiler::{
        ColumnRef, Predicate, Program,
        parser::{Name, scoped_join::Parsed},
        projection::OutputColumn,
        relational::{Binding, Node, Relational, field},
    },
    source::Contract,
};
use anyhow::{Context, Result, ensure};

pub(super) struct Scope {
    pub(super) alias: String,
    pub(super) ir: Relational,
    pub(super) revision: String,
}
pub(super) fn bind(parsed: Parsed, contract: &Contract) -> Result<Compiled> {
    let left = scope(parsed.left, "left_", 0, contract)?;
    let right = scope(parsed.right, "right_", left.ir.sources.len(), contract)?;
    ensure!(left.alias != right.alias, "duplicate scoped join qualifier");
    let scopes = [left, right];
    let keys = parsed.keys.map(|value| keys(value, &scopes)).transpose()?;
    let join_revision = if keys.is_some() {
        "sql-scoped-equijoin-v1:typed-boundary-map-v1:canonical-compound-key-v1"
    } else {
        "sql-scoped-crossjoin-v1:typed-boundary-map-v1:constant-navigation-key-v1"
    };
    let resolve = |name: &Name| Ok(resolve(name, &scopes)?.1);
    let (columns, terminal) = super::relational::output(parsed.columns, &resolve)?;
    let predicate = parsed
        .predicate
        .map(|value| crate::compiler::expression::bind(value, &resolve).map(Predicate::new))
        .transpose()?;
    let [mut left, right] = scopes;
    let revision = format!("{join_revision}:{}:{}", left.revision, right.revision);
    left.ir.sources.extend(right.ir.sources);
    left.ir.nodes.extend(right.ir.nodes);
    left.ir.nodes.extend(join_keys(keys));
    left.ir.nodes.push(Node::Join {
        id: "joined".into(),
        left: "left_key".into(),
        right: "right_key".into(),
    });
    let input = if let Some(predicate) = predicate {
        left.ir.nodes.push(Node::Filter {
            id: "join_filter".into(),
            input: "joined".into(),
            predicate,
        });
        "join_filter"
    } else {
        "joined"
    };
    left.ir.nodes.push(Node::Output { id: "project".into(), input: input.into() });
    left.ir.output.columns = columns;
    left.ir.output.terminal = terminal;
    Ok(Compiled {
        program: Program::Relational { relational: left.ir },
        predicates: None,
        revision: Some(revision),
    })
}
fn join_keys(keys: Option<Vec<(ColumnRef, ColumnRef)>>) -> [Node; 2] {
    let Some(keys) = keys else {
        return [
            Node::CrossKey { id: "left_key".into(), input: "left_visible".into() },
            Node::CrossKey { id: "right_key".into(), input: "right_visible".into() },
        ];
    };
    [
        Node::EquijoinKey {
            id: "left_key".into(),
            input: "left_visible".into(),
            keys: keys.iter().map(|pair| pair.0.clone()).collect(),
        },
        Node::EquijoinKey {
            id: "right_key".into(),
            input: "right_visible".into(),
            keys: keys.into_iter().map(|pair| pair.1).collect(),
        },
    ]
}
pub(super) fn scope(
    (parsed, alias): (Box<crate::compiler::parser::Parsed>, String),
    prefix: &str,
    offset: usize,
    contract: &Contract,
) -> Result<Scope> {
    let mut compiled = bind_inner(*parsed, contract)?;
    super::derived::compose_join(&mut compiled)?;
    let Program::Relational { relational: mut ir } = compiled.program else {
        anyhow::bail!("scoped join requires relational input");
    };
    ensure!(
        ir.output.terminal.as_ref().is_none_or(crate::catalog::Terminal::native_rendering),
        "scoped join cannot consume sink-deferred expressions"
    );
    super::partition::composition::lower_source_filter(&mut ir, compiled.predicates)?;
    let Some(Node::Output { input, .. }) = ir.nodes.pop() else {
        anyhow::bail!("scoped join requires terminal relational output");
    };
    for node in &mut ir.nodes {
        super::partition::composition::rewrite(node, &|id| format!("{prefix}{id}"), &|edge| {
            format!("{prefix}{edge}")
        });
    }
    let fields = ir
        .output
        .columns
        .iter()
        .enumerate()
        .map(|(index, output)| {
            let mut target = output.column.clone();
            target.name = format!("@{prefix}{index}");
            Binding { input: output.column.clone(), output: target }
        })
        .collect::<Vec<_>>();
    ir.output.columns = ir
        .output
        .columns
        .into_iter()
        .zip(&fields)
        .map(|(column, field)| OutputColumn { column: field.output.clone(), label: column.label })
        .collect();
    ir.nodes.push(Node::Rebind {
        id: format!("{prefix}visible"),
        input: format!("{prefix}{input}"),
        fields,
    });
    adapt_sources(&mut ir, offset, contract)?;
    Ok(Scope {
        alias,
        ir,
        revision: compiled.revision.context("missing scoped compiler revision")?,
    })
}
fn adapt_sources(ir: &mut Relational, offset: usize, contract: &Contract) -> Result<()> {
    let sources = ir
        .nodes
        .iter()
        .enumerate()
        .filter_map(|(position, node)| {
            if let Node::Source { id, source } = node {
                Some((position, id.clone(), *source))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    // Reverse insertion keeps recorded source positions valid.
    for (position, id, local) in sources.into_iter().rev() {
        let global = local.checked_add(offset).context("source index overflow")?;
        let Node::Source { source, .. } = &mut ir.nodes[position] else {
            anyhow::bail!("missing scoped source")
        };
        *source = global;
        if global == local {
            continue;
        }
        let source = ir.sources.get(local).context("missing scoped source metadata")?;
        let relation = contract
            .relations
            .iter()
            .find(|relation| relation.schema == source.schema && relation.table == source.table)
            .context("unknown scoped source")?;
        let fields = relation
            .columns
            .iter()
            .map(|column| {
                let input = ColumnRef {
                    name: field(global, &column.name),
                    right: false,
                    oid: column.oid,
                    nullable: column.nullable,
                };
                let mut output = input.clone();
                output.name = field(local, &column.name);
                Binding { input, output }
            })
            .collect();
        // Nested remaps strictly increase source indices; bind both boundaries into the ID.
        let mapped = format!("{id}_local_{global}_{local}");
        for node in &mut ir.nodes {
            super::partition::composition::rewrite(node, &|id| id.into(), &|edge| {
                if edge == id { mapped.clone() } else { edge.into() }
            });
        }
        ir.nodes.insert(position + 1, Node::Rebind { id: mapped, input: id, fields });
    }
    Ok(())
}
fn resolve(name: &Name, scopes: &[Scope; 2]) -> Result<(usize, ColumnRef)> {
    let (qualifier, label) = match name.0.as_slice() {
        [label] => (None, label),
        [qualifier, label] => (Some(qualifier), label),
        _ => anyhow::bail!("invalid scoped join column"),
    };
    let mut matches = scopes
        .iter()
        .enumerate()
        .filter(|(_, scope)| qualifier.is_none_or(|qualifier| qualifier == &scope.alias))
        .flat_map(|(index, scope)| {
            scope
                .ir
                .output
                .columns
                .iter()
                .filter(move |column| column.label == *label)
                .map(move |column| (index, column.column.clone()))
        });
    let column = matches.next().context("unknown scoped join column")?;
    ensure!(matches.next().is_none(), "ambiguous scoped join column");
    Ok(column)
}
fn keys(names: Vec<(Name, Name)>, scopes: &[Scope; 2]) -> Result<Vec<(ColumnRef, ColumnRef)>> {
    let mut keys = names
        .into_iter()
        .map(|(left, right)| {
            let left = resolve(&left, scopes)?;
            let right = resolve(&right, scopes)?;
            ensure!(left.0 != right.0, "scoped equality must connect both inputs");
            let (left, right) = if left.0 == 0 { (left.1, right.1) } else { (right.1, left.1) };
            ensure!(
                left.oid == right.oid
                    && matches!(left.oid, 16 | 20 | 21 | 23 | 1700 | 2950 | 1114 | 1184),
                "unsupported or incompatible scoped join key types"
            );
            Ok((left, right))
        })
        .collect::<Result<Vec<_>>>()?;
    keys.sort_unstable();
    keys.dedup();
    Ok(keys)
}
