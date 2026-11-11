//! Aggregate -> HAVING -> scalar/window projection, using the ordinary scope binder.
use super::{Body, Stage};
use crate::compiler::{
    ColumnRef, Compiled, Predicate,
    parser::{
        Name,
        partition::{self as syntax, Argument, Target, Value},
        scalar::Key,
    },
    relational::Node as RelNode,
};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};

type Resolver<'a> = dyn Fn(&Name) -> Result<ColumnRef> + 'a;
struct Rewrite<'a, F> {
    resolve: &'a F,
    keys: Vec<ColumnRef>,
    scope: Option<usize>,
    aggregates: Vec<syntax::Aggregate>,
    visited: usize,
}
fn column(kind: char, index: usize) -> Node {
    name_node(&Name(vec![String::new(), format!("{kind}{index}")]))
}
fn name_node(name: &Name) -> Node {
    Node {
        node: Some(NodeEnum::ColumnRef(pg::ColumnRef {
            fields: name
                .0
                .iter()
                .map(|s| Node { node: Some(NodeEnum::String(pg::String { sval: s.clone() })) })
                .collect(),
            location: -1,
        })),
    }
}
fn key_node(key: &Key) -> Node {
    match key {
        Key::Column(name) => name_node(name),
        Key::Typed(value) => (**value).clone(),
    }
}
pub(super) fn required(body: &Body) -> bool {
    body.having.is_some()
        || body.targets.iter().any(|target| match &target.value {
            Value::Typed(value) => contains_aggregate(value),
            Value::Aggregate(value) if value.window.is_none() => body.groups.is_empty(),
            Value::Aggregate(value) => !body.groups.is_empty() || window_aggregate(value),
        })
}
fn key_aggregate(key: &Key) -> bool {
    matches!(key,Key::Typed(value) if contains_aggregate(value))
}
fn window_aggregate(value: &syntax::Aggregate) -> bool {
    value.argument.as_ref().is_some_and(
        |argument| matches!(argument,Argument::Expression(value) if contains_aggregate(value)),
    ) || value.filter.as_deref().is_some_and(contains_aggregate)
        || value.window.as_ref().is_some_and(|window| {
            window.keys.iter().any(key_aggregate)
                || window.order.iter().any(|(key, _, _)| key_aggregate(key))
        })
}
fn contains_aggregate(value: &Node) -> bool {
    if syntax::group_aggregate(value) {
        return true;
    }
    match &value.node {
        Some(NodeEnum::FuncCall(value)) => value.args.iter().any(contains_aggregate),
        Some(NodeEnum::AExpr(value)) => [&value.lexpr, &value.rexpr]
            .into_iter()
            .flatten()
            .any(|value| contains_aggregate(value)),
        Some(NodeEnum::BoolExpr(value)) => value.args.iter().any(contains_aggregate),
        Some(NodeEnum::List(value)) => value.items.iter().any(contains_aggregate),
        Some(NodeEnum::CoalesceExpr(value)) => value.args.iter().any(contains_aggregate),
        Some(NodeEnum::MinMaxExpr(value)) => value.args.iter().any(contains_aggregate),
        Some(NodeEnum::TypeCast(value)) => value.arg.as_deref().is_some_and(contains_aggregate),
        Some(NodeEnum::CollateClause(value)) => {
            value.arg.as_deref().is_some_and(contains_aggregate)
        }
        Some(NodeEnum::NullTest(value)) => value.arg.as_deref().is_some_and(contains_aggregate),
        Some(NodeEnum::BooleanTest(value)) => value.arg.as_deref().is_some_and(contains_aggregate),
        Some(NodeEnum::CaseExpr(value)) => {
            [&value.arg, &value.defresult]
                .into_iter()
                .flatten()
                .any(|value| contains_aggregate(value))
                || value.args.iter().any(contains_aggregate)
        }
        Some(NodeEnum::CaseWhen(value)) => [&value.expr, &value.result]
            .into_iter()
            .flatten()
            .any(|value| contains_aggregate(value)),
        _ => false,
    }
}
pub(super) fn build(
    body: &Body,
    groups: Vec<Key>,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    context: Stage<'_>,
) -> Result<Compiled> {
    let mut computed = Vec::new();
    let keys = super::keys(groups.clone(), resolve, &mut computed, context.scalar_scope)?;
    let mut rewrite =
        Rewrite { resolve, keys, scope: context.scalar_scope, aggregates: Vec::new(), visited: 0 };
    let targets = rewrite.targets(&body.targets)?;
    let mut condition = body.having.as_deref().cloned();
    if let Some(condition) = &mut condition {
        rewrite.expression(condition, false, 0)?;
    }
    if body.groups.is_empty() && rewrite.aggregates.is_empty() {
        rewrite.aggregates.push(syntax::Aggregate {
            function: crate::compiler::partition::Function::Count,
            argument: None,
            filter: None,
            window: None,
            navigation: None,
        });
    }
    let key_count = groups.len();
    let mut group_targets = groups
        .iter()
        .enumerate()
        .map(|(index, key)| Target {
            value: Value::Typed(Box::new(key_node(key))),
            label: format!("group_{index}"),
        })
        .collect::<Vec<_>>();
    group_targets.extend(rewrite.aggregates.into_iter().enumerate().map(|(index, value)| Target {
        value: Value::Aggregate(Box::new(value)),
        label: format!("aggregate_{index}"),
    }));
    let group = Body { targets: group_targets, having: None, ..body.clone() };
    // Suppress this staging pass only: its targets are already the aggregate stage.
    let contract = context.contract;
    let boundary = context.sources.is_empty().then_some(context.occurrence_keys);
    let mut compiled = super::build_direct(&group, groups, resolve, context)?;
    let columns = compiled.relational.output.columns.clone();
    let post_resolve = |name: &Name| -> Result<ColumnRef> {
        if let [empty, field] = name.0.as_slice()
            && empty.is_empty()
        {
            let (kind, index) = field.split_at(1);
            let index = index.parse::<usize>()?;
            let position = if kind == "g" {
                index
            } else {
                key_count.checked_add(index).context("HAVING field overflow")?
            };
            return columns
                .get(position)
                .map(|value| value.column.clone())
                .context("missing staged HAVING field");
        }
        let column = resolve(name)?;
        columns[..key_count]
            .iter()
            .find(|value| value.column == column)
            .map(|value| value.column.clone())
            .context("column must appear in GROUP BY")
    };
    if let Some(condition) = condition {
        let predicate =
            Predicate::new(crate::compiler::expression::bind_quals(&condition, &post_resolve)?);
        let Some(RelNode::Output { id, input }) = compiled.relational.nodes.pop() else {
            anyhow::bail!("HAVING requires aggregate output")
        };
        compiled.relational.nodes.push(RelNode::Filter { id: "having".into(), input, predicate });
        compiled.relational.nodes.push(RelNode::Output { id, input: "having".into() });
    }
    let post = post_projection(targets);
    let post_resolve: &Resolver<'_> = &post_resolve;
    let mut outer =
        super::composition::stage(&compiled.relational, &post, &post_resolve, contract, boundary)?;
    super::composition::compose(&mut compiled.relational, &mut outer)?;
    Ok(compiled)
}
const fn post_projection(targets: Vec<Target>) -> Body {
    Body {
        expansion: None,
        visible: targets.len(),
        targets,
        groups: Vec::new(),
        predicate: None,
        having: None,
    }
}
impl<F: Fn(&Name) -> Result<ColumnRef>> Rewrite<'_, F> {
    fn targets(&mut self, targets: &[Target]) -> Result<Vec<Target>> {
        let mut outputs = Vec::new();
        for target in targets {
            let value = match &target.value {
                Value::Typed(value) => {
                    let mut value = (**value).clone();
                    self.expression(&mut value, true, 0)?;
                    Value::Typed(Box::new(value))
                }
                Value::Aggregate(value) if value.window.is_none() => {
                    Value::Typed(Box::new(self.aggregate((**value).clone())?))
                }
                Value::Aggregate(value) => {
                    let mut value = (**value).clone();
                    self.window(&mut value)?;
                    Value::Aggregate(Box::new(value))
                }
            };
            outputs.push(Target { value, label: target.label.clone() });
        }
        Ok(outputs)
    }
    fn aggregate(&mut self, value: syntax::Aggregate) -> Result<Node> {
        ensure!(self.aggregates.len() < 64, "HAVING aggregate budget exceeded");
        let index = self.aggregates.len();
        self.aggregates.push(value);
        Ok(column('a', index))
    }
    fn expression(&mut self, value: &mut Node, windows: bool, depth: usize) -> Result<()> {
        self.visited += 1;
        ensure!(self.visited <= 1024 && depth < 64, "HAVING expression budget exceeded");
        if syntax::group_aggregate(value) {
            let target = Node {
                node: Some(NodeEnum::ResTarget(Box::new(pg::ResTarget {
                    val: Some(Box::new(value.clone())),
                    ..pg::ResTarget::default()
                }))),
            };
            let Value::Aggregate(aggregate) = syntax::parse_target(&target)?.value else {
                anyhow::bail!("invalid HAVING aggregate")
            };
            *value = self.aggregate(*aggregate)?;
            return Ok(());
        }
        let mut scratch = Vec::new();
        if let Ok(key) =
            crate::compiler::typed::computed(value, self.resolve, &mut scratch, self.scope)
            && let Some(index) = self.keys.iter().position(|candidate| *candidate == key)
        {
            *value = column('g', index);
            return Ok(());
        }
        match &mut value.node {
            Some(NodeEnum::FuncCall(call)) => {
                ensure!(
                    windows || call.over.is_none(),
                    "window functions are not allowed in HAVING"
                );
                for arg in &mut call.args {
                    self.expression(arg, windows, depth + 1)?;
                }
                if let Some(filter) = call.agg_filter.as_deref_mut() {
                    self.expression(filter, windows, depth + 1)?;
                }
            }
            Some(NodeEnum::AExpr(expr)) => {
                for arg in [&mut expr.lexpr, &mut expr.rexpr].into_iter().flatten() {
                    self.expression(arg, windows, depth + 1)?;
                }
            }
            Some(NodeEnum::BoolExpr(expr)) => self.expressions(&mut expr.args, windows, depth)?,
            Some(NodeEnum::List(expr)) => self.expressions(&mut expr.items, windows, depth)?,
            Some(NodeEnum::BooleanTest(expr)) => {
                self.optional_expression(expr.arg.as_deref_mut(), windows, depth)?;
            }
            Some(NodeEnum::NullTest(expr)) => {
                self.optional_expression(expr.arg.as_deref_mut(), windows, depth)?;
            }
            Some(NodeEnum::TypeCast(expr)) => {
                self.optional_expression(expr.arg.as_deref_mut(), windows, depth)?;
            }
            Some(NodeEnum::CollateClause(expr)) => {
                self.optional_expression(expr.arg.as_deref_mut(), windows, depth)?;
            }
            Some(NodeEnum::MinMaxExpr(expr)) => self.expressions(&mut expr.args, windows, depth)?,
            Some(NodeEnum::CoalesceExpr(expr)) => {
                self.expressions(&mut expr.args, windows, depth)?;
            }
            Some(NodeEnum::CaseExpr(expr)) => {
                for arg in [&mut expr.arg, &mut expr.defresult].into_iter().flatten() {
                    self.expression(arg, windows, depth + 1)?;
                }
                for arg in &mut expr.args {
                    self.expression(arg, windows, depth + 1)?;
                }
            }
            Some(NodeEnum::CaseWhen(expr)) => {
                for arg in [&mut expr.expr, &mut expr.result].into_iter().flatten() {
                    self.expression(arg, windows, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn expressions(&mut self, values: &mut [Node], windows: bool, depth: usize) -> Result<()> {
        for value in values {
            self.expression(value, windows, depth + 1)?;
        }
        Ok(())
    }

    fn optional_expression(
        &mut self,
        value: Option<&mut Node>,
        windows: bool,
        depth: usize,
    ) -> Result<()> {
        if let Some(value) = value {
            self.expression(value, windows, depth + 1)?;
        }
        Ok(())
    }
    fn window(&mut self, value: &mut syntax::Aggregate) -> Result<()> {
        if let Some(argument) = &mut value.argument {
            let mut node = match argument {
                Argument::Column(name) => name_node(name),
                Argument::Expression(node) => (**node).clone(),
            };
            self.expression(&mut node, true, 0)?;
            *argument = Argument::Expression(Box::new(node));
        }
        if let Some(filter) = &mut value.filter {
            self.expression(filter, true, 0)?;
        }
        if let Some(window) = &mut value.window {
            for key in &mut window.keys {
                self.key(key)?;
            }
            for (key, _, _) in &mut window.order {
                self.key(key)?;
            }
        }
        if let Some(navigation) = &mut value.navigation {
            let mut node = name_node(&navigation.argument);
            self.expression(&mut node, true, 0)?;
            navigation.argument = crate::compiler::parser::expressions::column(&node)?;
            for value in [&mut navigation.offset, &mut navigation.default].into_iter().flatten() {
                self.expression(value, true, 0)?;
            }
        }
        Ok(())
    }
    fn key(&mut self, key: &mut Key) -> Result<()> {
        let mut node = key_node(key);
        self.expression(&mut node, true, 0)?;
        *key = crate::compiler::parser::scalar::key(&node)?;
        Ok(())
    }
}
