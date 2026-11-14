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
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum GroupingSlot {
    Expression,
    HiddenConstant,
    ConstantTarget(usize),
}

struct Rewrite<'a, F> {
    resolve: &'a F,
    keys: Vec<ColumnRef>,
    constant_slots: Vec<GroupingSlot>,
    scope: Option<usize>,
    aggregates: Vec<syntax::Aggregate>,
    visited: usize,
    grouping: Vec<Vec<usize>>,
    dependent: Vec<(Name, ColumnRef)>,
    base_dependency: bool,
}
pub(super) fn column(kind: char, index: usize) -> Node {
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
pub(super) fn key_node(key: &Key) -> Node {
    match key {
        Key::Column(name) => name_node(name),
        Key::Typed(value) | Key::Target(value, _) => (**value).clone(),
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
    ) || value.string_agg.as_ref().is_some_and(|string| {
        contains_aggregate(&string.value) || contains_aggregate(&string.delimiter)
    }) || value.filter.as_deref().is_some_and(contains_aggregate)
        || value.position.as_deref().is_some_and(contains_aggregate)
        || value.window.as_ref().is_some_and(|window| {
            window.keys.iter().any(key_aggregate)
                || window.order.iter().any(|(key, _, _)| key_aggregate(key))
        })
}
fn contains_aggregate(value: &Node) -> bool {
    if syntax::group_aggregate(value) || matches!(value.node, Some(NodeEnum::GroupingFunc(_))) {
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
    mut groups: Vec<Key>,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    context: Stage<'_>,
) -> Result<Compiled> {
    let mut rewrite = Rewrite::new(&groups, body, resolve, context.scalar_scope)?;
    rewrite.base_dependency = base_dependency(body, &rewrite, &context);
    let targets = rewrite.targets(&body.targets)?;
    let mut condition = body.having.as_deref().cloned();
    if let Some(condition) = &mut condition {
        rewrite.expression(condition, false, 0)?;
    }
    if (body.groups.is_empty() || body.grouping_sets.is_some()) && rewrite.aggregates.is_empty() {
        rewrite.aggregates.push(syntax::Aggregate {
            function: crate::compiler::partition::Function::Count,
            argument: None,
            filter: None,
            window: None,
            navigation: None,
            position: None,
            string_agg: None,
        });
    }
    let sets = rewrite.extend_groups(&mut groups, body.grouping_sets.as_ref());
    let key_count = groups.len();
    let mut group_targets = groups
        .iter()
        .enumerate()
        .map(|(index, key)| Target {
            value: Value::Typed(Box::new(key_node(key))),
            label: format!("group_{index}"),
        })
        .collect::<Vec<_>>();
    group_targets.extend(std::mem::take(&mut rewrite.aggregates).into_iter().enumerate().map(
        |(index, value)| Target {
            value: Value::Aggregate(Box::new(value)),
            label: format!("aggregate_{index}"),
        },
    ));
    let group = Body { targets: group_targets, having: None, grouping_sets: None, ..body.clone() };
    // Suppress this staging pass only: its targets are already the aggregate stage.
    let contract = context.contract;
    let boundary = context.sources.is_empty().then_some(context.occurrence_keys);
    let mut compiled =
        aggregate_stage(&group, groups, resolve, context, (sets.as_ref(), &rewrite))?;
    let columns = compiled.relational.output.columns.clone();
    let post_resolve = |name: &Name| -> Result<ColumnRef> {
        if let [empty, field] = name.0.as_slice()
            && empty.is_empty()
        {
            let (kind, index) = field.split_at(1);
            let index = index.parse::<usize>()?;
            let position = if kind == "m" {
                columns.len().checked_sub(index + 1).context("missing GROUPING mask")?
            } else if kind == "g" {
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
    apply_filter(&mut compiled.relational, condition.as_ref(), &post_resolve)?;
    let post = post_projection(targets);
    let post_resolve: &Resolver<'_> = &post_resolve;
    let mut outer =
        super::composition::stage(&compiled.relational, &post, &post_resolve, contract, boundary)?;
    super::composition::compose(&mut compiled.relational, &mut outer)?;
    Ok(compiled)
}
// PostgreSQL recognizes only the declared base-table primary key here.
// When every grouping set contains it, adding a dependent column does
// not split a group. Keep these physical keys separate from SQL GROUPING().
fn base_dependency<F>(body: &Body, rewrite: &Rewrite<'_, F>, context: &Stage<'_>) -> bool {
    primary_key_covered(context, |field| {
        body.grouping_sets.as_ref().map_or_else(
            || rewrite.keys.iter().any(|key| key.name == field),
            |sets| {
                sets.groups
                    .iter()
                    .all(|set| set.iter().any(|index| rewrite.keys[*index].name == field))
            },
        )
    })
}

pub(super) fn ordinary_base_dependency(
    groups: &[Key],
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    context: &Stage<'_>,
) -> bool {
    primary_key_covered(context, |field| {
        groups.iter().any(|key| {
            super::bind_key(key.clone(), resolve, &mut Vec::new(), context.scalar_scope)
                .is_ok_and(|resolved| resolved.name == field)
        })
    })
}

fn primary_key_covered(context: &Stage<'_>, contains: impl Fn(&str) -> bool) -> bool {
    let Some(relation) = context.base_relation else { return false };
    let mut primary = relation.columns.iter().filter(|column| column.primary).peekable();
    primary.peek().is_some()
        && primary.all(|column| contains(&crate::compiler::relational::field(0, &column.name)))
}

fn render_aggregate(value: &Node) -> Result<syntax::Aggregate> {
    let target = Node {
        node: Some(NodeEnum::ResTarget(Box::new(pg::ResTarget {
            val: Some(Box::new(value.clone())),
            ..pg::ResTarget::default()
        }))),
    };
    let Value::Aggregate(aggregate) = syntax::parse_target(&target)?.value else {
        anyhow::bail!("invalid HAVING aggregate")
    };
    Ok(*aggregate)
}

fn apply_filter(
    relational: &mut crate::compiler::relational::Relational,
    condition: Option<&Node>,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<()> {
    if let Some(condition) = condition {
        let predicate =
            Predicate::new(crate::compiler::expression::bind_quals(condition, resolve)?);
        let Some(RelNode::Output { id, input }) = relational.nodes.pop() else {
            anyhow::bail!("HAVING requires aggregate output")
        };
        relational.nodes.push(RelNode::Filter { id: "having".into(), input, predicate });
        relational.nodes.push(RelNode::Output { id, input: "having".into() });
    }
    Ok(())
}

fn aggregate_stage<F: Fn(&Name) -> Result<ColumnRef>>(
    group: &Body,
    groups: Vec<Key>,
    resolve: &F,
    context: Stage<'_>,
    (sets, rewrite): (Option<&crate::compiler::parser::grouping_sets::Sets>, &Rewrite<'_, F>),
) -> Result<Compiled> {
    if sets.is_none() && rewrite.grouping.is_empty() {
        return super::build_direct(group, groups, resolve, context);
    }
    let ordinary = sets.is_none().then(|| crate::compiler::parser::grouping_sets::Sets {
        groups: vec![(0..groups.len()).collect()],
        distinct: false,
    });
    let sets = sets.or(ordinary.as_ref()).context("missing grouping sets")?;

    super::grouping_sets::build(
        group,
        &groups,
        resolve,
        &context,
        super::grouping_sets::Layout {
            sets,
            keys: &rewrite.keys,
            masks: &rewrite.grouping,
            constant_slots: &rewrite.constant_slots,
        },
    )
}

const fn post_projection(targets: Vec<Target>) -> Body {
    Body {
        expansion: None,
        visible: targets.len(),
        targets,
        groups: Vec::new(),
        grouping_sets: None,
        predicate: None,
        having: None,
    }
}
impl<'a, F: Fn(&Name) -> Result<ColumnRef>> Rewrite<'a, F> {
    fn new(groups: &[Key], body: &Body, resolve: &'a F, scope: Option<usize>) -> Result<Self> {
        let mut computed = Vec::new();
        let keys = super::keys(groups.to_vec(), resolve, &mut computed, scope)?;
        let constant_slots = constant_slots(groups, &keys, body, resolve, (scope, &computed));
        Ok(Self {
            resolve,
            constant_slots,
            keys,
            scope,
            aggregates: Vec::new(),
            visited: 0,
            grouping: Vec::new(),
            dependent: Vec::new(),
            base_dependency: false,
        })
    }

    fn targets(&mut self, targets: &[Target]) -> Result<Vec<Target>> {
        let mut outputs = Vec::new();
        for (index, target) in targets.iter().enumerate() {
            let value = match &target.value {
                Value::Typed(value) => {
                    let value = if let Some(key) = self
                        .constant_slots
                        .iter()
                        .position(|slot| *slot == GroupingSlot::ConstantTarget(index))
                    {
                        column('g', key)
                    } else {
                        let mut value = (**value).clone();
                        self.expression(&mut value, true, 0)?;
                        value
                    };
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
    fn grouping_mask(&mut self, args: &[Node]) -> Result<Node> {
        ensure!((1..=31).contains(&args.len()), "GROUPING requires 1 to 31 arguments");
        let mut positions = Vec::new();
        for arg in args {
            ensure!(!bare_unknown(arg), "GROUPING arguments must match GROUP BY expressions");
            let key =
                crate::compiler::typed::computed(arg, self.resolve, &mut Vec::new(), self.scope)?;
            positions.push(
                self.keys
                    .iter()
                    .position(|candidate| *candidate == key)
                    .context("GROUPING arguments must match GROUP BY expressions")?,
            );
        }
        let index = self.grouping.len();
        ensure!(index < 64, "GROUPING expression budget exceeded");
        self.grouping.push(positions);
        Ok(column('m', index))
    }
    fn extend_groups(
        &mut self,
        keys: &mut Vec<Key>,
        sets: Option<&crate::compiler::parser::grouping_sets::Sets>,
    ) -> Option<crate::compiler::parser::grouping_sets::Sets> {
        let mut sets = sets.cloned();
        let first = keys.len();
        for (name, resolved) in std::mem::take(&mut self.dependent) {
            keys.push(Key::Column(name));
            self.keys.push(resolved);
            self.constant_slots.push(GroupingSlot::Expression);
        }
        if let Some(sets) = &mut sets {
            for set in &mut sets.groups {
                set.extend(first..keys.len());
            }
        }
        sets
    }

    fn dependent_column(&mut self, value: &Node) -> Result<Option<Node>> {
        if !self.base_dependency || !matches!(value.node, Some(NodeEnum::ColumnRef(_))) {
            return Ok(None);
        }
        let name = crate::compiler::parser::expressions::column(value)?;
        let resolved = (self.resolve)(&name)?;
        if resolved.name == crate::compiler::expansion::FIELD {
            return Ok(None);
        }
        let index = self.dependent.iter().position(|(_, column)| *column == resolved);
        let index = index.unwrap_or(self.dependent.len());
        if index == self.dependent.len() {
            self.dependent.push((name, resolved));
        }
        Ok(Some(column('g', self.keys.len() + index)))
    }

    fn grouped_expression(&self, value: &Node) -> Option<usize> {
        let key =
            crate::compiler::typed::computed(value, self.resolve, &mut Vec::new(), self.scope)
                .ok()?;
        self.keys
            .iter()
            .zip(&self.constant_slots)
            .position(|(candidate, slot)| *candidate == key && *slot == GroupingSlot::Expression)
    }
    fn expression(&mut self, value: &mut Node, windows: bool, depth: usize) -> Result<()> {
        self.visited += 1;
        ensure!(self.visited <= 1024 && depth < 64, "HAVING expression budget exceeded");
        if let Some(NodeEnum::GroupingFunc(grouping)) = &value.node {
            *value = self.grouping_mask(&grouping.args)?;
            return Ok(());
        }
        if syntax::group_aggregate(value) {
            *value = self.aggregate(render_aggregate(value)?)?;
            return Ok(());
        }
        if let Some(index) = self.grouped_expression(value) {
            *value = column('g', index);
            return Ok(());
        }
        if let Some(dependent) = self.dependent_column(value)? {
            *value = dependent;
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
        if let Some(string) = &mut value.string_agg {
            self.expression(&mut string.value, true, 0)?;
            self.expression(&mut string.delimiter, true, 0)?;
        }
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
        if let Some(position) = &mut value.position {
            self.expression(position, true, 0)?;
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

// Unlike a SELECT target that becomes a grouping key, a GROUPING argument
// has no context that coerces a bare unknown literal to text.
fn bare_unknown(value: &Node) -> bool {
    let mut value = value;
    while let Some(NodeEnum::CollateClause(clause)) = &value.node {
        let Some(input) = clause.arg.as_deref() else { return false };
        value = input;
    }
    matches!(&value.node, Some(NodeEnum::AConst(c))
        if c.isnull || matches!(c.val, Some(pg::a_const::Val::Sval(_))))
}

// PostgreSQL associates a constant grouping expression with a single target
// entry. Equal constant expressions in other targets or HAVING stay constant.
fn constant_slots(
    groups: &[Key],
    keys: &[ColumnRef],
    body: &Body,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    (scope, computed): (Option<usize>, &[crate::compiler::scalar::Computed]),
) -> Vec<GroupingSlot> {
    groups
        .iter()
        .zip(keys)
        .map(|(key, column)| {
            if !computed.iter().any(|item| item.column == *column && item.expression.is_constant())
            {
                return GroupingSlot::Expression;
            }
            if let Key::Target(_, index) = key {
                return GroupingSlot::ConstantTarget(*index);
            }
            let unknown = bare_unknown(&key_node(key));
            body.targets
                .iter()
                .position(|target| {
                    let Value::Typed(node) = &target.value else { return false };
                    bare_unknown(node) == unknown
                        && crate::compiler::typed::computed(node, resolve, &mut Vec::new(), scope)
                            .is_ok_and(|candidate| candidate == *column)
                })
                .map_or(GroupingSlot::HiddenConstant, GroupingSlot::ConstantTarget)
        })
        .collect()
}
