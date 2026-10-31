//! Extract side-local equijoin keys; bind the remaining ON condition before null extension.
use super::{Column, resolve};
use crate::compiler::{
    ColumnRef, Predicate, expression,
    parser::{
        expressions::{names, node, optional},
        scalar::Key,
    },
    scalar::{self, Computed},
    typed,
};
use anyhow::{Context, Result, ensure};
use pg_query::{Node, NodeEnum, protobuf as pg};
use std::cell::Cell;
pub(super) struct Bound {
    pub keys: Vec<(ColumnRef, ColumnRef)>,
    pub computed: [Vec<Computed>; 2],
    pub predicate: Option<Predicate>,
}
struct Operand {
    sides: u8,
    expression: typed::Typed,
    collation: u32,
}
struct Scope<'a> {
    all: Vec<Column>,
    left: &'a [Column],
}
impl<'a> Scope<'a> {
    fn new(left: &'a [Column], right: &[Column]) -> Self {
        let mut all = left.to_vec();
        all.extend_from_slice(right);
        Self { all, left }
    }
    fn collation(&self, value: &ColumnRef) -> Result<u32> {
        self.all
            .iter()
            .find(|column| column.value.name == value.name)
            .map(|column| column.collation)
            .context("missing join text collation")
    }
    fn operand(&self, key: Key) -> Result<Operand> {
        let sides = Cell::new(0);
        let resolve = |name: &crate::compiler::parser::Name| {
            let column = resolve(name, &self.all)?;
            let left = self.left.iter().any(|prior| prior.value.name == column.value.name);
            sides.set(sides.get() | if left { 1 } else { 2 });
            Ok(column.value.clone())
        };
        let expression = match key {
            Key::Column(name) => typed::Typed::column(resolve(&name)?),
            Key::Typed(ast) => typed::bind(&ast, &resolve, 0)?,
        };
        let collation = expression.collation(&|value| self.collation(value))?;
        Ok(Operand { sides: sides.get(), expression, collation })
    }
}
impl Bound {
    const fn new() -> Self {
        Self { keys: Vec::new(), computed: [Vec::new(), Vec::new()], predicate: None }
    }
    fn key(&mut self, a: Operand, b: Operand, prefix: &str) -> Result<bool> {
        if !matches!((a.sides, b.sides), (1, 2) | (2, 1)) {
            return Ok(false);
        }
        let (a, b) = if a.sides == 1 { (a, b) } else { (b, a) };
        let (aoid, boid) = (a.expression.oid(), b.expression.oid());
        let compatible = aoid == boid
            || (matches!(aoid, 20 | 21 | 23) && matches!(boid, 20 | 21 | 23))
            || (matches!(aoid, 25 | 1043) && matches!(boid, 25 | 1043));
        if !compatible
            || !matches!(aoid, 16 | 20 | 21 | 23 | 25 | 1043 | 1082 | 1700 | 2950 | 1114 | 1184)
        {
            return Ok(false);
        }
        if matches!(aoid, 25 | 1043) {
            typed::collation::common(a.collation, b.collation)?;
            ensure!(a.collation != 0 && b.collation != 0, "unresolved join text collation");
        }
        let mut columns = Vec::new();
        for (side, value) in [a, b].into_iter().enumerate() {
            columns.push(if let Some(column) = value.expression.as_column() {
                column.clone()
            } else {
                scalar::register(
                    value.expression,
                    &mut self.computed[side],
                    &format!("@{prefix}_input_{side}_"),
                )?
            });
        }
        let columns =
            <[ColumnRef; 2]>::try_from(columns).map_err(|_| anyhow::anyhow!("join key arity"))?;
        self.keys.push(columns.into());
        ensure!(self.keys.len() <= 16, "join key count exceeded");
        Ok(true)
    }
    fn normalize(&mut self) {
        self.keys.sort_unstable();
        self.keys.dedup();
    }
}
pub(super) fn bind(
    names: Vec<(Key, Key)>,
    left: &[Column],
    right: &[Column],
    prefix: &str,
) -> Result<Bound> {
    let scope = Scope::new(left, right);
    let mut bound = Bound::new();
    for (a, b) in names {
        ensure!(
            bound.key(scope.operand(a)?, scope.operand(b)?, prefix)?,
            "ON equality must connect compatible side-local inputs"
        );
    }
    bound.normalize();
    Ok(bound)
}
pub(super) fn condition(
    condition: Option<&Node>,
    left: &[Column],
    right: &[Column],
    prefix: &str,
) -> Result<Bound> {
    let scope = Scope::new(left, right);
    let mut bound = Bound::new();
    let mut residual = Vec::new();
    if let Some(condition) = condition {
        conjunct(condition, (&scope, prefix), &mut bound, &mut residual, 0)?;
    }
    bound.normalize();
    bound.predicate = match residual.len() {
        0 => None,
        1 => residual.pop().map(Predicate::new),
        _ => Some(Predicate::new(expression::Expr::And(residual))),
    };
    Ok(bound)
}
fn conjunct(
    value: &Node,
    context: (&Scope<'_>, &str),
    bound: &mut Bound,
    residual: &mut Vec<expression::Expr>,
    depth: usize,
) -> Result<()> {
    let (scope, prefix) = context;
    ensure!(depth < 16 && residual.len() + bound.keys.len() < 64, "join condition budget exceeded");
    match node(value)? {
        NodeEnum::BoolExpr(value) if value.boolop == i32::from(pg::BoolExprType::AndExpr) => {
            ensure!(value.xpr.is_none(), "invalid ON conjunction");
            for value in &value.args {
                conjunct(value, context, bound, residual, depth + 1)?;
            }
            return Ok(());
        }
        NodeEnum::AExpr(value)
            if value.kind == i32::from(pg::AExprKind::AexprOp)
                && names(&value.name)?.0 == ["="] =>
        {
            let a = scope.operand(crate::compiler::parser::scalar::key(optional(
                value.lexpr.as_deref(),
            )?)?)?;
            let b = scope.operand(crate::compiler::parser::scalar::key(optional(
                value.rexpr.as_deref(),
            )?)?)?;
            if bound.key(a, b, prefix)? {
                return Ok(());
            }
        }
        _ => {}
    }
    residual.push(expression::bind_join(
        value,
        &|name| Ok(resolve(name, &scope.all)?.value.clone()),
        &|column| scope.collation(column),
    )?);
    Ok(())
}

pub(super) fn using(pairs: &[super::using::Pair], prefix: &str) -> Result<Bound> {
    let mut bound = Bound::new();
    for pair in pairs {
        let left = Operand {
            sides: 1,
            expression: typed::Typed::set_cast(pair.left.value.clone(), pair.oid, None)?,
            collation: pair.left.collation,
        };
        let right = Operand {
            sides: 2,
            expression: typed::Typed::set_cast(pair.right.value.clone(), pair.oid, None)?,
            collation: pair.right.collation,
        };
        ensure!(bound.key(left, right, prefix)?, "incompatible USING key types");
    }
    bound.normalize();
    Ok(bound)
}
