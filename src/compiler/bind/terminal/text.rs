use super::{Builder, Expression, Function, names, node, optional};
use anyhow::{Context, Result, ensure};
use pg_query::{NodeEnum, protobuf as pg};
impl Builder<'_> {
    pub(super) fn collate(&mut self, value: &pg::CollateClause, depth: usize) -> Result<usize> {
        ensure!(
            names(&value.collname)?.0 == ["pg_catalog", "C"],
            "terminal text requires explicit pg_catalog.\"C\" collation"
        );
        let input = self.expression(optional(value.arg.as_deref())?, depth)?;
        self.push(Expression::CollateC(input))
    }
    pub(super) fn array(&mut self, value: &pg::AIndirection, depth: usize) -> Result<usize> {
        let [index] = value.indirection.as_slice() else {
            anyhow::bail!("terminal selection requires one index");
        };
        let NodeEnum::AIndices(index) = node(index)? else {
            anyhow::bail!("terminal selection requires array index");
        };
        ensure!(!index.is_slice && index.lidx.is_none(), "terminal slices unsupported");
        let NodeEnum::AConst(value_index) = node(optional(index.uidx.as_deref())?)? else {
            anyhow::bail!("terminal index requires integer literal");
        };
        ensure!(!value_index.isnull, "NULL terminal index unsupported");
        let Some(pg::a_const::Val::Ival(index)) = &value_index.val else {
            anyhow::bail!("terminal index requires integer literal");
        };
        let index = u16::try_from(index.ival).context("terminal index out of range")?;
        let input = self.expression(optional(value.arg.as_deref())?, depth)?;
        self.push(Expression::ArrayElement { input, index })
    }
    pub(super) fn null_test(&mut self, value: &pg::NullTest, depth: usize) -> Result<usize> {
        ensure!(value.xpr.is_none() && !value.argisrow, "terminal NULL test requires scalar");
        let negated = match pg::NullTestType::try_from(value.nulltesttype)? {
            pg::NullTestType::IsNull => false,
            pg::NullTestType::IsNotNull => true,
            pg::NullTestType::Undefined => anyhow::bail!("invalid terminal NULL test"),
        };
        let input = self.expression(optional(value.arg.as_deref())?, depth)?;
        self.push(Expression::IsNull { input, negated })
    }
    pub(super) fn text_call(
        &mut self,
        value: &pg::FuncCall,
        name: &str,
        depth: usize,
    ) -> Result<usize> {
        let function = if name == "lower" { Function::Lower } else { Function::RegexMatch };
        let arguments = value
            .args
            .iter()
            .map(|value| self.expression(value, depth))
            .collect::<Result<Vec<_>>>()?;
        self.push(Expression::Call { function, arguments })
    }
    pub(super) fn membership(&mut self, value: &pg::AExpr, depth: usize) -> Result<usize> {
        ensure!(names(&value.name)?.0 == ["="], "only terminal IN supported");
        let input = self.expression(optional(value.lexpr.as_deref())?, depth)?;
        let NodeEnum::List(list) = node(optional(value.rexpr.as_deref())?)? else {
            anyhow::bail!("terminal IN requires value list");
        };
        ensure!((1..=64).contains(&list.items.len()), "terminal IN list exceeds budget");
        let mut comparisons = Vec::new();
        for value in &list.items {
            let right = self.expression(value, depth)?;
            comparisons.push(self.push(Expression::Call {
                function: Function::TextEqual,
                arguments: vec![input, right],
            })?);
        }
        if let [comparison] = comparisons.as_slice() {
            return Ok(*comparison);
        }
        self.push(Expression::Or(comparisons))
    }
}
