//! Evaluate grouped argument expressions only after their aggregate FILTER.
use crate::compiler::{
    ColumnRef, Predicate, expression,
    parser::{Name, partition::Argument},
    partition::Function,
    scalar::{self, Computed},
    syntax,
    typed::{self, Typed},
};
use anyhow::{Result, ensure};
#[derive(Clone, Copy)]
pub(super) struct Context<'a> {
    pub scope: Option<usize>,
    pub function: &'a Function,
    pub framed: bool,
    pub default_bytewise: bool,
}
pub(super) struct Bound {
    pub column: Option<ColumnRef>,
    pub filter: Option<Predicate>,
    pub expression: Option<Typed>,
}
pub(super) fn bind(
    argument: Option<Argument>,
    filter: Option<syntax::Expr>,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
    computed: &mut Vec<Computed>,
    context: Context<'_>,
) -> Result<Bound> {
    let filter = filter.map(|value| expression::bind(&value, resolve)).transpose()?;
    let Some(Argument::Expression(ast)) = argument else {
        let column = match argument {
            Some(Argument::Column(name)) => Some(resolve(&name)?),
            _ => None,
        };
        if let Some(column) = &column {
            validate_extremum(context, column.oid, false, || Ok(column.collation))?;
        }
        return Ok(Bound { column, filter: filter.map(Predicate::new), expression: None });
    };
    let mut expression = if context.function.boolean() || context.function.frame_value() {
        Typed::bind_contextual(&ast, resolve, 0)?
    } else {
        typed::bind(&ast, resolve, 0)?
    };
    ensure!(
        !context.function.frame_value() || !matches!(expression.oid(), 0 | 705),
        "could not determine polymorphic type because input has type unknown"
    );
    if context.function.boolean() && matches!(expression.oid(), 0 | 705) {
        expression = expression.coerce_common(16)?;
    } else if expression.oid() == 0 {
        ensure!(
            matches!(context.function, Function::Count) || context.function.extremum(),
            "untyped NULL aggregate input requires an explicit cast"
        );
        expression = Typed::null_text();
    } else if expression.oid() == 705 {
        expression = expression.unknown_text();
    }
    validate_extremum(context, expression.oid(), expression.is_constant(), || {
        expression.collation(&|column| Ok(column.collation))
    })?;
    if context.framed {
        let column = ColumnRef {
            name: "@frame_argument".into(),
            right: false,
            oid: expression.oid(),
            nullable: expression.nullable(),
            collation: if context.function.frame_value() || context.function.extremum() {
                expression.collation(&|column| Ok(column.collation))?
            } else {
                0
            },
            modifier: None,
        };
        return Ok(Bound {
            column: Some(column),
            filter: filter.map(Predicate::new),
            expression: Some(expression),
        });
    }
    if let Some(filter) = filter {
        expression = expression.filtered(filter);
    }
    let column =
        scalar::register(expression, computed, &format!("@aggregate_input_{:?}_", context.scope))?;
    Ok(Bound { column: Some(column), filter: None, expression: None })
}

pub(super) fn integer_window_argument(
    ast: &pg_query::Node,
    resolve: &impl Fn(&Name) -> Result<ColumnRef>,
) -> Result<Typed> {
    let position = Typed::bind_contextual(ast, resolve, 0)?;
    ensure!(
        matches!(position.oid(), 0 | 705 | 21 | 23),
        "window function argument requires integer"
    );
    position.coerce_common(23)
}

fn validate_extremum(
    context: Context<'_>,
    oid: u32,
    constant: bool,
    collation: impl FnOnce() -> Result<u32>,
) -> Result<()> {
    if context.function.extremum() && matches!(oid, 25 | 1043) && !constant {
        crate::compiler::partition::require_bytewise_ordering(
            collation()?,
            context.default_bytewise,
        )?;
    }
    Ok(())
}
