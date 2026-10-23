use super::parser::expressions::node;
use anyhow::Result;
use pg_query::Node;
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(super) enum Compare {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}
pub(super) type Expr = Box<Node>;
pub(super) fn predicate(value: Option<&Node>) -> Result<Option<Expr>> {
    value
        .map(|value| {
            node(value)?;
            Ok(Box::new(value.clone()))
        })
        .transpose()
}
