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
impl Compare {
    pub(super) fn parse(operator: &str) -> Result<Self> {
        Ok(match operator {
            "=" => Self::Eq,
            "<>" => Self::Ne,
            "<" => Self::Lt,
            "<=" => Self::Le,
            ">" => Self::Gt,
            ">=" => Self::Ge,
            _ => anyhow::bail!("unsupported comparison operator"),
        })
    }

    /// Complement a strict comparison while preserving UNKNOWN.
    pub(super) const fn negate(self) -> Self {
        match self {
            Self::Eq => Self::Ne,
            Self::Ne => Self::Eq,
            Self::Lt => Self::Ge,
            Self::Le => Self::Gt,
            Self::Gt => Self::Le,
            Self::Ge => Self::Lt,
        }
    }
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
