use super::ColumnRef;
use crate::transaction::Row;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

// JSON native scalars; the ordered, resolved native layout disambiguates text/UUID.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Cell {
    Boolean(bool),
    Integer(i64),
    Text(String),
}
#[derive(Clone, Serialize)]
pub struct OutputColumn {
    pub(crate) column: ColumnRef,
    pub(crate) label: String,
}
#[derive(Clone, Serialize)]
pub struct Projected {
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) columns: Vec<OutputColumn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) terminal: Option<crate::catalog::Terminal>,
}
impl Projected {
    pub(crate) fn row(&self, row: &Row) -> Result<Vec<Option<Cell>>> {
        self.columns
            .iter()
            .map(|output| {
                row.get(&output.column.name)
                    .context("compiled projection column absent")?
                    .as_deref()
                    .map(|value| match output.column.oid {
                        16 => match value {
                            "t" => Ok(Cell::Boolean(true)),
                            "f" => Ok(Cell::Boolean(false)),
                            _ => anyhow::bail!("invalid native boolean"),
                        },
                        20 | 21 | 23 => Ok(Cell::Integer(value.parse()?)),
                        25 | 1043 | 2950 | 1700 => Ok(Cell::Text(value.into())),
                        _ => anyhow::bail!("unsupported native projection codec"),
                    })
                    .transpose()
            })
            .collect()
    }
}
