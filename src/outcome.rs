use anyhow::Result;

// Keep the primary failure's chain and include evidence of secondary failures.
pub fn combine<T>(primary: Result<T>, secondary: Result<()>, stage: &str) -> Result<T> {
    match (primary, secondary) {
        (Err(primary), Err(secondary)) => {
            Err(primary.context(format!("{stage} also failed: {secondary:#}")))
        }
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

#[cfg(test)]
mod tests;
