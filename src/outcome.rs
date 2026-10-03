use anyhow::Result;

// Keep the primary failure's chain and include evidence of secondary failures.
pub fn combine(primary: Result<()>, secondary: Result<()>, stage: &str) -> Result<()> {
    match (primary, secondary) {
        (Err(primary), Err(secondary)) => {
            Err(primary.context(format!("{stage} also failed: {secondary:#}")))
        }
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

#[cfg(test)]
mod tests;
