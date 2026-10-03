use super::ZSet;
use anyhow::Result;

impl<T: Ord + Clone> ZSet<T> {
    /// Retain tuples matching a fallible predicate, preserving signed weights.
    /// Predicate evaluation must be deterministic for the same tuple.
    ///
    /// # Errors
    /// Returns the predicate's error; the input remains unchanged.
    pub fn try_filter(&self, mut predicate: impl FnMut(&T) -> Result<bool>) -> Result<Self> {
        let mut output = Self::default();
        for (tuple, weight) in self.iter() {
            if predicate(tuple)? {
                output.add(tuple.clone(), *weight)?;
            }
        }
        Ok(output)
    }

    /// Project full tuples, consolidating collisions and removing zero weights.
    /// Projection must be deterministic for the same tuple.
    ///
    /// # Errors
    /// Returns projection or consolidated-weight overflow errors. The input
    /// remains unchanged; no partial output is returned.
    pub fn try_map<U: Ord + Clone>(
        &self,
        mut project: impl FnMut(&T) -> Result<U>,
    ) -> Result<ZSet<U>> {
        let mut output = ZSet::default();
        for (tuple, weight) in self.iter() {
            output.add(project(tuple)?, *weight)?;
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests;
