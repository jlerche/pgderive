use anyhow::{Context, Result};

/// An in-memory commit boundary around an explicitly ordered acyclic graph.
///
/// The graph is Rust evaluation code, not a dynamic planner. Every mutable
/// operator and integrated output must live in `S`. Its `Clone` must create an
/// independent value snapshot: shared mutable handles and external side effects
/// are unsupported. Evaluation must be deterministic and side-effect free.
/// There is no persistence, recursion, or frontier scheduling.
#[derive(Debug)]
pub struct Circuit<S> {
    state: S,
    time: u64,
}

/// Output published only after a complete circuit step succeeds.
#[derive(Debug)]
pub struct CircuitStep<O> {
    /// One transaction-ordered logical tick, unrelated to source LSNs.
    pub time: u64,
    /// Output produced by the successful graph evaluation.
    pub output: O,
}

impl<S: Default> Default for Circuit<S> {
    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<S> Circuit<S> {
    /// Start an owned graph state at logical tick zero.
    pub const fn new(state: S) -> Self {
        Self { state, time: 0 }
    }

    /// Inspect the last successfully committed graph state.
    pub const fn state(&self) -> &S {
        &self.state
    }
}

impl<S: Clone> Circuit<S> {
    /// Evaluate the complete source transaction against staged graph state.
    /// Commit state and advance time only after every graph node succeeds.
    /// Empty transactions also advance time. Callers may retry a failed input;
    /// successful inputs must not be replayed without resetting source state.
    ///
    /// # Errors
    /// Returns evaluation or tick-overflow errors without changing committed
    /// state/time or returning partial output, subject to the ownership contract.
    pub fn step<I, O>(
        &mut self,
        input: &I,
        evaluate: impl FnOnce(&mut S, &I) -> Result<O>,
    ) -> Result<CircuitStep<O>> {
        let time = self.time.checked_add(1).context("circuit logical time overflow")?;
        let mut staged = self.state.clone();
        let output = evaluate(&mut staged, input)?;
        self.state = staged;
        self.time = time;
        Ok(CircuitStep { time, output })
    }
}

#[cfg(test)]
mod tests;
