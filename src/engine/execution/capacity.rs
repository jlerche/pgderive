/// A permanent resource-policy rejection. Retrying unchanged input cannot fix it.
#[derive(Debug)]
pub struct CapacityExceeded(pub(crate) &'static str);
impl std::fmt::Display for CapacityExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}
impl std::error::Error for CapacityExceeded {}

/// A row-dependent expression failure. Later source changes can remove the row.
#[derive(Debug)]
pub struct EvaluationFailed;
impl std::fmt::Display for EvaluationFailed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("query expression evaluation failed")
    }
}
impl std::error::Error for EvaluationFailed {}

/// A compiled plan contradicted its own bound types; no source change can repair it.
#[derive(Debug)]
pub struct PlanFault(pub(crate) &'static str);
impl std::fmt::Display for PlanFault {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}
impl std::error::Error for PlanFault {}
