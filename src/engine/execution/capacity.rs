/// A permanent resource-policy rejection. Retrying unchanged input cannot fix it.
#[derive(Debug)]
pub struct CapacityExceeded(pub(crate) &'static str);
impl std::fmt::Display for CapacityExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}
impl std::error::Error for CapacityExceeded {}
