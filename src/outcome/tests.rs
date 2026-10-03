use super::combine;
use anyhow::{Result, anyhow};

#[test]
fn retain_both_failure_chains() -> Result<()> {
    let result =
        combine(Err(anyhow!("protocol failure")), Err(anyhow!("slot still active")), "cleanup");
    let error = result.err().ok_or_else(|| anyhow!("expected failure"))?;
    assert_eq!(error.root_cause().to_string(), "protocol failure");
    assert!(format!("{error:#}").contains("slot still active"));
    assert!(combine(Ok(()), Ok(()), "cleanup").is_ok());
    assert!(combine(Err(anyhow!("primary")), Ok(()), "cleanup").is_err());
    assert!(combine(Ok(()), Err(anyhow!("secondary")), "cleanup").is_err());
    Ok(())
}
