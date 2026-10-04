use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

/// `PostgreSQL` WAL address, independent of the engine's logical clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Lsn(u64);
impl FromStr for Lsn {
    type Err = anyhow::Error;
    fn from_str(value: &str) -> Result<Self> {
        let (high, low) = value.split_once('/').ok_or_else(|| anyhow::anyhow!("invalid LSN"))?;
        ensure!(
            !high.is_empty() && !low.is_empty() && high.len() <= 8 && low.len() <= 8,
            "invalid LSN"
        );
        let high = u64::from(u32::from_str_radix(high, 16)?);
        let low = u64::from(u32::from_str_radix(low, 16)?);
        Ok(Self((high << 32) | low))
    }
}
impl fmt::Display for Lsn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:X}/{:X}", self.0 >> 32, self.0 & 0xffff_ffff)
    }
}
/// Complete committed source transaction identity for one publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
    /// `PostgreSQL` transaction ID; it is diagnostic identity, not ordering.
    pub xid: u32,
    /// Commit record location.
    pub commit: Lsn,
    /// Position immediately after the committed transaction, used for resume/ACK.
    pub end: Lsn,
}
impl Progress {
    /// Validate and parse the source transaction's WAL positions.
    ///
    /// # Errors
    /// Rejects malformed addresses or an end address before its commit record.
    pub fn new(xid: u32, commit: &str, end: &str) -> Result<Self> {
        let value = Self { xid, commit: commit.parse()?, end: end.parse()? };
        ensure!(value.end >= value.commit, "source end LSN precedes commit LSN");
        Ok(value)
    }
}
