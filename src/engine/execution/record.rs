use crate::engine::reader::BatchData;
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::{self, Read, Write};

struct Buffer {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("scratch record limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(super) fn encode(tuple: &impl Serialize, weight: &BigInt, limit: usize) -> Result<Vec<u8>> {
    ensure!(
        weight.bits() <= u64::try_from(limit)?.saturating_mul(3),
        "scratch coefficient limit exceeded"
    );
    encode_value(&(tuple, weight.to_string()), limit)
}
pub(super) fn encode_value(value: &impl Serialize, limit: usize) -> Result<Vec<u8>> {
    let mut buffer = Buffer { bytes: Vec::new(), limit };
    serde_json::to_writer(&mut buffer, value)?;
    Ok(buffer.bytes)
}
pub(super) fn write(
    writer: &mut impl Write,
    bytes: &[u8],
    written: u64,
    limit: u64,
) -> Result<u64> {
    let next = written
        .checked_add(u64::try_from(bytes.len())?.checked_add(36).context("record size overflow")?)
        .context("scratch size overflow")?;
    ensure!(next <= limit, "scratch byte limit exceeded");
    writer.write_all(&u32::try_from(bytes.len())?.to_le_bytes())?;
    writer.write_all(&Sha256::digest(bytes))?;
    writer.write_all(bytes)?;
    Ok(next)
}
pub(super) fn read<T: BatchData>(
    reader: &mut impl Read,
    limit: usize,
) -> Result<Option<(T, BigInt)>> {
    let mut first = [0_u8; 1];
    if reader.read(&mut first)? == 0 {
        return Ok(None);
    }
    let mut length = [0_u8; 4];
    length[0] = first[0];
    reader.read_exact(&mut length[1..])?;
    let length = usize::try_from(u32::from_le_bytes(length))?;
    ensure!(length <= limit, "scratch record limit exceeded");
    let mut expected = [0_u8; 32];
    reader.read_exact(&mut expected)?;
    let mut bytes = vec![0_u8; length];
    reader.read_exact(&mut bytes)?;
    ensure!(Sha256::digest(&bytes).as_slice() == expected, "scratch checksum mismatch");
    let (tuple, weight): (T, String) = serde_json::from_slice(&bytes)?;
    Ok(Some((tuple, weight.parse()?)))
}
