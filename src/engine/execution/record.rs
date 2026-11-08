use super::CapacityExceeded;
use crate::engine::reader::BatchData;
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use serde::{Deserialize, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::{
    borrow::Cow,
    io::{self, Read, Seek, Write},
    path::PathBuf,
};

#[derive(Clone)]
pub(super) struct Run {
    pub(super) path: PathBuf,
    pub(super) bytes: u64,
}

pub(super) fn read_complete<T: BatchData>(
    reader: &mut (impl Read + Seek),
    limit: usize,
    bytes: u64,
    buffer: &mut Vec<u8>,
) -> Result<Option<(T, BigInt)>> {
    let record = read(reader, limit, buffer)?;
    if record.is_none() {
        ensure!(reader.stream_position()? == bytes, "scratch run length mismatch");
    }
    Ok(record)
}

struct Buffer<'a> {
    bytes: &'a mut Vec<u8>,
    limit: usize,
    exceeded: bool,
}
impl Write for Buffer<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other(CapacityExceeded("scratch record limit exceeded")));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Counter {
    bytes: usize,
    limit: usize,
    exceeded: bool,
}
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes) {
            self.exceeded = true;
            return Err(io::Error::other(CapacityExceeded("scratch record limit exceeded")));
        }
        self.bytes += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(super) fn size_value(value: &impl Serialize, limit: usize) -> Result<usize> {
    let mut counter = Counter { bytes: 0, limit, exceeded: false };
    let encoded = serde_json::to_writer(&mut counter, value);
    ensure!(!counter.exceeded, CapacityExceeded("scratch record limit exceeded"));
    encoded?;
    Ok(counter.bytes)
}
pub(super) fn weighted_size(tuple_bytes: usize, weight: &BigInt, limit: usize) -> Result<usize> {
    check_weight(weight, limit)?;
    let bytes = tuple_bytes
        .checked_add(decimal_len(weight)?)
        .and_then(|bytes| bytes.checked_add(5))
        .context("scratch record size overflow")?;
    ensure!(bytes <= limit, CapacityExceeded("scratch record limit exceeded"));
    Ok(bytes)
}
fn decimal_len(weight: &BigInt) -> Result<usize> {
    let Ok(weight) = i128::try_from(weight) else {
        return Ok(weight.to_string().len());
    };
    let digits = weight.unsigned_abs().checked_ilog10().map_or(1, |log| log + 1);
    Ok(usize::try_from(digits)? + usize::from(weight < 0))
}
pub(super) fn size(tuple: &impl Serialize, weight: &BigInt, limit: usize) -> Result<usize> {
    check_weight(weight, limit)?;
    weighted_size(size_value(tuple, limit)?, weight, limit)
}
fn check_weight(weight: &BigInt, limit: usize) -> Result<()> {
    ensure!(
        weight.bits() <= u64::try_from(limit)?.saturating_mul(3),
        CapacityExceeded("scratch coefficient limit exceeded")
    );
    Ok(())
}
// Keep the coefficient a decimal JSON string without allocating that string.
struct Decimal<'a>(&'a BigInt);
impl Serialize for Decimal<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match i128::try_from(self.0) {
            Ok(weight) => serializer.collect_str(&weight),
            Err(_) => serializer.collect_str(self.0),
        }
    }
}
pub(super) fn encode(
    tuple: &impl Serialize,
    weight: &BigInt,
    limit: usize,
    bytes: &mut Vec<u8>,
) -> Result<()> {
    bytes.clear();
    check_weight(weight, limit)?;
    let mut buffer = Buffer { bytes, limit, exceeded: false };
    let encoded = serde_json::to_writer(&mut buffer, &(tuple, Decimal(weight)));
    ensure!(!buffer.exceeded, CapacityExceeded("scratch record limit exceeded"));
    encoded?;
    Ok(())
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
    ensure!(next <= limit, CapacityExceeded("scratch byte limit exceeded"));
    writer.write_all(&u32::try_from(bytes.len())?.to_le_bytes())?;
    writer.write_all(&Sha256::digest(bytes))?;
    writer.write_all(bytes)?;
    Ok(next)
}
pub(super) fn read<T: BatchData>(
    reader: &mut impl Read,
    limit: usize,
    bytes: &mut Vec<u8>,
) -> Result<Option<(T, BigInt)>> {
    bytes.clear();
    let mut first = [0_u8; 1];
    if reader.read(&mut first)? == 0 {
        return Ok(None);
    }
    let mut length = [0_u8; 4];
    length[0] = first[0];
    reader.read_exact(&mut length[1..])?;
    let length = usize::try_from(u32::from_le_bytes(length))?;
    ensure!(length <= limit, CapacityExceeded("scratch record limit exceeded"));
    let mut expected = [0_u8; 32];
    reader.read_exact(&mut expected)?;
    bytes.resize(length, 0);
    reader.read_exact(bytes)?;
    ensure!(Sha256::digest(bytes.as_slice()).as_slice() == expected, "scratch checksum mismatch");
    let Decoded(tuple, weight): Decoded<'_, T> = serde_json::from_slice(bytes)?;
    Ok(Some((tuple, weight.parse()?)))
}

// Borrow decimal strings directly; JSON escapes use the deserializer scratch buffer.
#[derive(Deserialize)]
struct Decoded<'a, T>(T, #[serde(borrow)] Cow<'a, str>);
