//! Private replay retention encoding; the fixture JSON contract is unchanged.
use anyhow::Result;
use serde::Serialize;
use std::io::{BufReader, Read};

pub(super) fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    let mut encoder = zstd::stream::Encoder::new(Vec::new(), 1)?;
    serde_json::to_writer(&mut encoder, value)?;
    Ok(encoder.finish()?)
}

pub(super) fn reader(bytes: &[u8]) -> Result<impl Read + '_> {
    Ok(BufReader::new(zstd::stream::Decoder::new(bytes)?))
}
