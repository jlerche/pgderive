use super::bounded;
use anyhow::{Result, ensure};

pub(super) const STORED_LIMIT: usize = bounded::MAX_BYTES + 65_536;
pub(super) fn compress(bytes: &[u8]) -> Result<Vec<u8>> {
    let encoded = zstd::bulk::compress(bytes, 3)?;
    ensure!(encoded.len() <= STORED_LIMIT, "compressed block exceeds stored byte limit");
    Ok(encoded)
}
pub(super) fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
    ensure!(
        zstd::zstd_safe::find_frame_compressed_size(bytes)
            .map_err(|code| anyhow::anyhow!("invalid Zstd frame: {code}"))?
            == bytes.len(),
        "trailing data after compressed block"
    );
    let mut decoder = zstd::bulk::Decompressor::new()?;
    decoder.set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(23))?;
    Ok(decoder.decompress(bytes, bounded::MAX_BYTES)?)
}

#[cfg(test)]
mod tests {
    use super::{bounded, compress, decode};
    use anyhow::Result;
    #[test]
    fn bounded_frames_reject_plain_json() -> Result<()> {
        let data = vec![b'x'; 100_000];
        let encoded = compress(&data)?;
        assert!(encoded.len() < 1000);
        assert_eq!(decode(&encoded)?, data);
        assert!(decode(b"[]").is_err());
        assert!(decode(b"invalid").is_err());
        let mut trailing = encoded.clone();
        trailing.extend_from_slice(&encoded);
        assert!(decode(&trailing).is_err());
        assert!(decode(&compress(&vec![0; bounded::MAX_BYTES + 1])?).is_err());
        Ok(())
    }
}
