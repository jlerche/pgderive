use super::bounded;
use anyhow::{Result, ensure};
use serde::Serialize;

pub(super) const fn stored_limit(version: u32) -> usize {
    if version == 3 { bounded::MAX_BYTES + 65_536 } else { bounded::MAX_BYTES }
}
pub(super) fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    compress(&bounded::encode(value)?)
}
pub(super) fn compress(bytes: &[u8]) -> Result<Vec<u8>> {
    let encoded = zstd::bulk::compress(bytes, 3)?;
    ensure!(encoded.len() <= stored_limit(3), "compressed block exceeds stored byte limit");
    Ok(encoded)
}
pub(super) fn decode(bytes: &[u8], version: u32) -> Result<Vec<u8>> {
    if version != 3 {
        return Ok(bytes.to_vec());
    }
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
    fn bounded_frames_and_legacy_json() -> Result<()> {
        let data = vec![b'x'; 100_000];
        let encoded = compress(&data)?;
        assert!(encoded.len() < 1000);
        assert_eq!(decode(&encoded, 3)?, data);
        assert_eq!(decode(b"[]", 2)?, b"[]");
        assert!(decode(b"invalid", 3).is_err());
        let mut trailing = encoded.clone();
        trailing.extend_from_slice(&encoded);
        assert!(decode(&trailing, 3).is_err());
        assert!(decode(&compress(&vec![0; bounded::MAX_BYTES + 1])?, 3).is_err());
        Ok(())
    }
}
