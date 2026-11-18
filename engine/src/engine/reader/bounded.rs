use anyhow::{Result, ensure};
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use std::{
    io::{self, Write},
    sync::Arc,
};

pub(super) const MAX_BYTES: usize = 8 * 1024 * 1024;

struct Buffer(Vec<u8>);

impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        check_length(self.0.len(), bytes.len())?;
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn check_length(retained: usize, additional: usize) -> io::Result<()> {
    if additional > MAX_BYTES.saturating_sub(retained) {
        return Err(io::Error::other("immutable block/index exceeds byte limit"));
    }
    Ok(())
}

struct Counter(usize);
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        check_length(self.0, bytes.len())?;
        self.0 += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn encoded_len(value: &impl Serialize) -> Result<usize> {
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

pub(super) fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut buffer = Buffer(Vec::new());
    serde_json::to_writer(&mut buffer, value)?;
    Ok(buffer.0)
}

struct HashWriter {
    counter: Counter,
    digest: Sha256,
}

impl Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.counter.write(bytes)?;
        self.digest.update(bytes);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn encoded_identity(value: &impl Serialize) -> Result<(u64, String)> {
    let mut writer = HashWriter { counter: Counter(0), digest: Sha256::new() };
    serde_json::to_writer(&mut writer, value)?;
    Ok((u64::try_from(writer.counter.0)?, format!("{:x}", writer.digest.finalize())))
}

pub(super) async fn put(store: &Arc<dyn ObjectStore>, path: &str, bytes: Vec<u8>) -> Result<()> {
    let path = Path::from(path);
    let bytes = Bytes::from(bytes);
    let options = PutOptions { mode: PutMode::Create, ..PutOptions::default() };
    match store.put_opts(&path, bytes.clone().into(), options).await {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            ensure!(
                store.head(&path).await?.size == u64::try_from(bytes.len())?,
                "immutable size mismatch"
            );
            ensure!(
                store.get_range(&path, 0..u64::try_from(bytes.len())?).await?.as_ref()
                    == bytes.as_ref(),
                "immutable object collision or corruption"
            );
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_BYTES, encode, encoded_identity, encoded_len, put};
    use anyhow::Result;
    use object_store::{ObjectStore, ObjectStoreExt as _, memory::InMemory, path::Path};
    use std::sync::Arc;

    #[test]
    fn measured_lengths_match_encoded_json_and_byte_limit_failures() -> Result<()> {
        for value in [
            serde_json::json!([null, -12, true, "quoted \"\n\t\\", "é🙂"]),
            serde_json::json!({"key": [1, 2, 3], "other": {"nested": false}}),
            serde_json::json!("x".repeat(MAX_BYTES - 2)),
        ] {
            let bytes = encode(&value)?;
            assert_eq!(encoded_len(&value)?, bytes.len());
            assert_eq!(
                encoded_identity(&value)?,
                (u64::try_from(bytes.len())?, super::super::format::hash(&bytes))
            );
        }
        let oversized = "x".repeat(MAX_BYTES - 1);
        assert_eq!(
            encoded_len(&oversized).err().map(|error| error.to_string()),
            encode(&oversized).err().map(|error| error.to_string())
        );
        assert!(encoded_len(&oversized).is_err());
        assert_eq!(
            encoded_identity(&oversized).err().map(|error| error.to_string()),
            encode(&oversized).err().map(|error| error.to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn repeated_put_requires_exact_existing_payload() -> Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let expected = vec![42; 8192];
        put(&store, "immutable", expected.clone()).await?;
        put(&store, "immutable", expected.clone()).await?;
        let mut corrupted = expected.clone();
        corrupted[4096] = 43;
        assert!(put(&store, "immutable", corrupted).await.is_err());
        assert!(put(&store, "immutable", vec![42; 8191]).await.is_err());
        let retained = store.get(&Path::from("immutable")).await?.bytes().await?;
        assert_eq!(retained.as_ref(), expected);
        Ok(())
    }
}
