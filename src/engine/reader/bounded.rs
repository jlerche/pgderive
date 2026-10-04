use anyhow::{Result, ensure};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use serde::Serialize;
use std::{
    io::{self, Write},
    sync::Arc,
};

pub(super) const MAX_BYTES: usize = 8 * 1024 * 1024;

struct Buffer(Vec<u8>);

impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::other("immutable block/index exceeds byte limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut buffer = Buffer(Vec::new());
    serde_json::to_writer(&mut buffer, value)?;
    Ok(buffer.0)
}

pub(super) async fn put(store: &Arc<dyn ObjectStore>, path: &str, bytes: Vec<u8>) -> Result<()> {
    let path = Path::from(path);
    let options = PutOptions { mode: PutMode::Create, ..PutOptions::default() };
    match store.put_opts(&path, bytes.clone().into(), options).await {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            ensure!(
                store.head(&path).await?.size == u64::try_from(bytes.len())?,
                "immutable size mismatch"
            );
            ensure!(
                store.get_range(&path, 0..u64::try_from(bytes.len())?).await?.as_ref() == bytes,
                "immutable object collision or corruption"
            );
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}
