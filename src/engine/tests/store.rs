//! Controlled request accounting and failure injection, never cloud timing claims.
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, memory::InMemory,
    path::Path,
};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub struct Store {
    inner: InMemory,
    pub(crate) puts: AtomicUsize,
    pub(crate) reads: AtomicUsize,
    pub(crate) range_start: AtomicU64,
    pub(crate) range_end: AtomicU64,
    pub(crate) range_bytes: AtomicU64,
    pub(crate) active: AtomicUsize,
    pub(crate) peak: AtomicUsize,
    pub(crate) fail_at: Option<usize>,
    unpaced: bool,
}
impl Store {
    pub fn unpaced() -> Self {
        Self { unpaced: true, ..Self::default() }
    }
    pub fn new(fail_at: Option<usize>) -> Self {
        Self { fail_at, ..Self::default() }
    }
}
impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("upload-probe")
    }
}
#[async_trait]
impl ObjectStore for Store {
    async fn put_opts(&self, path: &Path, data: PutPayload, opts: PutOptions) -> Result<PutResult> {
        let call = self.puts.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        if !self.unpaced {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
        if self.fail_at == Some(call) {
            return Err(object_store::Error::Generic {
                store: "upload-probe",
                source: Box::new(std::io::Error::other("injected PUT failure")),
            });
        }
        self.inner.put_opts(path, data, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> Result<GetResult> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if let Some(GetRange::Bounded(range)) = &opts.range {
            self.range_start.store(range.start, Ordering::SeqCst);
            self.range_end.store(range.end, Ordering::SeqCst);
            self.range_bytes.fetch_add(range.end - range.start, Ordering::SeqCst);
        }
        self.inner.get_opts(path, opts).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// Damage one authenticated range while leaving other pages in its pack intact.
pub async fn corrupt_block<
    K: crate::engine::reader::BatchData,
    V: crate::engine::reader::BatchData,
>(
    store: &dyn ObjectStore,
    object: &crate::engine::reader::ObjectBatch<K, V>,
    ordinal: usize,
) -> anyhow::Result<()> {
    use anyhow::Context as _;
    use object_store::ObjectStoreExt as _;
    let (path, range) = object.block_range(ordinal).context("missing fixture block")?;
    let path = Path::from(path);
    let mut bytes = store.get(&path).await?.bytes().await?.to_vec();
    *bytes.get_mut(usize::try_from(range.start)?).context("missing fixture range")? ^= 1;
    store.put(&path, bytes.into()).await?;
    Ok(())
}
