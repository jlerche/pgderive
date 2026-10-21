//! Controlled request accounting and failure injection, never cloud timing claims.
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, memory::InMemory, path::Path,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub struct Store {
    inner: InMemory,
    pub(crate) puts: AtomicUsize,
    pub(crate) reads: AtomicUsize,
    pub(crate) active: AtomicUsize,
    pub(crate) peak: AtomicUsize,
    pub(crate) fail_first: bool,
}
impl Store {
    pub fn new(fail_first: bool) -> Self {
        Self { fail_first, ..Self::default() }
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
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        if self.fail_first && call == 0 {
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
