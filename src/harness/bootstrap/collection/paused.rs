//! Controlled request accounting and failure injection, never cloud timing claims.
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;

#[derive(Debug)]
pub(super) struct Store {
    inner: Arc<dyn crate::storage::PageStore>,
    armed: AtomicBool,
    pub(super) entered: Notify,
    pub(super) release: Notify,
}
impl Store {
    pub(super) fn new(inner: Arc<dyn crate::storage::PageStore>) -> Self {
        Self { inner, armed: AtomicBool::new(true), entered: Notify::new(), release: Notify::new() }
    }
}
impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("gc-pause")
    }
}
#[async_trait]
impl ObjectStore for Store {
    async fn put_opts(&self, path: &Path, data: PutPayload, opts: PutOptions) -> Result<PutResult> {
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
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
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

#[async_trait]
impl crate::storage::PageStore for Store {
    async fn page(
        &self,
        prefix: &str,
        token: Option<String>,
        limit: usize,
    ) -> Result<object_store::list::PaginatedListResult> {
        self.inner.page(prefix, token, limit).await
    }
}
