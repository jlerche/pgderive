//! One request budget for streamed object operations and stateless GC pages.
use async_trait::async_trait;
use futures_util::{StreamExt as _, TryStreamExt as _, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    aws::AmazonS3,
    list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore},
    path::Path,
};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Object operations plus bounded, opaque-token listing. No ordering assumption.
#[async_trait]
pub trait PageStore: ObjectStore {
    /// List one page in this store's relative namespace.
    async fn page(
        &self,
        prefix: &str,
        token: Option<String>,
        limit: usize,
    ) -> Result<PaginatedListResult>;
}
#[derive(Debug)]
pub(super) struct Scoped {
    inner: Arc<dyn ObjectStore>,
    pages: Arc<dyn PageStore>,
    requests: Arc<Semaphore>,
    prefix: Path,
}
impl Scoped {
    pub(super) fn new(
        inner: Arc<dyn ObjectStore>,
        pages: Arc<dyn PageStore>,
        prefix: Path,
        requests: usize,
    ) -> Self {
        Self { inner, pages, prefix, requests: Arc::new(Semaphore::new(requests)) }
    }
    async fn permit(&self) -> Result<OwnedSemaphorePermit> {
        self.requests.clone().acquire_owned().await.map_err(admission)
    }
}
fn admission(error: tokio::sync::AcquireError) -> object_store::Error {
    object_store::Error::Generic { store: "shared-request-budget", source: Box::new(error) }
}
fn guarded<T: Send + 'static>(
    stream: BoxStream<'static, Result<T>>,
    permit: OwnedSemaphorePermit,
) -> BoxStream<'static, Result<T>> {
    futures_util::stream::unfold((stream, permit), |(mut stream, permit)| async move {
        stream.next().await.map(|value| (value, (stream, permit)))
    })
    .boxed()
}
impl std::fmt::Display for Scoped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("budgeted-store")
    }
}
#[async_trait]
impl PageStore for Scoped {
    async fn page(
        &self,
        prefix: &str,
        token: Option<String>,
        limit: usize,
    ) -> Result<PaginatedListResult> {
        let _permit = self.permit().await?;
        let prefix = if self.prefix.as_ref().is_empty() {
            prefix.to_owned()
        } else {
            format!("{}/{prefix}", self.prefix)
        };
        let mut page = self.pages.page(&prefix, token, limit).await?;
        for object in &mut page.result.objects {
            let location = object
                .location
                .prefix_match(&self.prefix)
                .ok_or_else(|| object_store::Error::Generic {
                    store: "scoped-page",
                    source: "listing escaped store prefix".into(),
                })?
                .collect();
            object.location = location;
        }
        Ok(page)
    }
}
#[async_trait]
impl ObjectStore for Scoped {
    async fn put_opts(&self, path: &Path, data: PutPayload, opts: PutOptions) -> Result<PutResult> {
        let _permit = self.permit().await?;
        self.inner.put_opts(path, data, opts).await
    }
    async fn put_multipart_opts(
        &self,
        _path: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotSupported {
            source: "Pgderive uses bounded ordinary PUTs; multipart uploads are unsupported".into(),
        })
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> Result<GetResult> {
        let permit = self.permit().await?;
        let result = self.inner.get_opts(path, opts).await?;
        let payload = match result.payload {
            GetResultPayload::Stream(stream) => GetResultPayload::Stream(guarded(stream, permit)),
            file @ GetResultPayload::File(_, _) => {
                drop(permit);
                file
            }
        };
        Ok(GetResult { payload, ..result })
    }
    async fn get_ranges(
        &self,
        path: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Vec<bytes::Bytes>> {
        let _permit = self.permit().await?;
        self.inner.get_ranges(path, ranges).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = self.inner.clone();
        let requests = self.requests.clone();
        futures_util::stream::once(async move {
            let permit = requests.acquire_owned().await.map_err(admission)?;
            Ok::<_, object_store::Error>(guarded(inner.delete_stream(paths), permit))
        })
        .try_flatten()
        .boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let inner = self.inner.clone();
        let requests = self.requests.clone();
        futures_util::stream::once(async move {
            let permit = requests.acquire_owned().await.map_err(admission)?;
            Ok::<_, object_store::Error>(guarded(inner.list(prefix.as_ref()), permit))
        })
        .try_flatten()
        .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let _permit = self.permit().await?;
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        let _permit = self.permit().await?;
        self.inner.copy_opts(from, to, opts).await
    }
}
#[async_trait]
impl PageStore for AmazonS3 {
    async fn page(
        &self,
        prefix: &str,
        token: Option<String>,
        limit: usize,
    ) -> Result<PaginatedListResult> {
        self.list_paginated(
            Some(prefix),
            PaginatedListOptions {
                page_token: token,
                max_keys: Some(limit),
                ..PaginatedListOptions::default()
            },
        )
        .await
    }
}
// In-memory harness adapter: a lexical key token is stable across deletions.
#[async_trait]
impl PageStore for object_store::memory::InMemory {
    async fn page(
        &self,
        prefix: &str,
        token: Option<String>,
        limit: usize,
    ) -> Result<PaginatedListResult> {
        let offset = token.map(Path::from).unwrap_or_default();
        let mut objects = self
            .list_with_offset(None, &offset)
            .try_filter(|object| {
                futures_util::future::ready(object.location.as_ref().starts_with(prefix))
            })
            .take(limit + 1)
            .try_collect::<Vec<_>>()
            .await?;
        let more = objects.len() > limit;
        objects.truncate(limit);
        let page_token =
            if more { objects.last().map(|object| object.location.to_string()) } else { None };
        Ok(PaginatedListResult {
            result: ListResult {
                objects,
                common_prefixes: Vec::new(),
                extensions: object_store::Extensions::default(),
            },
            page_token,
        })
    }
}
