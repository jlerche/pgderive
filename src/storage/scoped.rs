//! One request budget for streamed object operations and stateless GC pages.
use crate::engine::execution::{WorkspacePermit, Workspaces};
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
mod accounting;
use accounting::Accounting;
pub use accounting::ServiceStats;

/// Object operations plus bounded, opaque-token listing. No ordering assumption.
#[async_trait]
pub trait PageStore: ObjectStore {
    /// Runtime API service counters; raw harness adapters have no admission wrapper.
    fn service_stats(&self) -> Option<ServiceStats> {
        None
    }
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
    requests: Arc<Workspaces>,
    accounting: Accounting,
    prefix: Path,
}
impl Scoped {
    pub(super) fn new(
        inner: Arc<dyn ObjectStore>,
        pages: Arc<dyn PageStore>,
        prefix: Path,
        requests: usize,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner,
            pages,
            prefix,
            requests: Arc::new(Workspaces::new(requests)?),
            accounting: Accounting::default(),
        })
    }
}
fn guarded<T: Send + 'static>(
    stream: BoxStream<'static, Result<T>>,
    permit: WorkspacePermit,
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
    fn service_stats(&self) -> Option<ServiceStats> {
        Some(self.accounting.stats(&self.requests))
    }
    async fn page(
        &self,
        prefix: &str,
        token: Option<String>,
        limit: usize,
    ) -> Result<PaginatedListResult> {
        let request = self.accounting.request();
        let _permit = request.permit(&self.requests).await?;
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
        let request = self.accounting.request();
        let _permit = request.permit(&self.requests).await?;
        let bytes = data.content_length();
        request.writing(bytes);
        let result = self.inner.put_opts(path, data, opts).await;
        if result.is_ok() {
            request.written(bytes);
        }
        result
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
        let request = self.accounting.request();
        let permit = request.permit(&self.requests).await?;
        let head = opts.head;
        let estimated = if head {
            0
        } else {
            match &opts.range {
                Some(object_store::GetRange::Bounded(range)) => {
                    range.end.saturating_sub(range.start)
                }
                Some(object_store::GetRange::Suffix(bytes)) => *bytes,
                _ => 0,
            }
        };
        request.reading(estimated);
        let result = self.inner.get_opts(path, opts).await?;
        if !head && estimated == 0 {
            request.reading(result.range.end.saturating_sub(result.range.start));
        }
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        let stream = result
            .into_stream()
            .map(move |chunk| {
                if let Ok(bytes) = &chunk {
                    request.read(bytes.len());
                }
                chunk
            })
            .boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(guarded(stream, permit)),
            meta,
            range,
            attributes,
            extensions,
        })
    }
    async fn get_ranges(
        &self,
        path: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Vec<bytes::Bytes>> {
        let request = self.accounting.request();
        let _permit = request.permit(&self.requests).await?;
        for range in ranges {
            request.reading(range.end.saturating_sub(range.start));
        }
        let result = self.inner.get_ranges(path, ranges).await?;
        for bytes in &result {
            request.read(bytes.len());
        }
        Ok(result)
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = self.inner.clone();
        let requests = self.requests.clone();
        let request = self.accounting.request();
        futures_util::stream::once(async move {
            let permit = request.permit(&requests).await?;
            Ok::<_, object_store::Error>(guarded(inner.delete_stream(paths), permit))
        })
        .try_flatten()
        .boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let inner = self.inner.clone();
        let requests = self.requests.clone();
        let request = self.accounting.request();
        futures_util::stream::once(async move {
            let permit = request.permit(&requests).await?;
            Ok::<_, object_store::Error>(guarded(inner.list(prefix.as_ref()), permit))
        })
        .try_flatten()
        .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let request = self.accounting.request();
        let _permit = request.permit(&self.requests).await?;
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        let request = self.accounting.request();
        let _permit = request.permit(&self.requests).await?;
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
