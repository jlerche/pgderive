//! Complete virtual object interface; source GC can never delete a physical pack.
use super::{
    batch::Group,
    error,
    manifest::{Catalog, Chunk},
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{
    StreamExt as _, TryStreamExt as _,
    stream::{self, BoxStream},
};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    list::PaginatedListResult, path::Path,
};
use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

#[derive(Debug)]
pub(super) struct Store {
    pub(super) group: Arc<Group>,
    pub(super) catalog: Catalog,
}
impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("tenant-virtual-object-store")
    }
}
impl Store {
    fn missing(path: &Path) -> object_store::Error {
        object_store::Error::NotFound {
            path: path.to_string(),
            source: "logical object absent".into(),
        }
    }
    async fn meta(&self, path: &Path) -> object_store::Result<ObjectMeta> {
        require(self.catalog.tenant == self.group.tenant, "cross-tenant store binding rejected")?;
        let chunk = self
            .group
            .manifest
            .lookup(&self.catalog, path.as_ref())
            .await
            .map_err(error)?
            .ok_or_else(|| Self::missing(path))?;
        Ok(meta(&chunk))
    }
}
#[async_trait]
impl super::super::PageStore for Store {
    fn tenant_stats(&self) -> Option<serde_json::Value> {
        Some(self.group.stats())
    }
    fn service_stats(&self) -> Option<super::super::scoped::ServiceStats> {
        self.group.store.service_stats().map(|mut stats| {
            stats.shared_tenant = Some(self.catalog.tenant.clone());
            stats
        })
    }
    async fn page(
        &self,
        prefix: &str,
        token: Option<String>,
        limit: usize,
    ) -> object_store::Result<PaginatedListResult> {
        require(self.catalog.tenant == self.group.tenant, "cross-tenant store binding rejected")?;
        let chunks = self
            .group
            .manifest
            .page(&self.catalog, prefix, token.as_deref(), limit)
            .await
            .map_err(error)?;
        let token =
            if chunks.len() == limit { chunks.last().map(|c| c.name.clone()) } else { None };
        Ok(PaginatedListResult {
            result: ListResult {
                extensions: object_store::Extensions::default(),
                common_prefixes: Vec::new(),
                objects: chunks.iter().map(meta).collect(),
            },
            page_token: token,
        })
    }
}
#[async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        require(self.catalog.tenant == self.group.tenant, "cross-tenant store binding rejected")?;
        require(
            matches!(options.mode, PutMode::Create),
            "virtual objects require create-only PUT",
        )?;
        require(options.attributes.is_empty(), "virtual object attributes unsupported")?;
        super::namespace(path.as_ref()).map_err(error)?;
        if self.group.manifest.lookup(&self.catalog, path.as_ref()).await.map_err(error)?.is_some()
        {
            return Err(object_store::Error::AlreadyExists {
                path: path.to_string(),
                source: "immutable logical object exists".into(),
            });
        }
        self.group.put(self.catalog.clone(), path.to_string(), payload).await.map_err(error)
    }
    async fn put_multipart_opts(
        &self,
        _path: &Path,
        _options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        unsupported("virtual store uses bounded ordinary PUTs")
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        require(options.version.is_none(), "virtual object versions unsupported")?;
        let meta = self.meta(path).await?;
        options.check_preconditions(&meta)?;
        let range = options
            .range
            .as_ref()
            .map(|r| r.as_range(meta.size))
            .transpose()
            .map_err(|e| error(e.into()))?
            .unwrap_or(0..meta.size);
        let bytes = if options.head {
            Bytes::new()
        } else {
            self.group
                .manifest
                .read(&self.catalog, path.as_ref(), Some(range.clone()), &self.group.store)
                .await
                .map_err(error)?
                .ok_or_else(|| Self::missing(path))?
                .1
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
            meta,
            range,
            attributes: object_store::Attributes::default(),
            extensions: object_store::Extensions::default(),
        })
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let manifest = self.group.manifest.clone();
        let catalog = self.catalog.clone();
        let tenant = self.group.tenant.clone();
        paths
            .then(move |path| {
                let manifest = manifest.clone();
                let catalog = catalog.clone();
                let tenant = tenant.clone();
                async move {
                    require(catalog.tenant == tenant, "cross-tenant store binding rejected")?;
                    let path = path?;
                    manifest.remove(&catalog, path.as_ref()).await.map_err(error)?;
                    Ok(path)
                }
            })
            .boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let manifest = self.group.manifest.clone();
        let catalog = self.catalog.clone();
        if catalog.tenant != self.group.tenant {
            return stream::once(async {
                Err(error(anyhow::anyhow!("cross-tenant store binding rejected")))
            })
            .boxed();
        }
        let prefix = prefix.map_or_else(String::new, ToString::to_string);
        stream::try_unfold(
            (manifest, catalog, prefix, None, false),
            |(manifest, catalog, prefix, after, done)| async move {
                if done {
                    return Ok::<_, object_store::Error>(None);
                }
                let chunks = manifest
                    .page(&catalog, &prefix, after.as_deref(), 1000)
                    .await
                    .map_err(error)?;
                let done = chunks.len() < 1000;
                let after = chunks.last().map(|c| c.name.clone());
                let objects = chunks.into_iter().map(|chunk| Ok(meta(&chunk)));
                Ok(Some((stream::iter(objects), (manifest, catalog, prefix, after, done))))
            },
        )
        .try_flatten()
        .boxed()
    }
    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        unsupported("virtual GC uses bounded page() listing")
    }
    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        unsupported("virtual objects use immutable create-only PUTs")
    }
}
fn unsupported<T>(message: &str) -> object_store::Result<T> {
    Err(object_store::Error::NotSupported { source: message.to_owned().into() })
}
fn meta(chunk: &Chunk) -> ObjectMeta {
    ObjectMeta {
        location: Path::from(chunk.name.clone()),
        last_modified: (UNIX_EPOCH + Duration::from_micros(chunk.created)).into(),
        size: chunk.length,
        e_tag: Some(chunk.checksum.clone()),
        version: None,
    }
}

fn require(valid: bool, message: &str) -> object_store::Result<()> {
    if valid { Ok(()) } else { Err(error(anyhow::anyhow!("{message}"))) }
}
