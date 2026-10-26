//! Controlled request accounting and failure injection, never cloud timing claims.
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, memory::InMemory,
    path::Path,
};
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

pub type RangeRequests = BTreeMap<(String, u64, u64), usize>;

#[derive(Debug, Default)]
pub struct Store {
    inner: InMemory,
    pub(crate) puts: AtomicUsize,
    pub(crate) deletes: AtomicUsize,
    pub(crate) put_bytes: AtomicUsize,
    pub(crate) reads: AtomicUsize,
    pub(crate) range_start: AtomicU64,
    pub(crate) range_end: AtomicU64,
    pub(crate) range_bytes: AtomicU64,
    pub(crate) active: AtomicUsize,
    pub(crate) peak: AtomicUsize,
    pub(crate) fail_at: Option<usize>,
    unpaced: bool,
    pub(crate) read_delay: std::time::Duration,
    pub(crate) read_active: AtomicUsize,
    pub(crate) read_peak: AtomicUsize,
    range_history: Option<Mutex<RangeRequests>>,
}
struct InFlight<'a>(&'a AtomicUsize);
impl<'a> InFlight<'a> {
    fn enter(active: &'a AtomicUsize, peak: &AtomicUsize) -> Self {
        peak.fetch_max(active.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
        Self(active)
    }
}
impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Store {
    pub fn unpaced() -> Self {
        Self { unpaced: true, ..Self::default() }
    }
    pub(crate) fn record_ranges(&mut self) {
        self.range_history = Some(Mutex::default());
    }
    pub(crate) fn take_ranges(&self) -> anyhow::Result<RangeRequests> {
        let Some(history) = &self.range_history else { return Ok(BTreeMap::new()) };
        let mut history = history.lock().map_err(|_| anyhow::anyhow!("range history poisoned"))?;
        Ok(std::mem::take(&mut *history))
    }
    fn record_range(&self, path: &Path, range: &std::ops::Range<u64>) -> Result<()> {
        let Some(history) = &self.range_history else { return Ok(()) };
        let mut history = history.lock().map_err(|_| object_store::Error::Generic {
            store: "range-probe",
            source: Box::new(std::io::Error::other("range history poisoned")),
        })?;
        let key = (path.to_string(), range.start, range.end);
        if history.len() >= 65536 && !history.contains_key(&key) {
            return Err(object_store::Error::Generic {
                store: "range-probe",
                source: Box::new(std::io::Error::other("range history capacity exceeded")),
            });
        }
        *history.entry(key).or_default() += 1;
        drop(history);
        Ok(())
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
        self.put_bytes.fetch_add(data.content_length(), Ordering::SeqCst);
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
            self.record_range(path, range)?;
            self.range_start.store(range.start, Ordering::SeqCst);
            self.range_end.store(range.end, Ordering::SeqCst);
            self.range_bytes.fetch_add(range.end - range.start, Ordering::SeqCst);
            if !self.read_delay.is_zero() {
                let _in_flight = InFlight::enter(&self.read_active, &self.read_peak);
                tokio::time::sleep(self.read_delay).await;
            }
        }
        self.inner.get_opts(path, opts).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
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
    let (path, range) = object.block_range(ordinal).await?.context("missing fixture block")?;
    let path = Path::from(path);
    let mut bytes = store.get(&path).await?.bytes().await?.to_vec();
    *bytes.get_mut(usize::try_from(range.start)?).context("missing fixture range")? ^= 1;
    store.put(&path, bytes.into()).await?;
    Ok(())
}

/// Total requested bytes and interval union; callers retain the original keys privately.
pub fn range_totals(requests: &RangeRequests) -> anyhow::Result<(u64, u64)> {
    let mut total = 0_u64;
    let mut unique = 0_u64;
    let mut prior: Option<(&str, u64)> = None;
    for ((path, start, end), count) in requests {
        total += u64::try_from(*count)? * (end - start);
        let covered =
            prior.filter(|(old, _)| *old == path).map_or(*start, |(_, end)| end.max(*start));
        unique += end.saturating_sub(covered);
        prior = Some((path, covered.max(*end)));
    }
    Ok((total, unique))
}

#[async_trait]
impl crate::storage::PageStore for Store {
    async fn page(
        &self,
        prefix: &str,
        token: Option<String>,
        limit: usize,
    ) -> Result<object_store::list::PaginatedListResult> {
        crate::storage::PageStore::page(&self.inner, prefix, token, limit).await
    }
}
