//! Canonical values with a bounded native view behind an immutable identity API.
use anyhow::Result;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    any::Any,
    collections::BTreeMap,
    fmt,
    ops::{Deref, DerefMut},
    sync::Arc,
};

mod budget;
pub use budget::{Decoded, NativeBudget};

type Payload = dyn Any + Send + Sync;
struct View {
    codec: Arc<str>,
    payload: Arc<Payload>,
}
/// Canonical identity with a bounded, immutable native view.
#[derive(Clone)]
pub struct Value<T> {
    canonical: T,
    view: Option<Arc<View>>,
}
impl<T> Value<T> {
    /// Wrap canonical data without allocating native cache storage.
    pub const fn from_canonical(value: T) -> Self {
        Self { canonical: value, view: None }
    }
}
impl<T> Value<T> {
    /// The codec must be deterministic and native payloads immutable. IDs must
    /// include every interpretation parameter. `heap_bytes` must cover all heap
    /// allocations owned by value; wrapper/codec allocations are charged here.
    #[must_use]
    pub fn from_native<D: Send + Sync + 'static>(
        codec: Arc<str>,
        value: D,
        budget: &Arc<NativeBudget>,
        heap_bytes: usize,
        encode: impl FnOnce(&D) -> T,
    ) -> Self {
        let canonical = encode(&value);
        let overhead = budget::arc_bytes::<View>()
            .and_then(|size| size.checked_add(budget::string_arc_bytes(codec.len())?))
            .and_then(|size| size.checked_add(heap_bytes));
        let Some(payload) = budget::retained(value, budget, overhead) else {
            return Self::from_canonical(canonical);
        };
        Self { canonical, view: Some(Arc::new(View { codec, payload: payload.0 })) }
    }
}
impl<T> Value<T> {
    /// Reuse a matching immutable native view owned by this query budget.
    /// Cold canonical values and cross-budget values decode transiently.
    ///
    /// # Errors
    /// Returns decoder failures or a codec ID reused with a different Rust type.
    pub fn decode<D: Send + Sync + 'static>(
        &self,
        codec: &str,
        budget: &Arc<NativeBudget>,
        decode: impl FnOnce(&T) -> Result<D>,
    ) -> Result<Decoded<D>> {
        if let Some(view) = &self.view
            && &*view.codec == codec
        {
            let payload = view
                .payload
                .clone()
                .downcast::<budget::Charged<D>>()
                .map(Decoded)
                .map_err(|_| anyhow::anyhow!("native codec type mismatch"))?;
            if payload.belongs_to(budget) {
                return Ok(payload);
            }
        }
        let value = decode(self)?;
        Ok(Decoded::transient(value))
    }
    /// Iterate over the canonical representation without exposing cache mutation.
    pub fn iter<'a>(&'a self) -> <&'a T as IntoIterator>::IntoIter
    where
        &'a T: IntoIterator,
    {
        self.canonical().into_iter()
    }
    /// Consume the wrapper, moving its canonical data without copying.
    #[must_use]
    pub fn into_canonical(self) -> T {
        self.canonical
    }
    const fn canonical(&self) -> &T {
        &self.canonical
    }
}
impl<T: Default> Value<T> {
    /// Construct a mutable default canonical value.
    #[must_use]
    pub fn new() -> Self {
        Self { canonical: T::default(), view: None }
    }
}
impl<T: Default> Default for Value<T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T> Deref for Value<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.canonical()
    }
}
impl<T> DerefMut for Value<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.view = None;
        &mut self.canonical
    }
}
impl<T: fmt::Debug> fmt::Debug for Value<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.canonical().fmt(f)
    }
}
impl<T: PartialEq> PartialEq for Value<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}
impl<T: Eq> Eq for Value<T> {}
impl<T: PartialOrd> PartialOrd for Value<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (**self).partial_cmp(&**other)
    }
}
impl<T: Ord> Ord for Value<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (**self).cmp(&**other)
    }
}
impl<T: Serialize> Serialize for Value<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        self.canonical().serialize(serializer)
    }
}
impl<'de, T: Deserialize<'de>> Deserialize<'de> for Value<T> {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> std::result::Result<Self, D::Error> {
        Ok(Self::from_canonical(T::deserialize(decoder)?))
    }
}
impl<T, U> FromIterator<U> for Value<T>
where
    T: FromIterator<U>,
{
    fn from_iter<I: IntoIterator<Item = U>>(iter: I) -> Self {
        Self::from_canonical(T::from_iter(iter))
    }
}
impl<K: Ord, V, const N: usize> From<[(K, V); N]> for Value<BTreeMap<K, V>> {
    fn from(rows: [(K, V); N]) -> Self {
        Self::from_canonical(BTreeMap::from(rows))
    }
}
impl<K: Ord, V> From<BTreeMap<K, V>> for Value<BTreeMap<K, V>> {
    fn from(rows: BTreeMap<K, V>) -> Self {
        Self::from_canonical(rows)
    }
}
impl<'a, T> IntoIterator for &'a Value<T>
where
    &'a T: IntoIterator,
{
    type Item = <&'a T as IntoIterator>::Item;
    type IntoIter = <&'a T as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.canonical().into_iter()
    }
}
impl<T: IntoIterator> IntoIterator for Value<T> {
    type Item = T::Item;
    type IntoIter = T::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.canonical.into_iter()
    }
}

impl<T> From<Vec<T>> for Value<Vec<T>> {
    fn from(values: Vec<T>) -> Self {
        Self::from_canonical(values)
    }
}

#[cfg(test)]
mod tests;
