//! A separate hard cap for retained native views and outstanding decoded handles.
use std::{
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

/// Query-wide cap on native payloads retained by immutable values.
pub struct NativeBudget {
    limit: usize,
    used: AtomicUsize,
}
impl NativeBudget {
    /// Create a budget; zero disables retained native views.
    #[must_use]
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self { limit, used: AtomicUsize::new(0) })
    }
    /// Reserved bytes, including values pinned by outstanding decoded handles.
    #[must_use]
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
    fn reserve(self: &Arc<Self>, bytes: usize) -> Option<Reservation> {
        let mut previous = self.used();
        loop {
            let next = previous.checked_add(bytes)?;
            if next > self.limit {
                return None;
            }
            match self.used.compare_exchange_weak(
                previous,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(Reservation { budget: self.clone(), bytes }),
                Err(current) => previous = current,
            }
        }
    }
}
struct Reservation {
    budget: Arc<NativeBudget>,
    bytes: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
pub(super) struct Charged<T> {
    value: T,
    reservation: Reservation,
}
/// An immutable decoded handle. The handle itself owns its memory reservation,
/// including after its originating canonical row is evicted or mutated.
pub struct Decoded<T>(DecodedValue<T>);
enum DecodedValue<T> {
    Transient(T),
    Shared(Arc<Charged<T>>),
}
impl<T: Clone> Clone for Decoded<T> {
    fn clone(&self) -> Self {
        match &self.0 {
            DecodedValue::Transient(value) => Self::transient(value.clone()),
            DecodedValue::Shared(value) => Self::shared(value.clone()),
        }
    }
}
impl<T> Deref for Decoded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        match &self.0 {
            DecodedValue::Transient(value) => value,
            DecodedValue::Shared(value) => &value.value,
        }
    }
}
impl<T> Decoded<T> {
    /// Own a transient decoding inline, without a cache or wrapper allocation.
    #[must_use]
    pub const fn transient(value: T) -> Self {
        Self(DecodedValue::Transient(value))
    }
    pub(super) const fn shared(value: Arc<Charged<T>>) -> Self {
        Self(DecodedValue::Shared(value))
    }
    pub(super) fn belongs_to(&self, budget: &Arc<NativeBudget>) -> bool {
        matches!(&self.0, DecodedValue::Shared(value) if Arc::ptr_eq(&value.reservation.budget, budget))
    }
}
pub(super) fn arc_bytes<T>() -> Option<usize> {
    let header = std::alloc::Layout::new::<[AtomicUsize; 2]>();
    Some(header.extend(std::alloc::Layout::new::<T>()).ok()?.0.pad_to_align().size())
}
pub(super) fn string_arc_bytes(length: usize) -> Option<usize> {
    let header = std::alloc::Layout::new::<[AtomicUsize; 2]>();
    Some(header.extend(std::alloc::Layout::array::<u8>(length).ok()?).ok()?.0.pad_to_align().size())
}
pub(super) fn retained<T>(
    value: T,
    budget: &Arc<NativeBudget>,
    heap_bytes: Option<usize>,
) -> Option<Arc<Charged<T>>> {
    let bytes = heap_bytes?.checked_add(arc_bytes::<Charged<T>>()?)?;
    let reservation = budget.reserve(bytes)?;
    Some(Arc::new(Charged { value, reservation }))
}
