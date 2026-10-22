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
    reservation: Option<Reservation>,
}
/// An immutable decoded handle. The handle itself owns its memory reservation,
/// including after its originating canonical row is evicted or mutated.
pub struct Decoded<T>(pub(super) Arc<Charged<T>>);
impl<T> Clone for Decoded<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> Deref for Decoded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0.value
    }
}
impl<T> Decoded<T> {
    /// Own a transient decoding without retaining it in canonical state.
    #[must_use]
    pub fn transient(value: T) -> Self {
        Self(Arc::new(Charged { value, reservation: None }))
    }
    pub(super) fn belongs_to(&self, budget: &Arc<NativeBudget>) -> bool {
        self.0.reservation.as_ref().is_some_and(|permit| Arc::ptr_eq(&permit.budget, budget))
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
) -> Option<Decoded<T>> {
    let bytes = heap_bytes?.checked_add(arc_bytes::<Charged<T>>()?)?;
    let reservation = budget.reserve(bytes)?;
    Some(Decoded(Arc::new(Charged { value, reservation: Some(reservation) })))
}
