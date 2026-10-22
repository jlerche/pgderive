//! Exact immutable-batch sizes, reusable across edge checks and residency charges.
use super::ZSet;
use anyhow::{Context, Result, ensure};
use serde::Serialize;

#[derive(Debug, Clone, Copy)]
pub struct BatchSize {
    pub(crate) entries: usize,
    pub(crate) bytes: u64,
    pub(crate) largest: usize,
}
impl<T: Ord + Clone + Serialize> ZSet<T> {
    pub(crate) fn encoded_size(&self, record_limit: usize, byte_limit: u64) -> Result<BatchSize> {
        if let Some(size) = self.sizing.get() {
            ensure!(size.largest <= record_limit, "scratch record limit exceeded");
            ensure!(size.bytes <= byte_limit, "input byte limit exceeded");
            return Ok(*size);
        }
        let mut size = BatchSize { entries: 0, bytes: 0, largest: 0 };
        for row in self.iter() {
            let bytes = super::execution::record_size(&row, record_limit)?;
            size.bytes = size
                .bytes
                .checked_add(u64::try_from(bytes)?)
                .context("input byte count overflow")?;
            ensure!(size.bytes <= byte_limit, "input byte limit exceeded");
            size.largest = size.largest.max(bytes);
            size.entries += 1;
        }
        let _ = self.sizing.set(size);
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use super::ZSet;
    use anyhow::Result;

    #[test]
    fn shared_sizes_respect_limits_and_mutation() -> Result<()> {
        let mut batch = ZSet::from_updates([("quoted\"λ".to_owned(), 2)])?;
        let expected = serde_json::to_vec(&("quoted\"λ", 2_i64))?.len();
        let size = batch.encoded_size(expected, u64::MAX)?;
        assert_eq!(size.bytes, u64::try_from(expected)?);
        let copy = batch.clone();
        assert!(std::sync::Arc::ptr_eq(&batch.sizing, &copy.sizing));
        assert!(copy.encoded_size(expected - 1, u64::MAX).is_err());
        assert_eq!(batch, copy);
        batch.apply(&ZSet::from_updates([("second".to_owned(), 1)])?)?;
        assert!(batch.sizing.get().is_none());
        assert_eq!(copy.encoded_size(expected, u64::MAX)?.entries, 1);
        assert_eq!(batch.encoded_size(1024, u64::MAX)?.entries, 2);
        Ok(())
    }

    #[test]
    fn cloned_batches_share_exact_serialization_work() -> Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
        struct Counted(u64);
        impl serde::Serialize for Counted {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                CALLS.fetch_add(1, Ordering::Relaxed);
                serializer.serialize_u64(self.0)
            }
        }
        let batch = ZSet::from_updates([(Counted(1), 1), (Counted(2), 1)])?;
        let copy = batch.clone();
        batch.encoded_size(1024, u64::MAX)?;
        assert_eq!(CALLS.load(Ordering::Relaxed), 2);
        copy.encoded_size(1024, u64::MAX)?;
        assert_eq!(CALLS.load(Ordering::Relaxed), 2);
        Ok(())
    }

    #[test]
    fn cumulative_limits_never_cache_partial_sizes() -> Result<()> {
        let batch = ZSet::from_updates([("first".to_owned(), 1), ("second".to_owned(), 1)])?;
        assert!(batch.encoded_size(1024, 1).is_err());
        assert!(batch.sizing.get().is_none());
        let size = batch.encoded_size(1024, u64::MAX)?;
        assert!(batch.encoded_size(1024, size.bytes - 1).is_err());
        assert_eq!(batch.encoded_size(1024, size.bytes)?.entries, 2);
        Ok(())
    }

    #[test]
    fn failed_sizing_and_failed_mutation_preserve_cache_contract() -> Result<()> {
        let mut batch = ZSet::from_updates([("long".to_owned(), i64::MAX)])?;
        assert!(batch.encoded_size(1, u64::MAX).is_err());
        assert!(batch.sizing.get().is_none());
        let size = batch.encoded_size(1024, u64::MAX)?;
        let copy = batch.clone();
        assert!(batch.apply(&ZSet::from_updates([("long".to_owned(), 1)])?).is_err());
        assert_eq!(batch, copy);
        assert!(std::sync::Arc::ptr_eq(&batch.sizing, &copy.sizing));
        assert_eq!(batch.encoded_size(1024, u64::MAX)?.bytes, size.bytes);
        Ok(())
    }
}
