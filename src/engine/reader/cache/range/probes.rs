//! Opt-in, per-epoch Bloom-positive first-page candidates, including cache hits.
//! Continuation pages are excluded; these probes never read storage or affect admission.
use crate::engine::reader::{BatchData, index::Leaf};
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};

type Page = (String, String, u64);
type KeyPages = BTreeMap<(String, Vec<u8>), BTreeSet<Page>>;
#[derive(Default)]
pub(super) struct Probes {
    pages: BTreeMap<Page, (String, String, u64)>,
    keys: KeyPages,
    calls: usize,
}
impl Probes {
    pub(super) fn record<K: BatchData, V: BatchData>(
        &mut self,
        schema: &str,
        pack: &str,
        leaf: &Leaf<(K, V)>,
        key: &K,
    ) -> Result<()> {
        let page = (schema.to_owned(), pack.to_owned(), leaf.block.offset);
        self.pages
            .insert(page.clone(), (schema.to_owned(), leaf.block.hash.clone(), leaf.block.length));
        self.keys.entry((schema.to_owned(), serde_json::to_vec(key)?)).or_default().insert(page);
        self.calls += 1;
        Ok(())
    }
    pub(super) fn take(&mut self) -> serde_json::Value {
        let mut schemas = BTreeMap::<String, serde_json::Value>::new();
        let pages = std::mem::take(&mut self.pages);
        let keys = std::mem::take(&mut self.keys);
        let names = pages.values().map(|(schema, _, _)| schema).collect::<BTreeSet<_>>();
        for name in names {
            let selected =
                pages.iter().filter(|(_, (schema, _, _))| schema == name).collect::<Vec<_>>();
            let mut fanout = BTreeMap::<usize, usize>::new();
            for (_, selected) in keys.iter().filter(|((schema, _), _)| schema == name) {
                *fanout.entry(selected.len()).or_default() += 1;
            }
            schemas.insert(name.clone(), serde_json::json!({
                "pages": selected.len(),
                "content_pages":selected.iter().map(|(_, (_, hash, _))| hash).collect::<BTreeSet<_>>().len(),
                "packs":selected.iter().map(|((_, pack, _), _)| pack).collect::<BTreeSet<_>>().len(),
                "compressed_bytes":selected.iter().map(|(_, (_, _, length))| length).sum::<u64>(),
                "key_page_fanout":fanout
            }));
        }
        serde_json::json!({"selection_calls": std::mem::take(&mut self.calls), "schemas":schemas})
    }
}
