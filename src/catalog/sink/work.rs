//! Successful logical occurrence mutations, classified by the existing sink path.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct Mutations {
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
}
