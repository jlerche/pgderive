//! Keep committed transaction history compressed until its ingestion step.
use super::{Update, codec};
use anyhow::Result;
use serde::{Deserialize, Deserializer, de::Error as _};

pub(super) struct Updates {
    encoded: Vec<u8>,
    len: usize,
    pub(super) initial_weights: bool,
}

impl Updates {
    pub(super) fn new(rows: &[Update]) -> Result<Self> {
        Ok(Self {
            encoded: codec::encode(rows)?,
            len: rows.len(),
            initial_weights: rows.iter().all(|row| row.weight == 1),
        })
    }

    pub(super) const fn len(&self) -> usize {
        self.len
    }

    pub(super) const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(super) fn decode(&self) -> Result<Vec<Update>> {
        Ok(serde_json::from_reader(codec::reader(&self.encoded)?)?)
    }
}

impl<'de> Deserialize<'de> for Updates {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Self::new(&Vec::<Update>::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

#[test]
fn compressed_transactions_preserve_order_identity_nulls_and_signed_weights() -> Result<()> {
    let json = r#"[{"table":"entry","row":{"id":"1","amount":null},"weight":-1},
                   {"table":"entry","row":{"id":"1","amount":"7"},"weight":1}]"#;
    let retained: Updates = serde_json::from_str(json)?;
    assert_eq!(retained.len(), 2);
    assert!(!retained.is_empty());
    assert!(!retained.initial_weights);
    let rows = retained.decode()?;
    assert_eq!(serde_json::to_value(&rows)?, serde_json::from_str::<serde_json::Value>(json)?);
    let positive = Updates::new(&rows[1..])?;
    assert!(positive.initial_weights);
    assert_eq!(positive.len(), 1);
    let empty = Updates::new(&[])?;
    assert!(empty.is_empty());
    assert!(empty.decode()?.is_empty());
    Ok(())
}

#[test]
fn malformed_transaction_rows_and_corrupt_retention_are_rejected() {
    for json in [
        r#"[{"table":"entry","row":{},"weight":"one"}]"#,
        r#"[{"table":"entry","row":{},"weight":1,"unknown":true}]"#,
        r#"[{"table":"entry","weight":1}]"#,
    ] {
        assert!(serde_json::from_str::<Updates>(json).is_err());
    }
    let corrupt = Updates { encoded: vec![0, 1, 2], len: 1, initial_weights: true };
    assert!(corrupt.decode().is_err());
}

#[test]
fn retained_transaction_history_uses_compressed_bytes() -> Result<()> {
    let rows = (0..4096)
        .map(|index| Update {
            table: "entry".into(),
            row: [("id".into(), Some(index.to_string())), ("amount".into(), Some("7".into()))]
                .into_iter()
                .collect(),
            weight: if index % 2 == 0 { -1 } else { 1 },
        })
        .collect::<Vec<_>>();
    let raw_bytes = serde_json::to_vec(&rows)?.len();
    let retained = Updates::new(&rows)?;
    assert!(retained.encoded.len() < raw_bytes / 8);
    assert_eq!(retained.decode()?.len(), rows.len());
    Ok(())
}
