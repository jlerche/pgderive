// Ephemeral format experiment only. Never publish these objects into a catalog.
use super::{BatchData, Entry};
use anyhow::{Result, ensure};
use bincode::Options;
use std::io::Cursor;
type Columns<K, V> = (Vec<K>, Vec<V>, Vec<i64>);
pub(super) fn encode<K: BatchData, V: BatchData>(rows: &[Entry<K, V>]) -> Result<Vec<u8>> {
    let mode = std::env::var("PGDERIVE_EXPERIMENT_CODEC").unwrap_or_default();
    let binary = mode.contains("binary");
    let columns = mode.contains("column");
    let compressed = mode.contains("zstd");
    let bytes = if columns {
        let keys = rows.iter().map(|row| &row.0.0).collect::<Vec<_>>();
        let values = rows.iter().map(|row| &row.0.1).collect::<Vec<_>>();
        let weights = rows.iter().map(|row| row.1).collect::<Vec<_>>();
        bincode::DefaultOptions::new().serialize(&(keys, values, weights))?
    } else if binary {
        bincode::DefaultOptions::new().serialize(rows)?
    } else {
        serde_json::to_vec(rows)?
    };
    let mut encoded = vec![u8::from(binary || columns), u8::from(columns), u8::from(compressed)];
    if compressed {
        encoded.extend(zstd::stream::encode_all(Cursor::new(bytes), 3)?);
    } else {
        encoded.extend(bytes);
    }
    Ok(encoded)
}
pub(super) fn decode<K: BatchData, V: BatchData>(bytes: &[u8]) -> Result<Vec<Entry<K, V>>> {
    ensure!(bytes.len() > 3, "short experiment block");
    let decompressed;
    let data = if bytes[2] == 1 {
        decompressed = zstd::stream::decode_all(Cursor::new(&bytes[3..]))?;
        &decompressed[..]
    } else {
        &bytes[3..]
    };
    if bytes[1] == 1 {
        let (keys, values, weights): Columns<K, V> =
            bincode::DefaultOptions::new().with_limit(8 * 1024 * 1024).deserialize(data)?;
        ensure!(
            keys.len() == values.len() && values.len() == weights.len(),
            "column lengths differ"
        );
        Ok(keys.into_iter().zip(values).zip(weights).collect())
    } else if bytes[0] == 1 {
        Ok(bincode::DefaultOptions::new().with_limit(8 * 1024 * 1024).deserialize(data)?)
    } else {
        Ok(serde_json::from_slice(data)?)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bincode::Options;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
    #[serde(untagged)]
    enum Domain {
        Integral(i64),
        Text(String),
    }

    #[test]
    fn generic_serde_contract_requires_json_fallback() -> Result<()> {
        let rows = vec![((1_i64, Domain::Integral(7)), 1_i64)];
        let bytes = serde_json::to_vec(&rows)?;
        let restored: Vec<((i64, Domain), i64)> = serde_json::from_slice(&bytes)?;
        assert_eq!(rows, restored);
        let bytes = bincode::DefaultOptions::new().serialize(&rows)?;
        let restored = bincode::DefaultOptions::new()
            .deserialize::<Vec<((i64, Domain), i64)>>(&bytes);
        assert!(restored.is_err());
        Ok(())
    }
}
