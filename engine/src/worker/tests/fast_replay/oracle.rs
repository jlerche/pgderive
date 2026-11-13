//! Retain every independent oracle without retaining all decoded snapshots.
use super::{Bag, Cells, codec};
use anyhow::{Result, ensure};
use serde::{
    Deserialize, Deserializer,
    de::{Error as _, SeqAccess, Visitor},
};
use std::{collections::BTreeSet, fmt};

pub(super) struct Expected(Vec<u8>);

impl Expected {
    pub(super) fn new(rows: &[(Cells, i64)]) -> Result<Self> {
        let keys = rows.iter().map(|(row, _)| row).collect::<BTreeSet<_>>();
        ensure!(
            keys.len() == rows.len() && rows.iter().all(|(_, weight)| *weight > 0),
            "invalid expected bag"
        );
        drop(keys);
        Ok(Self(codec::encode(rows)?))
    }

    pub(super) fn matches(&self, output: &Bag) -> Result<bool> {
        let mut json = serde_json::Deserializer::from_reader(codec::reader(&self.0)?);
        let equal = serde::Deserializer::deserialize_seq(&mut json, Comparison(output))?;
        json.end()?;
        Ok(equal)
    }
}

/// Native PG permits any retained numeric spelling for these grouped keys.
/// The exporter freezes an explicit per-state set; no numeric decoder is shared.
type NativeChoice = (Option<String>, Vec<Option<String>>);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Representatives {
    column: usize,
    choices: Vec<NativeChoice>,
}
impl Representatives {
    pub(super) fn apply(&self, output: &Bag) -> Result<Bag> {
        let mut aliases = std::collections::BTreeMap::new();
        for (canonical, spellings) in &self.choices {
            ensure!(spellings.contains(canonical), "canonical spelling is not retained");
            for spelling in spellings {
                ensure!(aliases.insert(spelling, canonical).is_none(), "ambiguous native spelling");
            }
        }
        let mut result = Bag::new();
        for (cells, weight) in output {
            ensure!(*weight > 0, "negative native output multiplicity");
            let mut cells = cells.clone();
            let cell = cells
                .get_mut(self.column)
                .ok_or_else(|| anyhow::anyhow!("missing representative column"))?;
            *cell = (*aliases.get(cell).ok_or_else(|| {
                anyhow::anyhow!("numeric spelling is not retained by native source")
            })?)
            .clone();
            let total = result.entry(cells).or_default();
            *total = total
                .checked_add(*weight)
                .ok_or_else(|| anyhow::anyhow!("native multiplicity overflow"))?;
        }
        Ok(result)
    }
}

impl<'de> Deserialize<'de> for Expected {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Self::new(&Vec::<(Cells, i64)>::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

// Compare one decoded row at a time, outside the engine's timed section. The
// snapshot was checked for duplicate keys and positive weights at admission.
struct Comparison<'a>(&'a Bag);

impl<'de> Visitor<'de> for Comparison<'_> {
    type Value = bool;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an independent weighted output bag")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<bool, A::Error> {
        let mut rows = 0;
        let mut equal = true;
        while let Some((cells, weight)) = sequence.next_element::<(Cells, i64)>()? {
            rows += 1;
            equal &= self.0.get(&cells) == Some(&weight);
        }
        Ok(equal && rows == self.0.len())
    }
}

#[test]
fn compressed_oracle_roundtrips_nulls_weights_and_unsorted_rows() -> Result<()> {
    let rows = vec![(vec![Some("z".into()), None], 2), (vec![Some("a".into())], 3)];
    let encoded: Expected = serde_json::from_slice(&serde_json::to_vec(&rows)?)?;
    let mut output = rows.into_iter().collect::<Bag>();
    assert!(encoded.matches(&output)?);
    let key = vec![Some("a".into())];
    output.insert(key.clone(), 4);
    assert!(!encoded.matches(&output)?);
    output.remove(&key);
    assert!(!encoded.matches(&output)?);
    assert!(Expected::new(&[])?.matches(&Bag::new())?);
    Ok(())
}

#[test]
fn malformed_oracles_are_rejected_before_replay() {
    for json in [r#"[[["a"],1],[["a"],2]]"#, r"[[[null],0]]", r#"[[["a"],-1]]"#] {
        assert!(serde_json::from_str::<Expected>(json).is_err());
    }
    assert!(Expected(vec![0, 1, 2]).matches(&Bag::new()).is_err());
}

#[test]
fn retained_oracles_are_compressed_instead_of_decoded_snapshots() -> Result<()> {
    let rows = (0..4096)
        .map(|index| (vec![Some(index.to_string()), Some("same-value".repeat(8))], 1))
        .collect::<Vec<_>>();
    let raw_bytes = serde_json::to_vec(&rows)?.len();
    let encoded = Expected::new(&rows)?;
    assert!(encoded.0.len() < raw_bytes / 8);
    Ok(())
}

#[test]
fn native_oracle_preserves_null_positions_numeric_display_scale_and_weights() -> Result<()> {
    let expected = Expected::new(&[(vec![Some("1.20".into()), None], 2)])?;
    let mut actual = Bag::from([(vec![Some("1.20".into()), None], 2)]);
    assert!(expected.matches(&actual)?);
    actual = Bag::from([(vec![Some("1.2".into()), None], 2)]);
    assert!(!expected.matches(&actual)?);
    actual = Bag::from([(vec![Some("1.20".into()), Some(String::new())], 2)]);
    assert!(!expected.matches(&actual)?);
    actual = Bag::from([(vec![Some("1.20".into()), None], 1)]);
    assert!(!expected.matches(&actual)?);
    Ok(())
}

#[test]
fn only_retained_native_spellings_are_accepted_without_changing_other_cells() -> Result<()> {
    let choices = Representatives {
        column: 0,
        choices: vec![(Some("1.20".into()), vec![Some("1.20".into()), Some("1.2000".into())])],
    };
    let bag =
        |value: &str, weight| Bag::from([(vec![Some(value.into()), Some("5".into())], weight)]);
    assert_eq!(choices.apply(&bag("1.2000", 1))?, bag("1.20", 1));
    assert!(choices.apply(&bag("1.2", 1)).is_err());
    assert!(choices.apply(&bag("1.2000", -1)).is_err());
    let nullable = Bag::from([(vec![Some("1.2000".into()), None], 1)]);
    assert_eq!(choices.apply(&nullable)?, Bag::from([(vec![Some("1.20".into()), None], 1)]));
    let mut duplicated = bag("1.2000", 1);
    duplicated.extend(bag("1.20", 1));
    assert_eq!(choices.apply(&duplicated)?, bag("1.20", 2));
    Ok(())
}
