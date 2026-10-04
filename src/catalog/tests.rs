use super::{Deltas, Lsn, Progress, Sink, sink::Rows};
use crate::engine::Batch;
use anyhow::Result;
use serde_json::json;

#[test]
fn source_positions_are_distinct_ordered_wal_addresses() -> Result<()> {
    assert!("bad".parse::<Lsn>().is_err());
    for value in ["/0", "0/", "100000000/0", "0/100000000", "0/G", "0/0/0"] {
        assert!(value.parse::<Lsn>().is_err());
    }
    let value: Lsn = "abcdef/12345678".parse()?;
    assert_eq!(value.to_string(), "ABCDEF/12345678");
    assert!("1/0".parse::<Lsn>()? > "0/FFFFFFFF".parse()?);
    assert!(Progress::new(4, "1/10", "1/F").is_err());
    assert_eq!(Progress::new(u32::MAX, "1/10", "1/11")?.end.to_string(), "1/11");
    Ok(())
}
#[test]
fn sink_encodings_preserve_full_tuple_and_group_null_semantics() -> Result<()> {
    let bag = Batch::from_updates([((1, String::from("a")), 2), ((1, String::from("b")), -3)])?;
    let Rows::Bag(rows) = Deltas::bag(&bag)?.rows else { anyhow::bail!("wrong bag encoding") };
    assert_eq!(rows, vec![(json!([1, "a"]), 2), (json!([1, "b"]), -3)]);
    let grouped = Batch::from_updates([((None::<i64>, (1, None)), -1), ((None, (2, Some(0))), 1)])?;
    assert!(matches!(Deltas::grouped(&grouped)?.rows, Rows::Grouped(_)));
    for batch in [
        Batch::from_updates([((1, (1, None)), 2)])?,
        Batch::from_updates([((1, (0, None)), 1)])?,
        Batch::from_updates([((1, (1, None)), 1), ((1, (2, Some(0))), 1)])?,
    ] {
        assert!(Deltas::grouped(&batch).is_err());
    }
    for sink in [Sink::Bag("bad;sql".into()), Sink::Grouped("pgderive_queries".into())] {
        assert!(sink.validate().is_err());
    }
    Sink::Bag("derived".into()).validate()?;
    Ok(())
}
