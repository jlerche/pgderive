use super::{ORDER, VALUE, input, measure, token};
use crate::{
    compiler::{compile, relational::Node},
    engine::{Batch, dataflow::TimedBatch, execution::Limits},
    worker::tests::{contract, row},
};
use anyhow::{Context, Result};
#[test]
fn ordered_numeric_codec_matches_signed_order_and_reverses_it() {
    let values = [i64::MIN, -100, -1, 0, 1, 100, i64::MAX];
    let ascending = values.map(|value| token(value, false));
    let descending = values.map(|value| token(value, true));
    assert!(ascending.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(descending.windows(2).all(|pair| pair[0] > pair[1]));
    assert!(ascending.iter().chain(&descending).all(|value| value.as_str() < "1"));
}
#[test]
fn ordered_temporal_codec_infinity_equivalent_offsets_and_filter_null() -> Result<()> {
    let mut native = contract();
    let bid = native.relations.get_mut(1).context("missing bid")?;
    bid.columns.get_mut(2).context("missing price")?.oid = 1184;
    let compiled = compile(
        "SELECT b.auction AS g,MIN(b.price) AS lo,MAX(b.price) FILTER(WHERE b.auction IS NULL) AS hi FROM source.bid b GROUP BY b.auction",
        &native,
    )?;
    let spec = compiled
        .relational()
        .context("missing IR")?
        .nodes
        .iter()
        .find_map(|node| if let Node::OrderedInput { spec, .. } = node { Some(spec) } else { None })
        .context("missing ordered spec")?;
    let aggregate = spec.aggregates.first().context("missing aggregate")?;
    let values =
        ["-infinity", "1999-12-31 23:59:59.999999+00", "2000-01-01 00:00:00+00", "infinity"];
    let measured = values
        .iter()
        .map(|value| measure(aggregate, &row(&[("0:price", Some(value))])))
        .collect::<Result<Vec<_>>>()?;
    assert!(measured.windows(2).all(|pair| pair[0].0 < pair[1].0));
    assert_eq!(
        measure(aggregate, &row(&[("0:price", Some("2000-01-01 01:00:00+01"))]))?,
        measure(aggregate, &row(&[("0:price", Some("2000-01-01 00:00:00+00"))]))?
    );
    let excluded = spec.aggregates.get(1).context("missing filtered aggregate")?;
    assert_eq!(
        measure(excluded, &row(&[("0:auction", Some("1")), ("0:price", Some("infinity"))]))?,
        ("1".into(), None)
    );
    Ok(())
}
#[test]
fn ordered_identity_token_precedes_arbitrary_fields_and_rejects_reserved_collisions() -> Result<()>
{
    let native = contract();
    let compiled = compile(
        "SELECT b.auction AS g,MIN(b.price) AS lo FROM source.bid b GROUP BY b.auction",
        &native,
    )?;
    let spec = compiled
        .relational()
        .context("missing IR")?
        .nodes
        .iter()
        .find_map(|node| if let Node::OrderedInput { spec, .. } = node { Some(spec) } else { None })
        .context("missing spec")?;
    let source =
        row(&[("!quoted", Some("arbitrary")), ("0:auction", Some("1")), ("0:price", Some("-1"))]);
    let batch = Batch::from_updates([((vec!["\"1\"".into()], source.clone()), 1)])?;
    let expanded = input(spec).evaluate(&TimedBatch { time: 1, batch }, Limits::default())?;
    let value = &expanded.batch.iter().next().context("missing expansion")?.0.1;
    assert_eq!(value.keys().next().map(String::as_str), Some(ORDER));
    for reserved in [ORDER, VALUE, ""] {
        let mut source = source.clone();
        source.insert(reserved.into(), None);
        let batch = Batch::from_updates([((vec!["\"1\"".into()], source), 1)])?;
        assert!(input(spec).evaluate(&TimedBatch { time: 1, batch }, Limits::default()).is_err());
    }
    Ok(())
}
