use super::{OutputMap, Terminal};
use anyhow::{Context, Result};
fn expression(map: &Terminal, index: usize) -> Result<String> {
    let mut program = map.program.clone();
    program.outputs = vec![program.outputs.get(index).context("output absent")?.clone()];
    Ok(program.render(&map.types)?.0)
}
#[test]
fn signature_allowlist_and_typed_rendering() -> Result<()> {
    let map = Terminal::new(
        vec![
            OutputMap::Abs,
            OutputMap::Abs,
            OutputMap::Abs,
            OutputMap::Length,
            OutputMap::Identity,
        ],
        vec![20, 21, 23, 1043, 2950],
    )?;
    assert_eq!(map.signatures()?.len(), 4);
    assert!(map.validate().is_err());
    assert!(expression(&map, 0)?.contains("::pg_catalog.int8"));
    assert!(expression(&map, 1)?.contains("::pg_catalog.int2"));
    assert!(expression(&map, 2)?.contains("::pg_catalog.int4"));
    assert!(expression(&map, 3)?.contains("pg_catalog.length"));
    assert!(expression(&map, 4)?.contains("::pg_catalog.uuid"));
    for (transforms, types) in [
        (vec![], vec![]),
        (vec![OutputMap::Abs], vec![25]),
        (vec![OutputMap::Length], vec![23]),
        (vec![OutputMap::Identity], vec![23]),
        (vec![OutputMap::Abs], vec![23, 20]),
    ] {
        assert!(Terminal::new(transforms, types).is_err());
    }
    Ok(())
}

#[test]
fn numeric_maps_bind_exact_signatures_and_json_numbers() -> Result<()> {
    let map = Terminal::new(vec![OutputMap::Numeric, OutputMap::Average], vec![1700, 1700])?;
    assert_eq!(map.signatures()?.len(), 3);
    assert!(expression(&map, 0)?.contains("::pg_catalog.numeric"));
    assert!(expression(&map, 1)?.contains("pg_catalog.numeric_div"));
    assert!(Terminal::new(vec![OutputMap::Average], vec![20]).is_err());
    let exact = "9223372036854775806.6666666666666667";
    let value: serde_json::Value = serde_json::from_str(exact)?;
    assert_eq!(serde_json::to_string(&value)?, exact);
    Ok(())
}

#[test]
fn array_maps_parameterize_delimiters_and_validate_bounds() -> Result<()> {
    let delimiter = "'\\🙂".to_string();
    let map = Terminal::new(
        vec![OutputMap::ArrayElement { delimiter: delimiter.clone(), index: 4 }],
        vec![25],
    )?;
    assert!(map.program.has_collation());
    assert_eq!(map.signatures()?, [("pg_catalog.string_to_array(text,text)", "text_to_array")]);
    let expression = expression(&map, 0)?;
    assert!(!expression.contains(&delimiter));
    assert!(expression.contains("($2::pg_catalog.jsonb)->'expressions'"));
    assert!(expression.contains("COLLATE pg_catalog.\"C\""));
    assert!(expression.contains(")[4]"));
    for (delimiter, index, oid) in
        [("", 1, 25), ("\0", 1, 25), ("/", 0, 25), ("/", 1025, 25), ("/", 4, 23)]
    {
        assert!(
            Terminal::new(
                vec![OutputMap::ArrayElement { delimiter: delimiter.into(), index }],
                vec![oid]
            )
            .is_err()
        );
    }
    assert!(
        Terminal::new(
            vec![OutputMap::ArrayElement { delimiter: "/".repeat(65), index: 4 }],
            vec![1043]
        )
        .is_err()
    );
    assert!(!Terminal::new(vec![OutputMap::Length], vec![25])?.program.has_collation());
    Ok(())
}
#[test]
fn decimal_map_validates_exact_parameter_and_signatures() -> Result<()> {
    for oid in [20, 21, 23] {
        let map =
            Terminal::new(vec![OutputMap::NumericMultiply { factor: "0.908".into() }], vec![oid])?;
        assert!(expression(&map, 0)?.contains("numeric_mul"));
        assert!(!map.program.has_collation());
        assert_eq!(map.signatures()?.len(), 3);
        let rendered = expression(&map, 0)?;
        assert!(rendered.contains("numeric_mul"));
        assert!(rendered.contains("->'Literal'->>'Numeric'"));
        assert!(!rendered.contains("0.908"));
    }
    for factor in ["", ".908", "1.", "1e3", "-1.0", "NaN", "1.0'", "1.0.0"] {
        assert!(
            Terminal::new(vec![OutputMap::NumericMultiply { factor: factor.into() }], vec![20])
                .is_err()
        );
    }
    assert!(
        Terminal::new(
            vec![OutputMap::NumericMultiply { factor: format!("1.{}", "0".repeat(33)) }],
            vec![20]
        )
        .is_err()
    );
    assert!(
        Terminal::new(vec![OutputMap::NumericMultiply { factor: "1.0".into() }], vec![1700])
            .is_err()
    );
    Ok(())
}

#[test]
fn calendar_maps_bind_native_types_and_closed_patterns() -> Result<()> {
    for (oid, utc) in [(1114, false), (1184, true)] {
        for pattern in ["YYYY-MM-DD", "HH24:MI"] {
            let map = Terminal::new(
                vec![OutputMap::TimestampFormat { pattern: pattern.into(), utc }],
                vec![oid],
            )?;
            assert!(map.program.has_calendar());
            assert_eq!(map.signatures()?.len(), if utc { 2 } else { 1 });
            let expression = expression(&map, 0)?;
            assert!(expression.contains(pattern));
            assert_eq!(expression.contains("timezone('UTC'"), utc);
        }
    }
    for (oid, utc, pattern) in [
        (1114, true, "HH24:MI"),
        (1184, false, "HH24:MI"),
        (25, false, "HH24:MI"),
        (1114, false, "Month"),
        (1114, false, ""),
    ] {
        assert!(
            Terminal::new(
                vec![OutputMap::TimestampFormat { pattern: pattern.into(), utc }],
                vec![oid]
            )
            .is_err()
        );
    }
    Ok(())
}
