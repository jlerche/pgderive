use super::{Terminal, Transform};
use anyhow::Result;
#[test]
fn signature_allowlist_and_typed_rendering() -> Result<()> {
    let map = Terminal::new(
        vec![
            Transform::Abs,
            Transform::Abs,
            Transform::Abs,
            Transform::Length,
            Transform::Identity,
        ],
        vec![20, 21, 23, 1043, 2950],
    )?;
    assert_eq!(map.signatures()?.len(), 4);
    assert!(map.validate().is_err());
    assert!(map.expression(0)?.contains("::pg_catalog.int8"));
    assert!(map.expression(1)?.contains("::pg_catalog.int2"));
    assert!(map.expression(2)?.contains("::pg_catalog.int4"));
    assert!(map.expression(3)?.contains("pg_catalog.length"));
    assert_eq!(map.expression(4)?, "tuple->1->4");
    for (transforms, types) in [
        (vec![], vec![]),
        (vec![Transform::Abs], vec![25]),
        (vec![Transform::Length], vec![23]),
        (vec![Transform::Identity], vec![23]),
        (vec![Transform::Abs], vec![23, 20]),
    ] {
        assert!(Terminal::new(transforms, types).is_err());
    }
    Ok(())
}

#[test]
fn numeric_maps_bind_exact_signatures_and_json_numbers() -> Result<()> {
    let map = Terminal::new(vec![Transform::Numeric, Transform::Average], vec![1700, 1700])?;
    assert_eq!(map.signatures()?.len(), 3);
    assert!(map.expression(0)?.contains("::pg_catalog.numeric"));
    assert!(map.expression(1)?.contains("pg_catalog.numeric_div"));
    assert!(Terminal::new(vec![Transform::Average], vec![20]).is_err());
    let exact = "9223372036854775806.6666666666666667";
    let value: serde_json::Value = serde_json::from_str(exact)?;
    assert_eq!(serde_json::to_string(&value)?, exact);
    Ok(())
}

#[test]
fn array_maps_parameterize_delimiters_and_validate_bounds() -> Result<()> {
    let delimiter = "'\\🙂".to_string();
    let map = Terminal::new(
        vec![Transform::ArrayElement { delimiter: delimiter.clone(), index: 4 }],
        vec![25],
    )?;
    assert!(map.has_array());
    assert_eq!(map.signatures()?, [("pg_catalog.string_to_array(text,text)", "text_to_array")]);
    let expression = map.expression(0)?;
    assert!(!expression.contains(&delimiter));
    assert!(expression.contains("($2::pg_catalog.jsonb)->0->'ArrayElement'->>'delimiter'"));
    assert!(expression.contains("COLLATE pg_catalog.\"C\""));
    assert!(expression.contains(")[4]"));
    for (delimiter, index, oid) in
        [("", 1, 25), ("\0", 1, 25), ("/", 0, 25), ("/", 1025, 25), ("/", 4, 23)]
    {
        assert!(
            Terminal::new(
                vec![Transform::ArrayElement { delimiter: delimiter.into(), index }],
                vec![oid]
            )
            .is_err()
        );
    }
    assert!(
        Terminal::new(
            vec![Transform::ArrayElement { delimiter: "/".repeat(65), index: 4 }],
            vec![1043]
        )
        .is_err()
    );
    assert!(!Terminal::new(vec![Transform::Length], vec![25])?.has_array());
    Ok(())
}
#[test]
fn old_terminal_environment_keeps_serialized_identity() -> Result<()> {
    let value = serde_json::json!({"server":"170011","encoding":"UTF8","functions":[]});
    let environment: super::Environment = serde_json::from_value(value.clone())?;
    assert!(environment.collations.is_empty());
    assert_eq!(serde_json::to_value(&environment)?, value);
    Ok(())
}
