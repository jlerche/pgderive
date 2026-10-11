use super::{Expression, Function, Literal, Output, Program};
use anyhow::Result;
fn program(expressions: Vec<Expression>, predicate: Option<usize>) -> Program {
    let expression = expressions.len().saturating_sub(1);
    Program {
        expressions,
        outputs: vec![Output { label: "visible".into(), expression }],
        predicate,
    }
}
#[test]
fn program_validates_acyclic_types_and_parameterizes_literals() -> Result<()> {
    let value = "'\\🙂";
    let program = program(
        vec![
            Expression::Input(0),
            Expression::Literal(Literal::Text(value.into())),
            Expression::Literal(Literal::Text(String::new())),
            Expression::Call { function: Function::Replace, arguments: vec![0, 1, 2] },
        ],
        None,
    );
    assert_eq!(program.validate(&[25])?, [25, 25, 25, 25]);
    assert_eq!(
        program.signatures(&[25])?,
        [("pg_catalog.replace(text,text,text)", "replace_text")]
    );
    let (columns, predicate) = program.render(&[25])?;
    assert_eq!(predicate, "TRUE");
    assert!(!columns.contains(value));
    assert!(columns.contains("$2::pg_catalog.jsonb"));
    assert!(columns.contains("pg_catalog.replace"));
    assert_eq!(serde_json::from_value::<Program>(serde_json::to_value(&program)?)?, program);
    Ok(())
}
#[test]
fn malformed_programs_fail_before_environment_binding() {
    for expressions in [
        vec![],
        vec![Expression::Input(1)],
        vec![Expression::Input(0), Expression::Not(0)],
        vec![Expression::Call { function: Function::Length, arguments: vec![0] }],
        vec![Expression::Literal(Literal::Numeric("1.2.3".into()))],
        vec![Expression::Literal(Literal::Text("\0".into()))],
        vec![Expression::Input(0), Expression::And(vec![0])],
        vec![Expression::Input(0), Expression::Case { branches: vec![(0, 0)], otherwise: 0 }],
    ] {
        assert!(program(expressions, None).validate(&[25]).is_err());
    }
    assert!(program(vec![Expression::Input(0)], Some(0)).validate(&[25]).is_err());
    assert!(program(vec![Expression::Input(0)], None).validate(&[1700]).is_err());
    for value in ["", ".", "NaN", "1e3", "-1", "'", "1..0"] {
        assert!(
            program(vec![Expression::Literal(Literal::Numeric(value.into()))], None)
                .validate(&[23])
                .is_err()
        );
    }
}
#[test]
fn pure_boolean_case_preserves_native_three_valued_rendering() -> Result<()> {
    let program = program(
        vec![
            Expression::Input(0),
            Expression::Input(1),
            Expression::And(vec![0, 1]),
            Expression::Or(vec![0, 1]),
            Expression::Not(2),
            Expression::Case { branches: vec![(3, 0)], otherwise: 1 },
        ],
        Some(4),
    );
    assert_eq!(program.validate(&[16, 16])?, [16; 6]);
    let (columns, predicate) = program.render(&[16, 16])?;
    assert!(columns.contains("CASE WHEN"));
    assert!(predicate.contains("IS TRUE"));
    assert!(predicate.contains("NOT"));
    Ok(())
}
