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

#[test]
fn collated_regex_nodes_are_closed_and_render_native_null_tests() -> Result<()> {
    let program = program(
        vec![
            Expression::Input(0),
            Expression::CollateC(0),
            Expression::Literal(Literal::Text("(&|^)channel_id=([^&]*)".into())),
            Expression::Call { function: Function::RegexMatch, arguments: vec![1, 2] },
            Expression::ArrayElement { input: 3, index: 2 },
            Expression::IsNull { input: 4, negated: true },
        ],
        Some(5),
    );
    assert!(program.has_collation());
    assert_eq!(program.validate(&[1043])?, [1043, 25, 25, 1009, 25, 16]);
    assert_eq!(
        program.signatures(&[1043])?,
        [("pg_catalog.regexp_match(text,text)", "regexp_match_no_flags")]
    );
    let (columns, predicate) = program.render(&[1043])?;
    assert!(columns.contains("IS NOT NULL"));
    assert!(predicate.contains("COLLATE pg_catalog.\"C\""));
    assert!(predicate.contains(")[2]"));
    assert!(!predicate.contains("channel_id="));
    Ok(())
}
#[test]
fn restored_programs_reject_unattested_text_context() {
    for expression in [
        Expression::Call { function: Function::Lower, arguments: vec![0] },
        Expression::Call { function: Function::TextEqual, arguments: vec![0, 0] },
        Expression::Call { function: Function::RegexMatch, arguments: vec![0, 0] },
        Expression::ArrayElement { input: 0, index: 2 },
    ] {
        assert!(program(vec![Expression::Input(0), expression], None).validate(&[25]).is_err());
    }
    let value = program(
        vec![
            Expression::Input(0),
            Expression::CollateC(0),
            Expression::Literal(Literal::Text("arbitrary".into())),
            Expression::Call { function: Function::RegexMatch, arguments: vec![1, 2] },
        ],
        None,
    );
    assert!(value.validate(&[25]).is_err());
}

#[test]
fn pruning_boolean_intermediates_preserves_native_input_indices() -> Result<()> {
    let mut value = program(
        vec![
            Expression::Input(0),
            Expression::Literal(Literal::Text("unreachable".into())),
            Expression::Input(1),
            Expression::Not(0),
            Expression::And(vec![3, 2]),
        ],
        Some(4),
    );
    value.outputs[0].expression = 3;
    value.prune()?;
    assert_eq!(
        value.expressions,
        [
            Expression::Input(0),
            Expression::Input(1),
            Expression::Not(0),
            Expression::And(vec![2, 1])
        ]
    );
    assert_eq!(value.outputs[0].expression, 2);
    assert_eq!(value.predicate, Some(3));
    assert_eq!(value.validate(&[16, 16])?, [16; 4]);
    assert!(program(vec![Expression::Not(0)], None).prune().is_err());
    Ok(())
}
