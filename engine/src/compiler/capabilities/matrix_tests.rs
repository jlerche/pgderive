//! Keep the executable `PostgreSQL` inventory equal to the closed capability table.
use super::SIGNATURES;
use anyhow::Result;
use serde::Deserialize;
use std::collections::BTreeSet;

#[derive(Deserialize)]
struct Matrix {
    scalar_functions: Vec<Case>,
    coercion_cases: Vec<Case>,
}
#[derive(Deserialize)]
struct Case {
    name: String,
    input_oids: Vec<u32>,
    result_oid: u32,
    expression: String,
    arguments: Vec<serde_json::Value>,
}

#[test]
fn support_matrix_exactly_covers_closed_signatures_and_resolves_coercions() -> Result<()> {
    let matrix: Matrix =
        serde_json::from_str(include_str!("../../../docs/postgres-support-matrix.json"))?;
    let declared = matrix
        .scalar_functions
        .iter()
        .map(|case| (case.name.clone(), case.input_oids.clone(), case.result_oid))
        .collect::<BTreeSet<_>>();
    let implemented = SIGNATURES
        .iter()
        .map(|signature| (signature.name.to_owned(), signature.inputs.to_vec(), signature.result))
        .collect::<BTreeSet<_>>();
    assert_eq!(declared.len(), matrix.scalar_functions.len(), "duplicate documented signature");
    assert_eq!(
        declared, implemented,
        "update the matrix and native qualification with capabilities"
    );
    for case in matrix.scalar_functions.iter().chain(&matrix.coercion_cases) {
        assert!(!case.expression.is_empty());
        assert_eq!(case.arguments.len(), case.input_oids.len());
        let resolved = super::resolve(&case.name, &case.input_oids)?;
        assert_eq!(resolved.result, case.result_oid, "{}", case.expression);
    }
    Ok(())
}
