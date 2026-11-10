//! The shared SQL history matrix, using virtual time and frozen native oracles.
use super::*;

pub(super) struct Binding {
    pub native: Contract,
    pub compiled: crate::compiler::Compiled,
}

pub(super) fn bind(case: &Case) -> Result<Binding> {
    let mut native = native();
    if let Some(relations) = &case.relations {
        native.relations.clone_from(relations);
    }
    let mut compiled = crate::compiler::compile(&case.sql, &native)?;
    if let Some(columns) = &case.output {
        let output = &compiled.output().columns;
        ensure!(
            output.len() == columns.len()
                && output
                    .iter()
                    .zip(columns)
                    .all(|(actual, expected)| actual.column.oid == expected.oid),
            "semantic native output types differ: expected={columns:?}"
        );
        // Production QueryDefinition::describe hydrates this layout from PG.
        // Replay hydrates it from the frozen native description instead.
        compiled.result_columns.clone_from(columns);
    }
    for sql in &case.rejected {
        ensure!(
            crate::compiler::compile(sql, &native).is_err(),
            "expected semantic rejection: {sql}"
        );
    }
    Ok(Binding { native, compiled })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Matrix {
    version: u32,
    cases: Vec<Case>,
}

#[tokio::test(start_paused = true)]
async fn sql_semantics_virtual_clock() -> Result<()> {
    let bytes = if let Some(path) = std::env::var_os("PGDERIVE_SEMANTICS_REPLAY") {
        std::fs::read(path)?
    } else {
        include_bytes!("../../../../tests/fixtures/semantics.json.zst").to_vec()
    };
    let matrix: Matrix = serde_json::from_reader(zstd::stream::Decoder::new(bytes.as_slice())?)?;
    ensure!(matrix.version == 1 && !matrix.cases.is_empty(), "invalid semantic matrix");
    validate_cases(&matrix.cases)?;
    let mut reports = Vec::new();
    for case in matrix.cases {
        ensure!(
            case.relations.is_some() && case.output.is_some(),
            "missing native semantic layout"
        );
        let name = case.name.clone();
        let config = lifecycle::Config {
            memtable_bytes: 64 * 1024 * 1024,
            checkpoint_interval_ms: 3,
            maintenance_ticks: 2,
            source_rows_per_second: 1000,
        };
        let result = run(case, crate::engine::execution::Limits::default(), config).await;
        reports.push(result.with_context(|| format!("semantic case {name}"))?);
    }
    if let Some(path) = std::env::var_os("PGDERIVE_SEMANTICS_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&reports)?)?;
    }
    Ok(())
}
