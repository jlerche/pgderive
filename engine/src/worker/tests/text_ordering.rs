use super::contract;
use crate::compiler::compile;
use anyhow::Result;

#[test]
fn text_extrema_and_ordering_admit_only_supported_collations() -> Result<()> {
    let mut contract = contract();
    let category = &mut contract.relations[0].columns[1];
    category.oid = 1043;
    category.modifier = 16;
    category.collation = 950;
    let compiled = compile(
        "SELECT MIN(DISTINCT category), MAX(category COLLATE \"POSIX\")
         FROM source.auction",
        &contract,
    )?;
    for (output, collation) in compiled.output().columns.iter().zip([950, 951]) {
        assert_eq!(output.column.oid, 25);
        assert_eq!(output.column.collation, collation);
        assert_eq!(output.column.modifier, None);
    }
    for sql in [
        "SELECT category FROM source.auction ORDER BY category COLLATE pg_catalog.pg_c_utf8",
        "SELECT RANK() OVER (ORDER BY category COLLATE ucs_basic) FROM source.auction",
        "SELECT MIN(NULL), MAX('x') FROM source.auction",
        "SELECT id, ROW_NUMBER() OVER (ORDER BY category, id) FROM source.auction",
        "SELECT id, RANK() OVER (ORDER BY id::numeric) FROM source.auction",
        "SELECT NULL::interval UNION ALL SELECT '1 mon' ORDER BY 1",
    ] {
        compile(sql, &contract).map_err(|error| error.context(sql))?;
    }
    for sql in [
        "SELECT MIN(other) FROM source.auction",
        "SELECT RANK() OVER (ORDER BY other) FROM source.auction",
        "SELECT COUNT(*) OVER (ORDER BY category RANGE 1 PRECEDING) FROM source.auction",
    ] {
        assert!(compile(sql, &contract).is_err(), "{sql}");
    }
    Ok(())
}
