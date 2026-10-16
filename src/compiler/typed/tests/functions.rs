use super::expression;
use anyhow::Result;
#[test]
fn text_functions_preserve_unicode_empty_and_null_semantics() -> Result<()> {
    for (sql, expected) in [
        ("length('é🙂a\u{301}')", "4"),
        ("char_length('')", "0"),
        ("character_length('é')", "1"),
        ("octet_length('é🙂')", "6"),
        ("bit_length('é🙂')", "48"),
        ("ascii('🙂')", "128578"),
        ("ascii('')", "0"),
        ("chr(128578)", "🙂"),
        ("reverse('é🙂a\u{301}')", "\u{301}a🙂é"),
        ("left('é🙂x',2)", "é🙂"),
        ("right('é🙂x',2)", "🙂x"),
        ("left('é🙂x',-1)", "é🙂"),
        ("right('é🙂x',-1)", "🙂x"),
        ("left('a',-2147483648)", ""),
        ("btrim(' é🙂 ')", "é🙂"),
        ("ltrim('é🙂xé','🙂é')", "xé"),
        ("rtrim('éx🙂é','🙂é')", "éx"),
        ("btrim('éxé','')", "éxé"),
        ("replace('é🙂é','é','x')", "x🙂x"),
        ("replace('abc','','x')", "abc"),
        ("translate('é🙂éx','é🙂é','abz')", "abax"),
        ("translate('é🙂x','é🙂','a')", "ax"),
        ("strpos('é🙂x','🙂')", "2"),
        ("strpos('é','')", "1"),
        ("strpos('é','x')", "0"),
        ("starts_with('é🙂','é')", "t"),
        ("starts_with('é','x')", "f"),
        ("repeat('é🙂',2)", "é🙂é🙂"),
        ("repeat('x',-1)", ""),
        ("split_part('a--b--','--',-1)", ""),
        ("split_part('a--b--','--',-2)", "b"),
        ("split_part('é🙂','',-1)", "é🙂"),
        ("split_part('x','',2)", ""),
        ("split_part('a,b',',',3)", ""),
        ("split_part('a,b',',',-2147483648)", ""),
        ("substr('é🙂xyz',2,2)", "🙂x"),
        ("substring('é🙂xyz',0,3)", "é🙂"),
        ("substr('é🙂xyz',3)", "xyz"),
        ("substr('abc',-5,2)", ""),
        ("substr('abc',2147483647,2147483647)", ""),
        ("to_hex(0-1)", "ffffffff"),
        ("to_hex(0::bigint-1)", "ffffffffffffffff"),
        ("abs(0-7)", "7"),
        ("abs((0-7)::smallint)", "7"),
        ("abs(0::bigint-7)", "7"),
        ("length(reverse('é🙂'))+1", "3"),
        ("COALESCE(NULL::text,reverse('é🙂'))", "🙂é"),
        ("CASE WHEN a>0 THEN reverse('é🙂') ELSE 'no' END", "🙂é"),
        ("length('é'::varchar)", "1"),
    ] {
        let row = [("a".into(), Some("1".into()))].into();
        assert_eq!(expression(sql)?.evaluate(&row)?.as_deref(), Some(expected), "{sql}");
    }
    for sql in [
        "length(NULL)",
        "abs(NULL::integer)",
        "replace('a',NULL,'x')",
        "substr(NULL,1)",
        "chr(NULL)",
        "to_hex(NULL::integer)",
        "to_hex(NULL::bigint)",
        "substr('x',NULL)",
        "substring('x',NULL,2)",
        "substring('x',1,NULL)",
        "starts_with(NULL,'')",
        "repeat('a',NULL)",
    ] {
        assert_eq!(expression(sql)?.evaluate(&crate::transaction::Row::new())?, None, "{sql}");
    }
    Ok(())
}
#[test]
fn unsupported_signatures_and_function_errors_are_explicit() {
    for sql in [
        "right('x',-2147483648)",
        "to_hex(NULL)",
        "to_hex(1::smallint)",
        "substring('x',NULL)",
        "substring('x',NULL,NULL)",
        "abs(1.2)",
        "abs(NULL)",
        "length(1)",
        "substr('x','x')",
        "left('a',1::bigint)",
        "public.length('a')",
        "upper('a')",
        "length('a','b')",
        "abs(DISTINCT a)",
        "abs(a) OVER ()",
        "reverse(1)",
        "COALESCE('a',1)",
        "'a'+1",
        "1::text",
    ] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    for sql in [
        "abs(0::bigint-9223372036854775807-1)",
        "abs((0-32768)::smallint)",
        "abs(0-2147483647-1)",
        "chr(0)",
        "chr(55296)",
        "chr(1114112)",
        "chr(0-1)",
        "split_part('a',',',0)",
        "substr('a',1,0-1)",
        "repeat('xx',2147483647)",
    ] {
        assert!(expression(sql).is_err(), "{sql}");
    }
}

#[test]
fn strict_constant_null_folds_before_row_dependent_errors() -> Result<()> {
    let row = [("a".into(), Some("32768".into()))].into();
    for sql in [
        "left(NULL,a::smallint)",
        "repeat(NULL,a::smallint)",
        "substr(NULL,a::smallint)",
        "replace(NULL,chr(a::smallint),'x')",
    ] {
        assert_eq!(expression(sql)?.evaluate(&row)?, None, "{sql}");
    }
    // Constant child errors still reject during binding, including in NULL calls.
    assert!(expression("left(NULL,32768::smallint)").is_err());
    Ok(())
}
