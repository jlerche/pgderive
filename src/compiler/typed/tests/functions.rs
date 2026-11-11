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
        "lpad(NULL,2)",
        "rpad('a',NULL)",
        "lpad('a',2,NULL)",
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
        "round(1)",
        "abs(NULL)",
        "length(1)",
        "substr('x','x')",
        "left('a',1::bigint)",
        "lpad('a',1::bigint)",
        "rpad('a',2,1)",
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
        "lpad('x',2147483647,'🙂')",
        "rpad('x',2147483647)",
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

#[test]
fn padding_budget_counts_bytes_and_lazy_branches_guard_expansion() -> Result<()> {
    let row = [("a".into(), Some("2147483647".into()))].into();
    for sql in ["CASE WHEN a>0 THEN 'ok' ELSE lpad('x',a) END", "COALESCE('ok',rpad('x',a))"] {
        assert_eq!(expression(sql)?.evaluate(&row)?.as_deref(), Some("ok"));
    }
    assert!(expression("rpad('',a,'🙂')")?.evaluate(&row).is_err());
    let row = [("a".into(), Some("4194304".into()))].into();
    let value = expression("rpad('',a,'🙂')")?.evaluate(&row)?;
    assert_eq!(value.as_ref().map(String::len), Some(16 * 1024 * 1024));
    let row = [("a".into(), Some("4194305".into()))].into();
    assert!(expression("lpad('',a,'🙂')")?.evaluate(&row).is_err());
    Ok(())
}

#[test]
fn padding_preserves_character_lengths_and_fill_semantics() -> Result<()> {
    for (sql, expected) in [
        ("lpad('é🙂x',5,'界ab')", "界aé🙂x"),
        ("rpad('é🙂x',5,'界ab')", "é🙂x界a"),
        ("lpad('é🙂x',2)", "é🙂"),
        ("rpad('é',3)", "é  "),
        ("lpad('é',3)", "  é"),
        ("rpad('é🙂x',5,'')", "é🙂x"),
        ("lpad('',3,'🙂é')", "🙂é🙂"),
        ("rpad('abc',0,'x')", ""),
        ("lpad('abc',-2147483648,'x')", ""),
        ("rpad('é',2147483647,'')", "é"),
    ] {
        assert_eq!(
            expression(sql)?.evaluate(&crate::transaction::Row::new())?.as_deref(),
            Some(expected),
            "{sql}"
        );
    }
    Ok(())
}

#[test]
fn sign_and_div_preserve_numeric_types_scale_and_error_boundaries() -> Result<()> {
    for (sql, expected) in [
        ("sign(-1.2500)", Some("-1")),
        ("sign(0.000)", Some("0")),
        ("sign(0.00001)", Some("1")),
        ("sign('NaN'::numeric)", Some("NaN")),
        ("sign('-Infinity'::numeric)", Some("-1")),
        ("sign('Infinity'::numeric)", Some("1")),
        ("sign(NULL::numeric)", None),
        ("div(5.999, 2.0)", Some("2")),
        ("div(-5.999, 2.0)", Some("-2")),
        ("div(5.999, -2.0)", Some("-2")),
        ("div(-5.999, -2.0)", Some("2")),
        ("div(1::smallint, 2::bigint)", Some("0")),
        ("div('-9223372036854775808'::bigint, -1)", Some("9223372036854775808")),
        ("div(' 7.99 ', '2')", Some("3")),
        ("div(NULL, NULL)", None),
        ("div('NaN', 0)", Some("NaN")),
        ("div('Infinity', 'Infinity')", Some("NaN")),
        ("div('Infinity', -2)", Some("-Infinity")),
        ("div(1.00, 'Infinity')", Some("0")),
    ] {
        let value = expression(sql)?;
        assert_eq!(value.oid(), 1700, "{sql}");
        assert_eq!(value.modifier(&|_| None), -1, "{sql}");
        assert_eq!(value.evaluate(&crate::transaction::Row::new())?.as_deref(), expected, "{sql}");
    }
    for sql in [
        "sign(1)",
        "sign(NULL)",
        "sign('1')",
        "sign(1.0, 2)",
        "div(1)",
        "div('bad', NULL)",
        "div(TRUE, 2)",
        "div(1, 0)",
        "div('Infinity', 0)",
        "div(NULL, 1/0)",
    ] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    let row = [("a".into(), Some("0".into())), ("b".into(), None)].into();
    for sql in ["div(1, a)", "div(b, 1/a)"] {
        assert!(expression(sql)?.evaluate(&row).is_err(), "{sql}");
    }
    assert_eq!(expression("div(NULL, 1/a)")?.evaluate(&row)?, None);
    Ok(())
}

#[test]
fn variadic_text_joins_skip_nulls_and_keep_empty_arguments() -> Result<()> {
    let row = [("a".into(), Some("0".into())), ("b".into(), None)].into();
    for (sql, expected, nullable) in [
        ("concat(NULL, 'é', NULL, '🙂')", Some("é🙂"), false),
        ("concat(NULL, NULL)", Some(""), false),
        ("concat('')", Some(""), false),
        ("concat_ws('|', '', NULL, '', 'é', '')", Some("||é|"), false),
        ("concat_ws('🙂', NULL, NULL)", Some(""), false),
        ("concat_ws('', 'a', 'b')", Some("ab"), false),
        ("concat_ws(NULL, 'a')", None, true),
        ("concat_ws(CASE WHEN b IS NULL THEN NULL::text ELSE ',' END, 'a')", None, true),
        ("pg_catalog.concat('a', 'b')", Some("ab"), false),
    ] {
        let value = expression(sql)?;
        assert_eq!(value.oid(), 25, "{sql}");
        assert_eq!(value.nullable(), nullable, "{sql}");
        assert_eq!(value.evaluate(&row)?.as_deref(), expected, "{sql}");
    }
    for sql in [
        "concat()",
        "concat_ws(',')",
        "concat('a', 1)",
        "concat_ws(1, 'a')",
        "concat('a'::varchar)",
        "concat(VARIADIC ARRAY['a'])",
        "public.concat('a')",
        "concat_ws(NULL, chr(0))",
    ] {
        assert!(expression(sql).is_err(), "{sql}");
    }
    for sql in ["concat(NULL, chr(a))", "concat_ws(NULL, chr(a))"] {
        assert!(expression(sql)?.evaluate(&row).is_err(), "{sql}");
    }
    Ok(())
}

#[test]
fn variadic_text_join_limit_counts_utf8_separator_bytes() -> Result<()> {
    let row = crate::transaction::Row::new();
    for sql in [
        "concat(repeat('🙂', 4194303), NULL, 'éé')",
        "concat_ws('🙂', repeat('🙂', 4194303), NULL, '')",
    ] {
        let value = expression(sql)?.evaluate(&row)?;
        assert_eq!(value.as_ref().map(String::len), Some(16 * 1024 * 1024), "{sql}");
    }
    for sql in [
        "concat(repeat('🙂', 4194303), NULL, 'ééx')",
        "concat_ws('🙂', repeat('🙂', 4194303), NULL, '', '')",
    ] {
        let value = expression(sql)?;
        assert!(value.evaluate(&row).is_err_and(|error| format!("{error:#}").contains("16 MiB")));
    }
    Ok(())
}
