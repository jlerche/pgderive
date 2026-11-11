use super::expression;
use crate::transaction::Row;
use anyhow::Result;

#[test]
fn like_matches_unicode_and_honors_escapes_and_nulls() -> Result<()> {
    for (sql, expected) in [
        (r#"'a🙂é' COLLATE "C" LIKE 'a_%'"#, Some("t")),
        (r#"'🙂é' NOT LIKE '__' COLLATE "POSIX""#, Some("f")),
        (r#"'🙂é' COLLATE "C" NOT LIKE '_'"#, Some("t")),
        (r#"U&'e\0301' COLLATE "C" LIKE '__'"#, Some("t")),
        (r#"'aBc' COLLATE "C" LIKE 'a%c'"#, Some("t")),
        (r#"'aBc' COLLATE "C" LIKE 'ab%'"#, Some("f")),
        (r#"'xaaaabc' COLLATE "C" LIKE '%aab%_'"#, Some("t")),
        (r#"'xaaaabc' COLLATE "C" LIKE '%aab%__'"#, Some("f")),
        (r#"'' COLLATE "C" LIKE '%%'"#, Some("t")),
        (r#"'' COLLATE "C" LIKE ''"#, Some("t")),
        (r#"'' COLLATE "C" LIKE E'\\'"#, Some("f")),
        (r#"'x' COLLATE "C" LIKE E'y\\'"#, Some("f")),
        (r#"'x' COLLATE "C" LIKE E'x\\'"#, Some("f")),
        (r#"'a_%' COLLATE "C" LIKE E'a\\_\\%'"#, Some("t")),
        (r#"'a_%' COLLATE "C" LIKE 'a🙂_🙂%' ESCAPE '🙂'"#, Some("t")),
        (r#"'x%_' COLLATE "C" LIKE 'x%%_' ESCAPE '%'"#, Some("t")),
        (r#"'x\' COLLATE "C" LIKE 'x\' ESCAPE ''"#, Some("t")),
        (r#"'x\' COLLATE "C" LIKE 'x!\' ESCAPE '!'"#, Some("t")),
        (r#"'x\' COLLATE "C" LIKE 'x\' ESCAPE '!'"#, Some("t")),
        (r#"'x!' COLLATE "C" LIKE 'x!!' ESCAPE '!'"#, Some("t")),
        (r#"NULL COLLATE "C" LIKE E'\\'"#, None),
        (r#"'x' COLLATE "C" NOT LIKE NULL"#, None),
        (r#"'x' COLLATE "C" NOT LIKE 'x' ESCAPE NULL"#, None),
        (r#"'x' NOT LIKE 'x' ESCAPE ('!' COLLATE pg_catalog."C")"#, Some("f")),
        (r#"('x' COLLATE "C" || 'y') LIKE 'x_'"#, Some("t")),
    ] {
        let value = expression(sql)?;
        assert_eq!(value.oid(), 16, "{sql}");
        assert_eq!(value.evaluate(&Row::new())?.as_deref(), expected, "{sql}");
    }
    Ok(())
}

#[test]
fn like_rejects_unsupported_collations_and_reached_invalid_patterns() {
    for sql in [
        "'x' LIKE '%'",
        r#"'x' COLLATE "C" LIKE '%' COLLATE "POSIX""#,
        r#"'x' COLLATE "default" LIKE '%'"#,
        r#"('x' COLLATE "C" || 'y' COLLATE "POSIX") COLLATE "C" LIKE '%'"#,
        r#"'x' COLLATE "C" LIKE strpos('x' COLLATE "C", 'y' COLLATE "POSIX")::text"#,
        r#"'x' COLLATE "C" ILIKE '%'"#,
        r#"'x' COLLATE "C" SIMILAR TO '%'"#,
        r#"'x' COLLATE "C" LIKE 1"#,
        r#"'xx' COLLATE "C" LIKE E'x\\'"#,
        r#"'xx' COLLATE "C" LIKE E'%_\\'"#,
        r#"'xx' COLLATE "C" LIKE 'x🙂' ESCAPE '🙂'"#,
        r#"NULL COLLATE "C" LIKE 'x' ESCAPE '!!'"#,
    ] {
        assert!(expression(sql).is_err(), "{sql}");
    }
}
