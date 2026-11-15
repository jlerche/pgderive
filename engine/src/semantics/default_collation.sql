SELECT
    CASE datlocprovider
        WHEN 'c' THEN datcollate IN ('C', 'POSIX')
        WHEN 'b' THEN coalesce(to_jsonb(d)->>'datlocale' IN ('C', 'C.UTF-8', 'PG_UNICODE_FAST'), false)
        ELSE false
    END
FROM pg_catalog.pg_database d
WHERE datname = pg_catalog.current_database()
