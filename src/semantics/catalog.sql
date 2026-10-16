WITH RECURSIVE native AS (
    SELECT t.* FROM pg_catalog.pg_type t
    WHERE t.oid = ANY(ARRAY[16,17,20,21,23,25,26,701,1009,1043,1114,1184,1186,1700,2275,2950,3802]::oid[])
), casts AS (
    SELECT c.* FROM pg_catalog.pg_cast c
    WHERE c.castsource IN (SELECT oid FROM native)
      AND c.casttarget IN (SELECT oid FROM native)
), families AS (
    SELECT p.oid FROM pg_catalog.pg_proc p
    WHERE p.pronamespace = 'pg_catalog'::regnamespace
      AND p.proname = ANY(ARRAY['date_bin','mod','count','sum','avg','min','max',
          'row_number','rank','dense_rank','lag','lead','generate_series'])
      OR (p.pronamespace = 'pg_catalog'::regnamespace AND p.proname = ANY($2::text[]))
), aggregates AS (
    SELECT a.* FROM pg_catalog.pg_aggregate a
    WHERE a.aggfnoid IN (SELECT oid FROM families)
), classes AS (
    SELECT c.* FROM pg_catalog.pg_opclass c
    WHERE c.opcnamespace = 'pg_catalog'::regnamespace
      AND c.opcintype IN (SELECT oid FROM native)
), procedures AS (
    SELECT p.* FROM pg_catalog.pg_amproc p
    WHERE p.amprocfamily IN (SELECT opcfamily FROM classes)
), members AS (
    SELECT o.* FROM pg_catalog.pg_amop o
    WHERE o.amopfamily IN (SELECT opcfamily FROM classes)
), operators AS (
    SELECT o.* FROM pg_catalog.pg_operator o
    WHERE (o.oprnamespace = 'pg_catalog'::regnamespace
      AND o.oprleft IN (SELECT oid FROM native)
      AND o.oprright IN (SELECT oid FROM native))
      OR o.oid IN (SELECT aggsortop FROM aggregates)
      OR o.oid IN (SELECT amopopr FROM members)
), seeds AS (
    SELECT oid FROM families
    UNION SELECT typinput AS oid FROM native UNION SELECT typoutput FROM native
    UNION SELECT typreceive FROM native UNION SELECT typsend FROM native
    UNION SELECT typmodin FROM native UNION SELECT typmodout FROM native
    UNION SELECT typsubscript FROM native UNION SELECT castfunc FROM casts
    UNION SELECT oprcode FROM operators UNION SELECT oprrest FROM operators
    UNION SELECT oprjoin FROM operators UNION SELECT amproc FROM procedures
    UNION SELECT o.oprcode FROM pg_catalog.pg_operator o
          WHERE o.oid IN (SELECT amopopr FROM members)
    UNION SELECT aggfnoid FROM aggregates UNION SELECT aggtransfn FROM aggregates
    UNION SELECT aggfinalfn FROM aggregates UNION SELECT aggcombinefn FROM aggregates
    UNION SELECT aggserialfn FROM aggregates UNION SELECT aggdeserialfn FROM aggregates
    UNION SELECT aggmtransfn FROM aggregates UNION SELECT aggminvtransfn FROM aggregates
    UNION SELECT aggmfinalfn FROM aggregates
    UNION SELECT unnest(ARRAY[
        'pg_catalog.jsonb_array_elements(jsonb)'::regprocedure,
        'pg_catalog.jsonb_array_elements_text(jsonb)'::regprocedure,
        'pg_catalog.jsonb_build_array("any")'::regprocedure,
        'pg_catalog.to_jsonb(anyelement)'::regprocedure,
        'pg_catalog.octet_length(text)'::regprocedure,
        'pg_catalog.sha256(bytea)'::regprocedure,
        'pg_catalog.convert_to(text,name)'::regprocedure,
        'pg_catalog.encode(bytea,text)'::regprocedure,
        'pg_catalog.generate_series(bigint,bigint)'::regprocedure
    ]::oid[])
), function_ids(oid) AS (
    SELECT oid FROM seeds WHERE oid <> 0
    UNION
    SELECT p.prosupport FROM pg_catalog.pg_proc p
    JOIN function_ids f ON f.oid = p.oid WHERE p.prosupport <> 0
), functions AS (
    SELECT p.* FROM pg_catalog.pg_proc p WHERE p.oid IN (SELECT oid FROM function_ids)
), manifest AS (
SELECT pg_catalog.jsonb_build_object(
    'types', (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(t) ORDER BY oid) FROM native t),
    'casts', (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(c) ORDER BY oid) FROM casts c),
    'operators', (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(o) ORDER BY oid) FROM operators o),
    'classes', (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(c) ORDER BY oid) FROM classes c),
    'procedures', (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(p) ORDER BY oid) FROM procedures p),
    'members', (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(o) ORDER BY oid) FROM members o),
    'aggregates', (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(a) ORDER BY aggfnoid) FROM aggregates a),
    'functions', (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(p) ORDER BY oid) FROM functions p),
    'languages', (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(l) ORDER BY oid)
        FROM pg_catalog.pg_language l WHERE l.oid IN (SELECT prolang FROM functions)),
    'collations', (SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
        pg_catalog.to_jsonb(c), pg_catalog.pg_collation_actual_version(c.oid)) ORDER BY c.oid)
        FROM pg_catalog.pg_collation c WHERE c.oid = ANY($1::oid[])),
    'locale', (SELECT pg_catalog.jsonb_build_object(
        'collate', d.datcollate, 'ctype', d.datctype,
        'provider', d.datlocprovider, 'icu', pg_catalog.to_jsonb(d)->'daticulocale',
        'locale', pg_catalog.to_jsonb(d)->'datlocale', 'rules', pg_catalog.to_jsonb(d)->'daticurules',
        'version', d.datcollversion, 'actual', pg_catalog.pg_database_collation_actual_version(d.oid))
        FROM pg_catalog.pg_database d WHERE d.datname = pg_catalog.current_database())
) AS value
)
SELECT pg_catalog.current_setting('server_version_num'),
       pg_catalog.current_setting('server_encoding'),
       pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(value::pg_catalog.text,'UTF8')),'hex')
FROM manifest
