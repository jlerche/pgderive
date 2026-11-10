-- Physical locations are mutable; a logical object's identity and bytes are immutable.
CREATE TABLE IF NOT EXISTS {schema}.version (
    singleton boolean PRIMARY KEY CHECK(singleton),
    version integer NOT NULL CHECK(version=1),
    identity uuid NOT NULL DEFAULT pg_catalog.gen_random_uuid()
);
INSERT INTO {schema}.version(singleton,version) VALUES(true,1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS {schema}.tenants (
    tenant text PRIMARY KEY CHECK(length(tenant) BETWEEN 1 AND 128 AND tenant ~ '^[A-Za-z0-9_.-]+$'),
    identity uuid NOT NULL UNIQUE DEFAULT pg_catalog.gen_random_uuid(),
    storage jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS {schema}.catalogs (
    tenant text NOT NULL REFERENCES {schema}.tenants(tenant),
    catalog uuid NOT NULL,
    source jsonb NOT NULL,
    prefix text NOT NULL,
    PRIMARY KEY(tenant,catalog),
    UNIQUE(tenant,source,prefix)
);
-- A fenced owner can never install another mapping. Its keys remain recognizable
-- even if a canceled provider PUT completes after physical deletion.
CREATE TABLE IF NOT EXISTS {schema}.owners (
    id uuid PRIMARY KEY DEFAULT pg_catalog.gen_random_uuid(),
    tenant text NOT NULL REFERENCES {schema}.tenants(tenant),
    active boolean NOT NULL DEFAULT true,
    UNIQUE(tenant,id)
);
CREATE TABLE IF NOT EXISTS {schema}.packs (
    id uuid PRIMARY KEY DEFAULT pg_catalog.gen_random_uuid(),
    tenant text NOT NULL REFERENCES {schema}.tenants(tenant),
    owner uuid NOT NULL REFERENCES {schema}.owners(id),
    path text NOT NULL UNIQUE,
    length bigint NOT NULL CHECK(length>0),
    checksum text NOT NULL CHECK(length(checksum)=64),
    state text NOT NULL CHECK(state IN ('writing','ready','retired','deleted')),
    UNIQUE(tenant,id),
    FOREIGN KEY(tenant,owner) REFERENCES {schema}.owners(tenant,id)
);
CREATE TABLE IF NOT EXISTS {schema}.namespaces (
    tenant text NOT NULL,
    catalog uuid NOT NULL,
    namespace text NOT NULL,
    sealed boolean NOT NULL DEFAULT false,
    PRIMARY KEY(tenant,catalog,namespace),
    FOREIGN KEY(tenant,catalog) REFERENCES {schema}.catalogs(tenant,catalog)
);
CREATE TABLE IF NOT EXISTS {schema}.chunks (
    tenant text NOT NULL,
    catalog uuid NOT NULL,
    name text COLLATE "C" NOT NULL,
    namespace text NOT NULL,
    pack uuid NOT NULL,
    offset_bytes bigint NOT NULL CHECK(offset_bytes>=0),
    length bigint NOT NULL CHECK(length>0),
    checksum text NOT NULL CHECK(length(checksum)=64),
    created_micros bigint NOT NULL DEFAULT (extract(epoch FROM clock_timestamp())*1000000)::bigint,
    PRIMARY KEY(tenant,catalog,name),
    FOREIGN KEY(tenant,catalog,namespace) REFERENCES {schema}.namespaces(tenant,catalog,namespace),
    FOREIGN KEY(tenant,pack) REFERENCES {schema}.packs(tenant,id)
);
CREATE INDEX IF NOT EXISTS CHUNKS_PACK ON {schema}.chunks(tenant,pack);
CREATE INDEX IF NOT EXISTS PACKS_STATE ON {schema}.packs(tenant,state);
