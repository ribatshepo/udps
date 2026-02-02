-- UDPS-031: PostgreSQL Metadata Schema
-- Catalog metadata tables for the Unified Data Processing System

-- =============================================================================
-- databases
-- =============================================================================
CREATE TABLE databases (
    id          UUID PRIMARY KEY,
    name        VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============================================================================
-- schemas
-- =============================================================================
CREATE TABLE schemas (
    id          UUID PRIMARY KEY,
    database_id UUID NOT NULL REFERENCES databases(id) ON DELETE CASCADE,
    name        VARCHAR(255) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (database_id, name)
);

CREATE INDEX idx_schemas_database_id ON schemas(database_id);

-- =============================================================================
-- tables
-- =============================================================================
CREATE TABLE tables (
    id         UUID PRIMARY KEY,
    schema_id  UUID NOT NULL REFERENCES schemas(id) ON DELETE CASCADE,
    name       VARCHAR(255) NOT NULL,
    row_count  BIGINT NOT NULL DEFAULT 0,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    tier       VARCHAR(50) NOT NULL DEFAULT 'hot',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (schema_id, name)
);

CREATE INDEX idx_tables_schema_id ON tables(schema_id);

-- =============================================================================
-- columns
-- =============================================================================
CREATE TABLE columns (
    id               UUID PRIMARY KEY,
    table_id         UUID NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    name             VARCHAR(255) NOT NULL,
    data_type        VARCHAR(255) NOT NULL,
    nullable         BOOLEAN NOT NULL DEFAULT true,
    indexed          BOOLEAN NOT NULL DEFAULT false,
    fts_enabled      BOOLEAN NOT NULL DEFAULT false,
    pii_classified   BOOLEAN NOT NULL DEFAULT false,
    ordinal_position INT NOT NULL,
    UNIQUE (table_id, name)
);

CREATE INDEX idx_columns_table_id ON columns(table_id);

-- =============================================================================
-- partitions
-- =============================================================================
CREATE TABLE partitions (
    id              UUID PRIMARY KEY,
    table_id        UUID NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    partition_key   VARCHAR(255) NOT NULL,
    partition_value VARCHAR(1024) NOT NULL
);

CREATE INDEX idx_partitions_table_id ON partitions(table_id);

-- =============================================================================
-- lineage_edges
-- =============================================================================
CREATE TABLE lineage_edges (
    id               UUID PRIMARY KEY,
    source_table_id  UUID REFERENCES tables(id) ON DELETE SET NULL,
    source_column_id UUID REFERENCES columns(id) ON DELETE SET NULL,
    target_table_id  UUID REFERENCES tables(id) ON DELETE SET NULL,
    target_column_id UUID REFERENCES columns(id) ON DELETE SET NULL,
    query_id         UUID,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_lineage_edges_source_table ON lineage_edges(source_table_id);
CREATE INDEX idx_lineage_edges_target_table ON lineage_edges(target_table_id);
CREATE INDEX idx_lineage_edges_source_column ON lineage_edges(source_column_id);
CREATE INDEX idx_lineage_edges_target_column ON lineage_edges(target_column_id);

-- =============================================================================
-- profiles
-- =============================================================================
CREATE TABLE profiles (
    id         UUID PRIMARY KEY,
    table_id   UUID REFERENCES tables(id) ON DELETE CASCADE,
    column_id  UUID REFERENCES columns(id) ON DELETE CASCADE,
    stats_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_profiles_table_id ON profiles(table_id);
CREATE INDEX idx_profiles_column_id ON profiles(column_id);

-- =============================================================================
-- tags
-- =============================================================================
CREATE TABLE tags (
    id       UUID PRIMARY KEY,
    name     VARCHAR(255) NOT NULL,
    category VARCHAR(255)
);

-- =============================================================================
-- table_tags (junction)
-- =============================================================================
CREATE TABLE table_tags (
    table_id UUID NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    tag_id   UUID NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (table_id, tag_id)
);

CREATE INDEX idx_table_tags_tag_id ON table_tags(tag_id);

-- =============================================================================
-- glossary_terms
-- =============================================================================
CREATE TABLE glossary_terms (
    id              UUID PRIMARY KEY,
    term            VARCHAR(512) UNIQUE NOT NULL,
    definition      TEXT,
    related_columns UUID[]
);

-- =============================================================================
-- snapshots
-- =============================================================================
CREATE TABLE snapshots (
    id              UUID PRIMARY KEY,
    table_id        UUID NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    snapshot_time   TIMESTAMPTZ NOT NULL,
    file_paths_json JSONB NOT NULL
);

CREATE INDEX idx_snapshots_table_id ON snapshots(table_id);

-- =============================================================================
-- query_history
-- =============================================================================
CREATE TABLE query_history (
    id            UUID PRIMARY KEY,
    sql_text      TEXT NOT NULL,
    user_id       VARCHAR(255),
    start_time    TIMESTAMPTZ NOT NULL,
    end_time      TIMESTAMPTZ,
    duration_ms   BIGINT,
    rows_returned BIGINT,
    bytes_scanned BIGINT,
    status        VARCHAR(50) NOT NULL DEFAULT 'success',
    error_message TEXT
);

CREATE INDEX idx_query_history_user_start ON query_history(user_id, start_time);

-- =============================================================================
-- quality_violations
-- =============================================================================
CREATE TABLE quality_violations (
    id              UUID PRIMARY KEY,
    table_id        UUID REFERENCES tables(id) ON DELETE CASCADE,
    column_id       UUID REFERENCES columns(id) ON DELETE CASCADE,
    rule_name       VARCHAR(255) NOT NULL,
    violation_count BIGINT,
    violation_rate  DOUBLE PRECISION,
    details         JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_quality_violations_table_id ON quality_violations(table_id);
CREATE INDEX idx_quality_violations_column_id ON quality_violations(column_id);

-- =============================================================================
-- scan_checkpoints
-- =============================================================================
CREATE TABLE scan_checkpoints (
    id                UUID PRIMARY KEY,
    source_type       VARCHAR(255) NOT NULL,
    source_identifier VARCHAR(1024) NOT NULL,
    last_scan_time    TIMESTAMPTZ NOT NULL,
    checkpoint_data   JSONB,
    UNIQUE (source_type, source_identifier)
);
