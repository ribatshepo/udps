-- UDPS Catalog Schema Initialization
-- Executed automatically on first PostgreSQL container start

CREATE SCHEMA IF NOT EXISTS catalog;

COMMENT ON SCHEMA catalog IS 'UDPS metadata catalog - stores dataset registry, lineage, and governance metadata';

-- Grant usage to the udps role
ALTER SCHEMA catalog OWNER TO udps;

-- Create extensions used by the catalog
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Set the default search path to include the catalog schema
ALTER DATABASE udps_catalog SET search_path TO catalog, public;
