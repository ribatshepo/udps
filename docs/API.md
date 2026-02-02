# UDPS API Reference

UDPS exposes four network interfaces: gRPC (port 50060), HTTPS/REST (port 8443), Prometheus metrics (port 9090), and health checks (port 8081).

## Authentication

All gRPC and REST API calls require a valid JWT token obtained from the Unified Security Platform (USP). The token must be passed as:

- **gRPC:** `authorization` metadata key with value `Bearer <token>`
- **REST:** `Authorization` HTTP header with value `Bearer <token>`

Token validation is performed against USP's `AuthenticationService.ValidateToken` RPC on every request. Authorization checks are performed against USP's `AuthorizationService.CheckPermission` RPC before data access operations.

## gRPC Services (Port 50060)

The gRPC server listens on port 50060 with TLS enabled by default. Maximum message size is 100 MB.

### StorageService

Handles data ingestion, querying, compaction, tier management, indexing, full-text search, materialized views, and replication.

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `Ingest` | `IngestRequest` | `IngestResponse` | Ingest record batches (Arrow IPC format) into a table |
| `Query` | `QueryRequest` | `stream QueryResponse` | Execute a storage-level scan query; results streamed as record batches |
| `GetStats` | `GetStatsRequest` | `StatsResponse` | Get storage statistics (row count, size, partitions, compression ratio) |
| `Compact` | `CompactRequest` | `CompactResponse` | Trigger compaction on a table or partition |
| `SetTierPolicy` | `SetTierPolicyRequest` | `SetTierPolicyResponse` | Configure automatic tier transition rules |
| `MoveToTier` | `MoveToTierRequest` | `MoveToTierResponse` | Manually move data to a specific storage tier |
| `GetTierInfo` | `GetTierInfoRequest` | `TierInfoResponse` | Get current tier distribution for a table |
| `CreateIndex` | `CreateIndexRequest` | `CreateIndexResponse` | Create an index (ZoneMap, BloomFilter, Bitmap, SkipList, Inverted) |
| `DropIndex` | `DropIndexRequest` | `DropIndexResponse` | Drop an existing index |
| `ListIndexes` | `ListIndexesRequest` | `ListIndexesResponse` | List indexes for a table |
| `SearchFullText` | `SearchFullTextRequest` | `SearchFullTextResponse` | Execute a full-text search query |
| `CreateMaterializedView` | `CreateMaterializedViewRequest` | `CreateMaterializedViewResponse` | Create a materialized view |
| `RefreshMaterializedView` | `RefreshMaterializedViewRequest` | `RefreshMaterializedViewResponse` | Trigger a view refresh |
| `ListMaterializedViews` | `ListMaterializedViewsRequest` | `ListMaterializedViewsResponse` | List materialized views |

### QueryService

Handles SQL query parsing, optimization, execution, data source federation, and query history.

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `ExecuteQuery` | `ExecuteQueryRequest` | `stream ExecuteQueryResponse` | Parse, optimize, and execute a SQL query; results streamed |
| `ExplainQuery` | `ExplainQueryRequest` | `ExplainQueryResponse` | Return the query execution plan without executing |
| `CancelQuery` | `CancelQueryRequest` | `CancelQueryResponse` | Cancel a running query by ID |
| `RegisterDataSource` | `RegisterDataSourceRequest` | `RegisterDataSourceResponse` | Register an external data source for federation |
| `ListDataSources` | `ListDataSourcesRequest` | `ListDataSourcesResponse` | List registered federated data sources |
| `TestDataSource` | `TestDataSourceRequest` | `TestDataSourceResponse` | Test connectivity to a data source |
| `RemoveDataSource` | `RemoveDataSourceRequest` | `RemoveDataSourceResponse` | Unregister a federated data source |
| `GetQueryHistory` | `GetQueryHistoryRequest` | `GetQueryHistoryResponse` | Retrieve past query executions with timing and status |

Supported federated data source types (via adapters in `io.gbmm.udps.query.federation.adapters`):

| Adapter class | Data source |
|---------------|-------------|
| `PostgreSQLAdapter` | PostgreSQL |
| `MySQLAdapter` | MySQL |
| `JdbcFederationAdapter` | Any JDBC-compatible database |
| `MongoDBAdapter` | MongoDB |
| `ElasticsearchAdapter` | Elasticsearch / OpenSearch |
| `KafkaAdapter` | Apache Kafka topics |
| `S3Adapter` | S3/MinIO (Parquet, CSV, JSON files) |

### CatalogService

Handles metadata management, schema discovery, lineage, profiling, sampling, classification, and anonymization.

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `ScanDatabase` | `ScanDatabaseRequest` | `ScanDatabaseResponse` | Trigger schema discovery scan on a database |
| `GetDatabase` | `GetDatabaseRequest` | `DatabaseResponse` | Get database metadata |
| `ListDatabases` | `ListDatabasesRequest` | `ListDatabasesResponse` | List all registered databases |
| `GetSchema` | `GetSchemaRequest` | `SchemaResponse` | Get schema metadata |
| `ListSchemas` | `ListSchemasRequest` | `ListSchemasResponse` | List schemas in a database |
| `GetTable` | `GetTableRequest` | `TableResponse` | Get table metadata |
| `ListTables` | `ListTablesRequest` | `ListTablesResponse` | List tables in a schema |
| `UpdateTableMetadata` | `UpdateTableMetadataRequest` | `UpdateTableMetadataResponse` | Update table description, tags, owner |
| `GetColumn` | `GetColumnRequest` | `ColumnResponse` | Get column metadata |
| `ListColumns` | `ListColumnsRequest` | `ListColumnsResponse` | List columns in a table |
| `UpdateColumnMetadata` | `UpdateColumnMetadataRequest` | `UpdateColumnMetadataResponse` | Update column description, tags |
| `SearchMetadata` | `SearchMetadataRequest` | `SearchMetadataResponse` | Full-text search across all metadata |
| `GetLineage` | `GetLineageRequest` | `LineageResponse` | Get upstream/downstream lineage for a table |
| `GetColumnLineage` | `GetColumnLineageRequest` | `ColumnLineageResponse` | Get column-level lineage |
| `RecordLineage` | `RecordLineageRequest` | `RecordLineageResponse` | Record a new lineage relationship |
| `GetImpactAnalysis` | `GetImpactAnalysisRequest` | `ImpactAnalysisResponse` | Analyze downstream impact of a schema change |
| `ProfileTable` | `ProfileTableRequest` | `ProfileResponse` | Run statistical profiling on a table |
| `ProfileColumn` | `ProfileColumnRequest` | `ColumnProfileResponse` | Run statistical profiling on a specific column |
| `GetProfileResults` | `GetProfileResultsRequest` | `ProfileResultsResponse` | Retrieve previously computed profile results |
| `CreateSample` | `CreateSampleRequest` | `SampleResponse` | Create a data sample using a specified strategy |
| `GetSample` | `GetSampleRequest` | `SampleResponse` | Retrieve a previously created sample |
| `DeleteSample` | `DeleteSampleRequest` | `DeleteSampleResponse` | Delete a sample |
| `ClassifyColumns` | `ClassifyColumnsRequest` | `ClassifyColumnsResponse` | Run PII classification on table columns |
| `AnonymizeData` | `AnonymizeDataRequest` | `AnonymizeDataResponse` | Apply anonymization techniques to data |

Schema discovery sources (defined in `io.gbmm.udps.catalog.discovery.sources`):

| Scanner class | Source type |
|---------------|-----------|
| `JDBCScanner` | Any JDBC-compatible database |
| `KafkaScanner` | Kafka topic schemas (Schema Registry) |
| `S3Scanner` | S3/MinIO bucket schemas (from Parquet/CSV/JSON files) |

### HealthService

Standard gRPC health checking.

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `Check` | `HealthCheckRequest` | `HealthCheckResponse` | Synchronous health check |
| `Watch` | `HealthCheckRequest` | `stream HealthCheckResponse` | Stream health status changes |

## REST API (Port 8443)

The REST API is served via http4s Ember on port 8443 with HTTPS. All responses use JSON (Circe serialization).

### Catalog Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/databases` | List all databases |
| `GET` | `/api/v1/databases/{id}` | Get database by ID |
| `POST` | `/api/v1/databases/{id}/scan` | Trigger schema discovery scan |
| `GET` | `/api/v1/databases/{dbId}/schemas` | List schemas in a database |
| `GET` | `/api/v1/schemas/{id}` | Get schema by ID |
| `GET` | `/api/v1/schemas/{schemaId}/tables` | List tables in a schema |
| `GET` | `/api/v1/tables/{id}` | Get table by ID |
| `PUT` | `/api/v1/tables/{id}` | Update table metadata |
| `GET` | `/api/v1/tables/{tableId}/columns` | List columns in a table |
| `GET` | `/api/v1/columns/{id}` | Get column by ID |
| `PUT` | `/api/v1/columns/{id}` | Update column metadata |
| `GET` | `/api/v1/metadata/search?q={query}` | Search metadata |

### Storage Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/storage/ingest` | Ingest data (JSON body) |
| `POST` | `/api/v1/storage/query` | Execute a storage scan query |
| `GET` | `/api/v1/storage/{table}/stats` | Get storage statistics |
| `POST` | `/api/v1/storage/{table}/compact` | Trigger compaction |
| `GET` | `/api/v1/storage/{table}/tiers` | Get tier distribution |
| `PUT` | `/api/v1/storage/{table}/tiers` | Set tier policy |
| `POST` | `/api/v1/storage/{table}/tiers/move` | Move data to a specific tier |
| `GET` | `/api/v1/storage/costs` | Get storage cost analysis |

### Query Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/query/execute` | Execute a SQL query |
| `POST` | `/api/v1/query/explain` | Get query execution plan |
| `DELETE` | `/api/v1/query/{queryId}` | Cancel a running query |
| `GET` | `/api/v1/query/history` | Get query history |
| `GET` | `/api/v1/query/history/{queryId}` | Get details of a specific query execution |

### Data Source Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/datasources` | Register a federated data source |
| `GET` | `/api/v1/datasources` | List registered data sources |
| `GET` | `/api/v1/datasources/{id}` | Get data source details |
| `POST` | `/api/v1/datasources/{id}/test` | Test data source connectivity |
| `DELETE` | `/api/v1/datasources/{id}` | Remove a data source |

### Lineage Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/lineage/{tableId}/upstream` | Get upstream lineage |
| `GET` | `/api/v1/lineage/{tableId}/downstream` | Get downstream lineage |
| `GET` | `/api/v1/lineage/{tableId}/columns/{columnId}` | Get column-level lineage |
| `POST` | `/api/v1/lineage` | Record a lineage relationship |
| `GET` | `/api/v1/lineage/{tableId}/impact` | Run impact analysis |

### Profiling Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/profiling/tables/{tableId}` | Profile a table |
| `POST` | `/api/v1/profiling/columns/{columnId}` | Profile a column |
| `GET` | `/api/v1/profiling/results/{tableId}` | Get profile results |

### Sampling Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/sampling` | Create a data sample |
| `GET` | `/api/v1/sampling/{sampleId}` | Get a sample |
| `DELETE` | `/api/v1/sampling/{sampleId}` | Delete a sample |

### Governance Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/governance/classify/{tableId}` | Run PII classification |
| `POST` | `/api/v1/governance/anonymize` | Anonymize data |
| `POST` | `/api/v1/governance/access-requests` | Submit a data access request |
| `GET` | `/api/v1/governance/access-requests` | List access requests |
| `PUT` | `/api/v1/governance/access-requests/{id}` | Approve/deny an access request |
| `GET` | `/api/v1/governance/compliance/report` | Generate GDPR compliance report |
| `GET` | `/api/v1/governance/audit` | Search audit trail |

### Tags and Glossary Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/tags` | Create a tag |
| `GET` | `/api/v1/tags` | List tags |
| `PUT` | `/api/v1/tags/{id}` | Update a tag |
| `DELETE` | `/api/v1/tags/{id}` | Delete a tag |
| `POST` | `/api/v1/tags/{tagId}/assign` | Assign a tag to a resource |
| `POST` | `/api/v1/glossary` | Create a glossary term |
| `GET` | `/api/v1/glossary` | List glossary terms |
| `GET` | `/api/v1/glossary/{id}` | Get a glossary term |
| `PUT` | `/api/v1/glossary/{id}` | Update a glossary term |
| `DELETE` | `/api/v1/glossary/{id}` | Delete a glossary term |

### Utility Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/erd/generate` | Generate an ERD diagram |
| `POST` | `/api/v1/querybuilder/build` | Build a SQL query from a visual specification |
| `POST` | `/api/v1/querybuilder/validate` | Validate a constructed query |
| `GET` | `/api/v1/performance/slow-queries` | Get slow query report |
| `GET` | `/api/v1/performance/recommendations` | Get performance recommendations |
| `GET` | `/api/v1/costs/summary` | Get cost summary by tier |
| `GET` | `/api/v1/costs/by-table` | Get per-table cost breakdown |

## Health Check Endpoints (Port 8081)

Health check endpoints are served on a separate port (8081) without TLS or authentication, so that load balancers and Kubernetes probes can reach them.

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| `GET` | `/health` | Overall service health | `200 OK` with `{"status":"healthy"}` or `503` |
| `GET` | `/health/live` | Liveness probe (is the process running?) | `200 OK` with `{"status":"alive"}` |
| `GET` | `/health/ready` | Readiness probe (can the service accept traffic?) | `200 OK` with `{"status":"ready"}` or `503` |

The readiness probe checks connectivity to PostgreSQL, Redis, and Kafka. If any dependency is unreachable, the probe returns 503 and the service is removed from the load balancer rotation.

## Metrics Endpoint (Port 9090)

Prometheus metrics are exposed on a separate port (9090) without TLS or authentication.

| Method | Path | Description | Format |
|--------|------|-------------|--------|
| `GET` | `/metrics` | Prometheus metrics | `text/plain; version=0.0.4` |

### Key Metrics

**Storage metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `udps_ingest_total` | Counter | Total number of ingest operations |
| `udps_ingest_bytes_total` | Counter | Total bytes ingested |
| `udps_ingest_duration_seconds` | Histogram | Ingest operation latency |
| `udps_compression_ratio` | Gauge | Current compression ratio by codec |
| `udps_tier_bytes` | Gauge | Bytes stored per tier (hot, warm, cold, archive) |
| `udps_index_size_bytes` | Gauge | Index size by type |

**Query metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `udps_query_total` | Counter | Total queries executed |
| `udps_query_duration_seconds` | Histogram | Query latency |
| `udps_query_active` | Gauge | Currently executing queries |
| `udps_query_cache_hits_total` | Counter | Query cache hits |
| `udps_query_cache_misses_total` | Counter | Query cache misses |
| `udps_query_partitions_scanned_total` | Counter | Partitions scanned across queries |

**Catalog metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `udps_catalog_scan_total` | Counter | Schema discovery scans completed |
| `udps_catalog_tables_total` | Gauge | Total tables in catalog |
| `udps_catalog_lineage_nodes_total` | Gauge | Total nodes in lineage graph |
| `udps_catalog_profiling_jobs_total` | Counter | Profiling jobs completed |

**System metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `udps_jvm_memory_used_bytes` | Gauge | JVM heap memory used |
| `udps_jvm_memory_max_bytes` | Gauge | JVM heap memory maximum |
| `udps_jvm_threads_current` | Gauge | Current JVM thread count |
| `udps_jvm_gc_pause_seconds` | Summary | GC pause duration |
| `udps_http_requests_total` | Counter | Total HTTP requests by method and path |
| `udps_http_request_duration_seconds` | Histogram | HTTP request latency |
| `udps_grpc_requests_total` | Counter | Total gRPC requests by service and method |
| `udps_grpc_request_duration_seconds` | Histogram | gRPC request latency |

## USP Integration gRPC Services

These are client-side services that UDPS calls on the USP platform. The proto definitions are in `udps-integration/src/main/protobuf/usp/`.

### OutpostCommunicationService (`usp/outpost.proto`)

UDPS registers as `OUTPOST_TYPE_COMPUTE` (value 6).

| RPC | Description |
|-----|-------------|
| `Authenticate` | Authenticate the UDPS outpost with USP using a token |
| `Heartbeat` | Send periodic heartbeat with metrics (CPU, memory, connections) |
| `GetConfiguration` | Retrieve current outpost configuration |
| `ReportHealth` | Report health status and metrics to USP |
| `ReportAuthEvent` | Report authentication events (login, logout, access denied) |
| `StreamConfigurationUpdates` | Server-streaming RPC to receive real-time configuration changes |

### AuthenticationService (`usp/authentication.proto`)

| RPC | Description |
|-----|-------------|
| `ValidateToken` | Validate a JWT token and get user ID, roles, permissions, and expiry |
| `CreateSession` | Create an authentication session for a user |
| `ValidateSession` | Check if a session is still valid |
| `RevokeSession` | Terminate a session |
| `GetUserContext` | Get full user context (roles, permissions, attributes) for authorization |
| `RefreshToken` | Refresh an expired access token using a refresh token |

### AuthorizationService (`usp/authorization.proto`)

| RPC | Description |
|-----|-------------|
| `CheckPermission` | Check if a user has permission for a specific action on a resource |
| `CheckPermissions` | Batch permission check (multiple resource/action pairs in one call) |
| `EvaluatePolicy` | Evaluate an ABAC policy against user, resource, and environment attributes |
| `GetUserPermissions` | List all permissions a user has in a workspace |
| `GetUserRoles` | List all roles assigned to a user in a workspace |

### SecretsService (`usp/secrets.proto`)

| RPC | Description |
|-----|-------------|
| `GetSecret` | Read a secret from the KV engine (with optional version) |
| `PutSecret` | Write a secret to the KV engine |
| `DeleteSecret` | Delete a secret (specific versions or all) |
| `ListSecrets` | List secret keys under a path |
| `Encrypt` | Encrypt data using the transit engine |
| `Decrypt` | Decrypt data using the transit engine |
| `GenerateDataKey` | Generate a data encryption key (for envelope encryption) |

## UCCP Integration gRPC Services

These are client-side services that UDPS calls on the UCCP platform. The proto definitions are in `udps-integration/src/main/protobuf/uccp/`.

### ServiceDiscovery (`uccp/coordination.proto`)

| RPC | Description |
|-----|-------------|
| `Register` | Register UDPS as a discoverable service with capabilities and health config |
| `Deregister` | Remove UDPS from the service registry |
| `Discover` | Find other services by name, type, version, labels, or capabilities |
| `Heartbeat` | Send periodic heartbeat with health status and metadata updates |
| `ListServices` | List registered services with pagination |
| `GetServiceMetadata` | Get detailed metadata for a specific service instance |
| `WatchServices` | Server-streaming RPC to watch for service registration/deregistration events |
