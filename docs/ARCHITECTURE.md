# UDPS Architecture

## System Overview

The Unified Data Platform Service (UDPS) is a monolithic but modular Scala 2.13 application that provides columnar storage, SQL query execution, metadata cataloging, and data governance through a unified API surface. It is designed for high throughput (500 MB/sec ingestion, 10,000+ queries/sec indexed) and petabyte-scale storage.

### Design Principles

1. **Functional-first** -- All effects are expressed through Cats Effect `IO`, resource lifecycle through `Resource[IO, _]`, and streaming through FS2. Side effects are pushed to the edges.
2. **Modular monolith** -- Seven SBT modules with explicit dependency boundaries. Each module owns its domain and exposes typed interfaces to dependents.
3. **Configuration over convention** -- Every tunable parameter is externalized via Typesafe Config (`application.conf`) with environment variable overrides.
4. **Zero-trust security** -- TLS on all network boundaries, token validation delegated to USP, column-level access control in the governance module.
5. **Tiered cost optimization** -- Data automatically migrates across four storage tiers based on access patterns, age, and cost targets.

## Module Dependency Graph

```
                              +------------------+
                              |      api         |
                              | (udps-api)       |
                              | gRPC + HTTP/REST |
                              +--------+---------+
                             /    |    |     \
                            v     v    v      v
               +---------+ +--------+ +-----------+ +--------------+
               |  query  | | catalog| | governance| | integration  |
               |(udps-   | |(udps-  | | (udps-    | | (udps-       |
               | query)  | | catalog| | governance| | integration) |
               +----+----+ +---+----+ +-----+-----+ +------+-------+
                    |           |             |              |
                    |           |             v              |
                    |           |        (catalog)           |
                    v           v                            v
               +---------+ +---------+               +-----------+
               | storage | | storage |               |   core    |
               |(udps-   | |(udps-   |               | (udps-    |
               | storage)| | storage)|               |  core)    |
               +----+----+ +----+----+               +-----------+
                    |            |                         ^
                    v            v                         |
               +--------------------------------------+   |
               |              core                    +---+
               |           (udps-core)                |
               +--------------------------------------+
```

**Dependency rules:**

| Module | Depends on |
|--------|-----------|
| `udps-core` | (none -- leaf module) |
| `udps-storage` | `core` |
| `udps-query` | `core`, `storage` |
| `udps-catalog` | `core`, `storage` |
| `udps-governance` | `core`, `catalog` |
| `udps-integration` | `core` |
| `udps-api` | `core`, `query`, `catalog`, `governance`, `integration` |

These dependencies are enforced in `build.sbt` via `.dependsOn()` declarations.

## Module Descriptions

### udps-core

**Directory:** `udps-core/src/main/scala/io/gbmm/udps/core/`

The foundation module containing domain models and configuration. It has no external dependencies beyond the Cats ecosystem, logging, and PureConfig.

| Package | Key classes | Responsibility |
|---------|-----------|---------------|
| `config` | `UdpsConfig` | Typed configuration loaded from `application.conf` via PureConfig |
| `domain` | `DataType`, `SchemaInfo`, `ColumnMetadata`, `TableMetadata` | Schema representation |
| `domain` | `StorageTier`, `CompressionCodec`, `IndexType` | Storage abstractions |
| `domain` | `PartitionInfo`, `QueryPlan` | Query and partition models |

### udps-storage

**Directory:** `udps-storage/src/main/scala/io/gbmm/udps/storage/`

The storage engine handles all data persistence, from in-memory Arrow record batches through Parquet file I/O to tiered archival on S3/MinIO.

| Package | Key classes | Responsibility |
|---------|-----------|---------------|
| `arrow` | `SchemaAdapter` | Converts between UDPS domain schemas and Arrow schemas |
| `batch` | `RecordBatchProcessor`, `Transformations` | Arrow record batch ingestion and transformation |
| `parquet` | `ParquetWriter`, `ParquetReader`, `CompressionConfig`, `PredicatePushdown` | Parquet file I/O with codec selection and predicate pushdown |
| `indexing` | `ZoneMapIndexer`, `BloomFilterIndexer`, `BloomFilterReader`, `BitmapIndexer`, `SkipListIndexer` | Six index types for query acceleration |
| `fts` | `LuceneIndexer`, `LuceneSearcher`, `Analyzers` | Full-text search powered by Apache Lucene 9.9 |
| `tiering` | `TierManager`, `TierPolicy`, `MinIOClient` | Automatic data movement across Hot/Warm/Cold/Archive tiers |
| `mvcc` | `SnapshotManager`, `SnapshotGC` | Multi-version concurrency control for time-travel queries |
| `txn` | `TransactionCoordinator`, `TransactionLog`, `LockManager` | ACID transactions with snapshot isolation |
| `views` | `MaterializedViewEngine`, `ViewRefresher`, `QueryRewriter` | Materialized view lifecycle and transparent query rewriting |

### udps-query

**Directory:** `udps-query/src/main/scala/io/gbmm/udps/query/`

The query engine provides SQL parsing, optimization, and distributed execution. It integrates Apache Calcite for SQL parsing and cost-based optimization.

| Package | Key classes | Responsibility |
|---------|-----------|---------------|
| `parser` | `SQLParser` | SQL:2016 parsing via Apache Calcite |
| `calcite` | `UDPSSchema`, `UDPSTable`, `TypeMapper` | Calcite schema/table adapters for UDPS storage |
| `optimizer` | `LogicalOptimizer`, `CostBasedOptimizer`, `CostModel`, `OptimizationRules`, `StatisticsProvider` | Multi-phase query optimization |
| `physical` | `PhysicalPlanner`, `ExecutionDAG`, `Operators` | Converts logical plans to physical execution DAGs |
| `execution` | `DistributedExecutor`, `QueryCoordinator`, `StageExecutor`, `ShuffleExchange` | Distributed, stage-based query execution |
| `vectorized` | `VectorizedFilter`, `VectorizedAggregate`, `VectorizedProject` | SIMD-style batch processing on Arrow vectors |
| `adaptive` | `AdaptiveExecutor`, `ReOptimizer`, `RuntimeStatistics` | Re-optimizes running queries based on observed statistics |
| `cache` | `QueryCache`, `CacheInvalidator` | Redis-backed query result caching |
| `federation` | `FederationAdapter`, `JdbcFederationAdapter`, `PostgreSQLAdapter`, `MySQLAdapter`, `MongoDBAdapter`, `ElasticsearchAdapter`, `KafkaAdapter`, `S3Adapter` | Data source federation with intelligent pushdown |

### udps-catalog

**Directory:** `udps-catalog/src/main/scala/io/gbmm/udps/catalog/`

The metadata catalog manages schema discovery, data lineage, profiling, quality rules, and query history. It persists metadata to PostgreSQL via Doobie.

| Package | Key classes | Responsibility |
|---------|-----------|---------------|
| `repository` | `MetadataRepository`, `Tables` | PostgreSQL-backed metadata CRUD via Doobie |
| `discovery` | `SchemaDiscovery`, `IncrementalScanner`, `ChangeDetector` | Automatic schema scanning with change detection |
| `discovery.sources` | `JDBCScanner`, `KafkaScanner`, `S3Scanner` | Source-specific schema extraction |
| `lineage` | `LineageTracker`, `LineageGraph`, `LineageExtractor`, `GraphExporter` | Table/column-level lineage with DAG visualization |
| `profiling` | `DataProfiler`, `NumericProfiler`, `StringProfiler`, `DateProfiler` | Statistical profiling by data type |
| `quality` | `QualityRulesEngine`, `RuleEvaluator`, `RuleDefinitions` | Automated data quality rule evaluation |
| `sampling` | `SamplingEngine`, `Strategies` | Random, stratified, systematic, cluster, and time-based sampling |
| `querybuilder` | `QueryBuilder`, `SQLGenerator`, `CostEstimator` | Visual query construction and SQL generation |
| `history` | `QueryHistoryTracker`, `QueryAnalytics` | Query execution history and analytics |

### udps-governance

**Directory:** `udps-governance/src/main/scala/io/gbmm/udps/governance/`

The governance module provides PII classification, data anonymization, GDPR compliance tools, and fine-grained access control.

| Package | Key classes | Responsibility |
|---------|-----------|---------------|
| `classification` | `PIIClassifier`, `MLClassifier`, `Patterns`, `CustomRules` | PII detection with 16+ built-in patterns and ML support |
| `anonymization` | `Anonymizer`, `Techniques` | 10+ anonymization techniques (masking, hashing, tokenization, etc.) |
| `gdpr` | `GDPRCompliance`, `ConsentManager`, `RetentionPolicy` | GDPR compliance tooling and data retention |
| `access` | `AccessRequestWorkflow`, `WorkflowEngine` | Data access request and approval workflows |
| `audit` | `AuditLogger`, `AuditSearch` | Immutable audit trail with search |
| `security` | `ColumnSecurity`, `PolicyEnforcer` | Column-level security with dynamic data masking |

### udps-integration

**Directory:** `udps-integration/src/main/protobuf/`

Contains Protobuf service definitions for communication with external SeRi-SA platform services. ScalaPB generates Scala gRPC client/server stubs at compile time.

| Proto package | Service | Purpose |
|---------------|---------|---------|
| `usp.outpost` | `OutpostCommunicationService` | Outpost registration, heartbeat, configuration streaming |
| `usp.authentication` | `AuthenticationService` | Token validation, session management, user context |
| `usp.authorization` | `AuthorizationService` | Permission checks, policy evaluation, role management |
| `usp.secrets` | `SecretsService` | Secret storage/retrieval, transit encryption, data key generation |
| `uccp.coordination` | `ServiceDiscovery` | Service registration, discovery, health, and watch |

### udps-api

**Directory:** `udps-api/src/main/scala/io/gbmm/udps/api/`

The application entrypoint. Wires together all modules and exposes gRPC (port 50060), HTTPS/REST (port 8443), Prometheus metrics (port 9090), and health checks (port 8081).

## Data Flow Diagrams

### Ingestion Flow

```
Client
  |
  | gRPC IngestRequest (Arrow IPC or row data)
  v
+----------+     +-----------------------+     +---------------------+
|  api     | --> | storage.batch         | --> | storage.parquet     |
| (gRPC    |     | RecordBatchProcessor  |     | ParquetWriter       |
|  server) |     | + Transformations     |     | + CompressionConfig |
+----------+     +-----------------------+     +---------------------+
                         |                              |
                         v                              v
                 +------------------+         +-------------------+
                 | storage.indexing |         | storage.tiering   |
                 | ZoneMap, Bloom,  |         | TierManager       |
                 | Bitmap, SkipList |         | (Hot -> Warm ->   |
                 +------------------+         |  Cold -> Archive) |
                         |                    +-------------------+
                         v
                 +------------------+
                 | storage.fts      |
                 | LuceneIndexer    |
                 | (if FTS enabled) |
                 +------------------+
```

### Query Flow

```
Client
  |
  | SQL query string (gRPC or REST)
  v
+----------+     +------------------+     +------------------------+
|  api     | --> | query.parser     | --> | query.optimizer        |
| (gRPC/   |     | SQLParser        |     | LogicalOptimizer       |
|  HTTP)   |     | (Calcite)        |     | CostBasedOptimizer     |
+----------+     +------------------+     | StatisticsProvider     |
                                          +------------------------+
                                                    |
                                                    v
                                          +------------------------+
                                          | query.physical         |
                                          | PhysicalPlanner        |
                                          | -> ExecutionDAG        |
                                          +------------------------+
                                                    |
                                    +---------------+---------------+
                                    |                               |
                                    v                               v
                          +-------------------+          +--------------------+
                          | query.execution   |          | query.federation   |
                          | DistributedExecutor|         | (if external       |
                          | StageExecutor     |          |  data source)      |
                          | ShuffleExchange   |          | PostgreSQLAdapter  |
                          +-------------------+          | MongoDBAdapter ... |
                                    |                    +--------------------+
                                    v
                          +-------------------+
                          | query.vectorized  |
                          | VectorizedFilter  |
                          | VectorizedAgg     |
                          +-------------------+
                                    |
                                    v
                          +-------------------+
                          | query.cache       |
                          | QueryCache (Redis)|
                          +-------------------+
                                    |
                                    v
                             Result to client
```

### Catalog / Schema Discovery Flow

```
Trigger (manual or scheduled)
  |
  v
+-------------------------+     +---------------------+
| catalog.discovery       | --> | catalog.discovery   |
| SchemaDiscovery         |     | .sources            |
| IncrementalScanner      |     | JDBCScanner         |
| ChangeDetector          |     | KafkaScanner        |
+-------------------------+     | S3Scanner           |
          |                     +---------------------+
          v
+-------------------------+
| catalog.repository      |
| MetadataRepository      |  <-- PostgreSQL (Doobie)
| Tables                  |
+-------------------------+
          |
          +----> catalog.lineage.LineageTracker
          +----> catalog.profiling.DataProfiler
          +----> catalog.quality.QualityRulesEngine
          +----> governance.classification.PIIClassifier
```

## Key Abstractions and Patterns

### Cats Effect IO

All effectful operations return `IO[A]`. Long-lived resources (database pools, gRPC channels, Lucene index writers) are managed through `Resource[IO, A]` to guarantee cleanup on shutdown.

### FS2 Streams

Data pipelines (ingestion, batch processing, CDC) are expressed as `fs2.Stream[IO, A]`. This provides backpressure, chunking, and composable stream transformations.

### Resource Lifecycle

The application startup follows the Cats Effect `Resource` pattern:

```
Resource[IO, _]:
  DatabasePool (Doobie/HikariCP)
    -> StorageEngine (Arrow allocator, Parquet writers, Lucene indexes)
      -> QueryEngine (Calcite schema, execution pools)
        -> CatalogEngine (MetadataRepository, scanners)
          -> GovernanceEngine (classifiers, audit logger)
            -> gRPC Server (Netty)
              -> HTTP Server (Ember)
                -> Metrics Server
                  -> Health Server
```

All resources are released in reverse order on shutdown.

### Configuration

Configuration is loaded via PureConfig into the typed `UdpsConfig` case class hierarchy (defined in `udps-core/src/main/scala/io/gbmm/udps/core/config/UdpsConfig.scala`). Every field has an environment variable override defined in `application.conf` using the `${?ENV_VAR}` pattern.

## External Integrations

### USP (Unified Security Platform)

UDPS registers as a compute outpost (`OUTPOST_TYPE_COMPUTE = 6`) with USP and delegates all authentication and authorization decisions.

| Integration | Proto file | Purpose |
|-------------|-----------|---------|
| Outpost registration | `usp/outpost.proto` | Register, heartbeat, stream configuration |
| Token validation | `usp/authentication.proto` | Validate JWT tokens on every API request |
| Permission checks | `usp/authorization.proto` | Check permissions before data access |
| Secrets management | `usp/secrets.proto` | Retrieve database credentials, encryption keys |

Standard circuit breaker pattern: 5 failures in 30 seconds triggers the breaker; mTLS on all USP connections.

### UCCP (Unified Coordination & Control Plane)

UDPS registers with UCCP for service discovery so that other SeRi-SA services can locate it.

| Integration | Proto file | Purpose |
|-------------|-----------|---------|
| Service discovery | `uccp/coordination.proto` | Register, heartbeat, discover peer services |

## Technology Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| Language | Scala | 2.13.12 | Primary language |
| Runtime | JDK | 17 (Temurin) | JVM runtime |
| Effect system | Cats Effect | 3.5.3 | Functional effect management |
| Streaming | FS2 | 3.9.4 | Functional streams |
| HTTP server | http4s Ember | 0.23.25 | REST API |
| JSON | Circe | 0.14.6 | JSON serialization |
| gRPC | ScalaPB / grpc-netty | 0.11.15 / 1.60.1 | gRPC server and client stubs |
| SQL engine | Apache Calcite | 1.35.0 | SQL parsing and optimization |
| Columnar memory | Apache Arrow | 14.0.1 | In-memory columnar data |
| File format | Apache Parquet | 1.13.1 | Persistent columnar storage |
| Full-text search | Apache Lucene | 9.9.2 | Text indexing and search |
| Bitmap indexing | RoaringBitmap | 0.9.45 | Compressed bitmap indexes |
| Database | PostgreSQL (Doobie) | 16 / 1.0.0-RC5 | Metadata catalog persistence |
| Object storage | MinIO (S3 API) | 8.5.7 | Archive tier |
| Cache | Redis (redis4cats) | 7 / 1.7.1 | Query cache, distributed state |
| Messaging | Kafka (fs2-kafka) | 3.6.1 / 3.2.0 | Event streaming, CDC |
| Clustering | Akka Cluster | 2.8.5 | Distributed coordination |
| Configuration | PureConfig | 0.17.5 | Typed configuration loading |
| Logging | Logback + scala-logging | 1.4.14 / 3.9.5 | Structured logging |
| Tracing | OpenTelemetry + Jaeger | -- | Distributed tracing |
| Testing | ScalaTest + Testcontainers | 3.2.17 / 0.41.2 | Unit and integration tests |
| Build | SBT | 1.9.x | Build tool with multi-module support |
| Container | Docker (Temurin Alpine) | -- | Packaging and deployment |
