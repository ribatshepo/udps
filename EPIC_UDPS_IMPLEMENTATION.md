# Epic: Unified Data Platform Service (UDPS) Implementation

**Epic ID:** EPIC-UDPS-001  
**Created:** 2026-02-01  
**Status:** Planning  
**Technology Stack:** Scala 2.13, Java 17, Apache Calcite 1.35.0, Apache Arrow 14.0.1, Apache Parquet 1.13.x  
**Target Version:** 1.0.0  

---

## Phase 1: Scope Analysis

### Current State

**Existing Assets:**
- Comprehensive specification: `/home/tshepo/projects/seri-sa/udps/docs/udps.md`
- Empty repository with placeholder README.md
- Git repository initialized

**Infrastructure Context:**
- SeRi-SA ecosystem with established patterns:
  - USP (Unified Security Platform) for authentication, authorization, and secrets management
  - UCCP (Unified Coordination & Control Plane) for service discovery and coordination
  - USTRP (Unified Stream Runtime Platform) for stream processing
- Proven integration patterns from sister services (C++20, .NET 8)
- Docker-compose overlay networking (seri-sa-platform shared network)
- Standard circuit breaker pattern (5 failures/30s)
- mTLS everywhere

**No Existing Code:** This is a greenfield project requiring complete implementation from scratch.

### Target State

**Production-Ready Service Delivering:**

1. **Unified Columnar Storage** - Arrow/Parquet with 8 compression codecs, 500 MB/sec ingestion
2. **Tiered Storage** - Hot (NVMe), Warm (SSD), Cold (HDD), Archive (S3/MinIO) with automatic transitions
3. **Advanced Indexing** - Zone Maps, Bloom Filters, Bitmap, Skip Lists, Inverted indexes
4. **Full-Text Search** - Lucene-powered with multiple analyzers and real-time indexing
5. **Materialized Views** - Pre-computed results with multiple refresh modes
6. **SQL Query Engine** - Calcite-based SQL:2016 compliance, 10,000+ queries/sec
7. **Data Source Federation** - Connect to 20+ data sources with intelligent pushdown
8. **Metadata Catalog** - Auto-discovery, versioning, search (5,000 ops/sec)
9. **GraphQL API** - Sangria-powered flexible querying (3,000 queries/sec)
10. **Data Lineage** - Table and column-level tracking with impact analysis
11. **Data Profiling** - Comprehensive statistics and quality metrics
12. **Data Sampling** - Multiple strategies with reproducibility
13. **Query Builder & History** - Visual building with optimization recommendations
14. **Data Governance** - PII classification, anonymization, GDPR compliance
15. **Tagging & Glossary** - Hierarchical organization with auto-tagging
16. **Advanced Query Features** - Time travel, CDC, ACID transactions
17. **Access Control & Security** - JWT, mTLS, RBAC/ABAC/PBAC, column-level security
18. **Performance Monitoring** - Query analysis, slow query detection, auto-tuning
19. **Cost Analysis** - Multi-tier cost tracking with optimization
20. **ERD Generation** - Automatic diagram creation with multiple layouts
21. **ML Integration** - Feature store with versioning and lineage

**APIs:**
- gRPC on port 50060 (TLS) - StorageService, QueryService, CatalogService, HealthService
- HTTPS on port 8443 - REST API + GraphQL endpoint
- Metrics on port 9090 (Prometheus)
- Health on port 8081

**Integration:**
- USP outpost registration (OUTPOST_TYPE_COMPUTE=6)
- UCCP service discovery and coordination
- USTRP stream data consumption via Kafka

**Deployment:**
- Docker container (eclipse-temurin:17-jre-alpine, ~500MB)
- Kubernetes-ready with Helm chart
- Horizontal scaling to 100+ nodes
- Petabyte-scale storage capacity

### Gap Assessment

| Area | Current | Target | Gap |
|------|---------|--------|-----|
| Project Structure | None | SBT multi-module (core, storage, query, catalog, governance, api, integration) | Need complete scaffolding with 7+ modules |
| Storage Engine | None | Arrow/Parquet with 8 codecs, 4-tier storage, 6 index types | Full implementation required (~15 components) |
| Query Engine | None | Calcite integration, distributed execution, vectorized processing | Full implementation required (~10 components) |
| Catalog Engine | None | PostgreSQL schema, discovery, lineage, profiling | Full implementation required (~8 components) |
| Governance | None | PII classification, anonymization, GDPR compliance | Full implementation required (~6 components) |
| API Layer | None | gRPC (ScalaPB), REST (Akka HTTP), GraphQL (Sangria) | 3 complete API implementations |
| USP Integration | None | 4 client implementations (outpost, auth, authz, secrets) | Full client stack with circuit breakers |
| UCCP Integration | None | Service discovery client | Full client implementation |
| USTRP Integration | None | Kafka consumer with exactly-once semantics | Full consumer implementation |
| Deployment | None | Docker, docker-compose, Helm chart | Complete deployment pipeline |
| Observability | None | Metrics, tracing, logging | Full observability stack |
| Testing | None | Unit, integration, load tests | Comprehensive test suite |
| Documentation | README placeholder | Architecture docs, API docs, deployment guides | Complete documentation set |

### Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| Large scope (21+ feature categories) | HIGH | Phased delivery (P0-P10), parallel workstreams where possible |
| Complex Calcite integration | HIGH | Study reference implementations, allocate expert time, prototype early |
| Multi-service dependencies (USP/UCCP/USTRP) | MEDIUM | Copy proto files locally, mock integration endpoints for testing, contract testing |
| Arrow/Parquet performance optimization | MEDIUM | Benchmark early, use proven patterns from Arrow documentation, load testing |
| Tiered storage complexity | MEDIUM | Start with hot tier only, add warm/cold/archive incrementally |
| Security requirements (mTLS everywhere) | MEDIUM | Reuse USTRP patterns, certificate management automation |
| Scala 2.13 + Java 17 interop | LOW | Use ScalaPB for proto generation, standard SBT patterns |
| Context size for large modules | MEDIUM | Keep modules focused (<7 files per task), use clear interfaces |
| Test coverage for 21+ features | MEDIUM | TDD approach, integration tests per phase, load test infrastructure early |

### Out of Scope

- Modifying USP, UCCP, or USTRP codebases (consume as-is via proto contracts)
- Python/R SDKs (future roadmap item)
- Web UI for data catalog (future roadmap item)
- Migration tools from existing data platforms (future roadmap item)
- Custom Calcite optimizer rules beyond standard set (future optimization)

---

## Phase 2: Categorized Task Decomposition

### Overview

| Field | Value |
|-------|-------|
| Epic ID | EPIC-UDPS-001 |
| Parent | None (root epic) |
| Priority | HIGH |
| Total Tasks | 87 |
| Categories | 11 |
| Programs | 11 (P0-P10) |

### Category 1: Foundation (Program 0)

> Establish project structure, build system, proto integration, and development environment.

| ID | Title | Size | Risk | Depends | dispatch_hint |
|----|-------|------|------|---------|---------------|
| UDPS-001 | SBT Multi-Module Project Setup | small | low | - | implementer |
| UDPS-002 | Copy USP Proto Files | small | low | UDPS-001 | implementer |
| UDPS-003 | Copy UCCP Proto Files | small | low | UDPS-001 | implementer |
| UDPS-004 | Configure ScalaPB Generation | small | low | UDPS-002, UDPS-003 | implementer |
| UDPS-005 | Docker Compose Dev Environment | medium | low | UDPS-001 | implementer |
| UDPS-006 | Core Domain Models | small | low | UDPS-001 | implementer |
| UDPS-007 | Configuration Management | small | low | UDPS-001 | implementer |

**UDPS-001: SBT Multi-Module Project Setup**
- **Description**: Create SBT build with 7 modules: udps-core (domain models, config), udps-storage (Arrow/Parquet), udps-query (Calcite), udps-catalog (metadata), udps-governance (PII/GDPR), udps-api (REST/GraphQL/gRPC), udps-integration (USP/UCCP/USTRP clients). Configure Scala 2.13.12, Java 17, dependency management, assembly plugin for fat JARs.
- **Acceptance Criteria**: `sbt compile` succeeds for all modules; `sbt test` runs (no tests yet); module dependencies are correct (api depends on all others); cross-compilation works; assembly plugin configured.
- **Files**: `build.sbt`, `project/build.properties`, `project/plugins.sbt`, `project/Dependencies.scala`, module directories with stub `build.sbt` files
- **Risk**: low - Standard SBT pattern
- **dispatch_hint**: `implementer`

**UDPS-002: Copy USP Proto Files**
- **Description**: Copy proto files from `/home/tshepo/projects/seri-sa/ustrp/stream-compute/proto/usp/` to `udps-integration/src/main/protobuf/usp/`: outpost.proto, authentication.proto, authorization.proto, secrets.proto, common.proto, session.proto. Preserve package structure.
- **Acceptance Criteria**: All 6 USP proto files copied; package declarations intact; no syntax errors; ready for ScalaPB generation.
- **Files**: `udps-integration/src/main/protobuf/usp/*.proto` (6 files)
- **Risk**: low - Simple file copy
- **dispatch_hint**: `implementer`

**UDPS-003: Copy UCCP Proto Files**
- **Description**: Copy proto files from `/home/tshepo/projects/seri-sa/ustrp/stream-compute/proto/uccp/` to `udps-integration/src/main/protobuf/uccp/`: coordination.proto, common.proto. Preserve package structure.
- **Acceptance Criteria**: Both UCCP proto files copied; package declarations intact; no syntax errors; ready for ScalaPB generation.
- **Files**: `udps-integration/src/main/protobuf/uccp/*.proto` (2 files)
- **Risk**: low - Simple file copy
- **dispatch_hint**: `implementer`

**UDPS-004: Configure ScalaPB Generation**
- **Description**: Configure ScalaPB 0.11.x in `project/plugins.sbt` and module build files. Enable gRPC service stubs generation for USP/UCCP protos. Configure Akka gRPC support for UDPS service definitions (to be added later). Set proto source directories, generated code output paths.
- **Acceptance Criteria**: `sbt protocGenerate` succeeds; Scala client stubs for USP/UCCP services generated in `udps-integration/target/scala-2.13/src_managed/main/`; no compilation errors; gRPC service traits available.
- **Files**: `project/plugins.sbt`, `udps-integration/build.sbt`, `udps-api/build.sbt`
- **Risk**: low - Standard ScalaPB setup
- **dispatch_hint**: `implementer`

**UDPS-005: Docker Compose Dev Environment**
- **Description**: Create `docker-compose.dev.yml` with services: PostgreSQL 16 (metadata catalog), MinIO (S3-compatible archive tier), Redis 7 (distributed cache), Kafka 3.6 + Zookeeper (streaming), Jaeger (tracing). Join seri-sa-platform external network for integration with USP/UCCP/USTRP. Configure ports, volumes, health checks. Include init scripts for Postgres schema setup (placeholder).
- **Acceptance Criteria**: `docker-compose -f docker-compose.dev.yml up -d` starts all services; PostgreSQL accessible on 5432; MinIO on 9000; Redis on 6379; Kafka on 9092; Jaeger UI on 16686; all containers healthy; volumes persist data.
- **Files**: `docker-compose.dev.yml`, `.env.dev`, `docker/postgres-init/`
- **Risk**: low - Standard docker-compose pattern
- **dispatch_hint**: `implementer`

**UDPS-006: Core Domain Models**
- **Description**: Define core domain models in `udps-core/src/main/scala/io/gbmm/udps/core/domain/`: TableMetadata, ColumnMetadata, SchemaInfo, DataType (enum), PartitionInfo, StorageTier (enum), IndexType (enum), QueryPlan, CompressionCodec (enum). Use case classes with proper immutability.
- **Acceptance Criteria**: All domain models compile; case classes immutable; proper types for all fields; toString/equals/hashCode from case class; no external dependencies (pure domain).
- **Files**: `udps-core/src/main/scala/io/gbmm/udps/core/domain/*.scala` (~10 files)
- **Risk**: low - Pure domain modeling
- **dispatch_hint**: `implementer`

**UDPS-007: Configuration Management**
- **Description**: Implement configuration using Typesafe Config. Define `application.conf` with settings for all services (gRPC port 50060, HTTPS port 8443, metrics port 9090, health port 8081, database connection, MinIO endpoint, Kafka brokers, TLS paths, worker threads, timeouts). Create Config case class in udps-core to load and validate settings. Support environment variable overrides.
- **Acceptance Criteria**: Config loads from application.conf; environment variables override file settings; validation fails fast on missing required config; all ports, endpoints, paths configurable; TLS settings present.
- **Files**: `udps-core/src/main/resources/application.conf`, `udps-core/src/main/scala/io/gbmm/udps/core/config/Config.scala`
- **Risk**: low - Standard Typesafe Config pattern
- **dispatch_hint**: `implementer`

---

### Category 2: Storage Engine (Program 1)

> Implement columnar storage with Arrow/Parquet, compression, tiering, and indexing.

| ID | Title | Size | Risk | Depends | dispatch_hint |
|----|-------|------|------|---------|---------------|
| UDPS-008 | Arrow Schema Adapter | small | low | UDPS-006 | implementer |
| UDPS-009 | Parquet Reader | medium | medium | UDPS-008 | implementer |
| UDPS-010 | Parquet Writer with Compression | medium | medium | UDPS-008 | implementer |
| UDPS-011 | Record Batch Processor | medium | low | UDPS-008 | implementer |
| UDPS-012 | Storage Tier Manager | medium | medium | UDPS-010 | implementer |
| UDPS-013 | Zone Map Indexer | medium | low | UDPS-010 | implementer |
| UDPS-014 | Bloom Filter Indexer | medium | low | UDPS-010 | implementer |
| UDPS-015 | Bitmap Indexer | small | low | UDPS-010 | implementer |
| UDPS-016 | Skip List Indexer | small | low | UDPS-010 | implementer |
| UDPS-017 | Full-Text Search (Lucene) | medium | medium | UDPS-010 | implementer |
| UDPS-018 | Materialized View Engine | medium | medium | UDPS-010 | implementer |
| UDPS-019 | ACID Transaction Coordinator | medium | high | UDPS-010 | implementer |
| UDPS-020 | Snapshot Manager (MVCC) | medium | high | UDPS-019 | implementer |

**UDPS-008: Arrow Schema Adapter**
- **Description**: Implement bidirectional mapping between UDPS domain models (TableMetadata, ColumnMetadata, DataType) and Apache Arrow Schema. Handle all Arrow types: INT8-64, UINT8-64, FLOAT, DOUBLE, STRING, BINARY, BOOL, DATE32/64, TIMESTAMP, DECIMAL, LIST, STRUCT, MAP. Support metadata preservation.
- **Acceptance Criteria**: Convert TableMetadata to Arrow Schema; convert Arrow Schema to TableMetadata; round-trip conversion preserves all information; all Arrow types supported; custom metadata (like PII tags) preserved in Arrow metadata.
- **Files**: `udps-storage/src/main/scala/io/gbmm/udps/storage/arrow/SchemaAdapter.scala`
- **Risk**: low - Arrow Java API is stable
- **dispatch_hint**: `implementer`

**UDPS-009: Parquet Reader**
- **Description**: Implement Parquet file reader using Apache Parquet Java library. Support column projection (read subset of columns), row group filtering, predicate pushdown (simple predicates on Zone Maps). Integrate with Arrow for in-memory representation. Handle schema evolution (add columns, compatible type changes). Support reading compressed files (LZ4, ZSTD, Snappy, GZIP).
- **Acceptance Criteria**: Read Parquet file into Arrow RecordBatch; column projection works (only requested columns read); predicate pushdown reduces I/O (Zone Map filtering); compressed files decompress correctly; schema evolution handled gracefully; metadata extracted.
- **Files**: `udps-storage/src/main/scala/io/gbmm/udps/storage/parquet/ParquetReader.scala`, `udps-storage/src/main/scala/io/gbmm/udps/storage/parquet/PredicatePushdown.scala`
- **Risk**: medium - Complex predicate pushdown logic
- **dispatch_hint**: `implementer`

**UDPS-010: Parquet Writer with Compression**
- **Description**: Implement Parquet file writer using Apache Parquet Java library. Support 8 compression codecs: UNCOMPRESSED, LZ4, ZSTD, SNAPPY, GZIP, BROTLI, ZSTD (dictionary), LZ4 (raw). Accept Arrow RecordBatch as input. Configure row group size (default 1M rows), page size, dictionary encoding. Write metadata including custom properties (table name, partition info, creation time). Support append mode for incremental writes.
- **Acceptance Criteria**: Write Arrow RecordBatch to Parquet file; all 8 compression codecs work; compressed file size validates (e.g., ZSTD smaller than UNCOMPRESSED); metadata written correctly; row group size configurable; round-trip with ParquetReader preserves data; append mode works.
- **Files**: `udps-storage/src/main/scala/io/gbmm/udps/storage/parquet/ParquetWriter.scala`, `udps-storage/src/main/scala/io/gbmm/udps/storage/parquet/CompressionConfig.scala`
- **Risk**: medium - Compression tuning can be tricky
- **dispatch_hint**: `implementer`

**UDPS-011: Record Batch Processor**
- **Description**: Implement batch processing engine that streams data through Arrow RecordBatches. Support configurable batch sizes (default 64K rows). Handle back-pressure (Akka Streams). Implement transformations: filter, project, sort, aggregate (sum, count, min, max, avg). Use vectorized operations where possible (Arrow compute kernels). Integrate with Parquet reader/writer for streaming I/O.
- **Acceptance Criteria**: Stream large dataset (>1GB) without OOM; configurable batch size works; transformations correct (verified against known results); back-pressure prevents memory exhaustion; throughput >100 MB/sec on single core.
- **Files**: `udps-storage/src/main/scala/io/gbmm/udps/storage/batch/RecordBatchProcessor.scala`, `udps-storage/src/main/scala/io/gbmm/udps/storage/batch/Transformations.scala`
- **Risk**: low - Akka Streams handles complexity
- **dispatch_hint**: `implementer`

**UDPS-012: Storage Tier Manager**
- **Description**: Implement 4-tier storage: Hot (local path, NVMe), Warm (local path, SSD), Cold (local path, HDD), Archive (MinIO S3). Tier policy engine with rules: age (>7d -> Warm, >30d -> Cold, >90d -> Archive), access frequency (<10/month -> Cold), size (>10GB -> Archive), manual override. Asynchronous tier transitions (background job). Metadata tracks current tier in PostgreSQL. Transparent reads (fetch from Archive if needed, cache in Hot).
- **Acceptance Criteria**: Write to Hot tier succeeds; automatic transition to Warm after 7 days (simulated clock); manual tier transition works; read from Archive fetches and caches to Hot; MinIO client works (presigned URLs); policy rules evaluated correctly; PostgreSQL metadata updated on tier change.
- **Files**: `udps-storage/src/main/scala/io/gbmm/udps/storage/tiering/TierManager.scala`, `udps-storage/src/main/scala/io/gbmm/udps/storage/tiering/TierPolicy.scala`, `udps-storage/src/main/scala/io/gbmm/udps/storage/tiering/MinIOClient.scala`
- **Risk**: medium - S3 integration and policy complexity
- **dispatch_hint**: `implementer`

**UDPS-013: Zone Map Indexer**
- **Description**: Implement Zone Map indexing (min/max per row group per column). Generate during Parquet write. Store in Parquet metadata. Use for predicate pushdown (skip row groups outside range). Support numeric, string, date/timestamp types. Handle nulls (track null count per row group). Integrate with ParquetReader for automatic filtering.
- **Acceptance Criteria**: Zone Maps generated for all columns during write; min/max values correct; predicate pushdown skips row groups (e.g., `WHERE age > 50` skips row groups with max age < 50); null count tracked; query with Zone Map filtering is faster than full scan (measured).
- **Files**: `udps-storage/src/main/scala/io/gbmm/udps/storage/indexing/ZoneMapIndexer.scala`
- **Risk**: low - Parquet native support
- **dispatch_hint**: `implementer`

**UDPS-014: Bloom Filter Indexer**
- **Description**: Implement Bloom Filter indexing for membership testing (e.g., `WHERE user_id IN (...)`). Generate during write with configurable FPR (default 1%). Store as separate index files (`.bloom` extension alongside `.parquet`). Support string, integer, binary columns. Use 64-bit hash (Murmur3). Load Bloom Filter on query to skip row groups. Integrate with query planner.
- **Acceptance Criteria**: Bloom Filter generated with configurable FPR; false positive rate measured matches configuration; membership test works (IN clause uses Bloom Filter); query with Bloom Filter is faster than full scan for high-selectivity predicates; index file size reasonable (<1% of data size for FPR=1%).
- **Files**: `udps-storage/src/main/scala/io/gbmm/udps/storage/indexing/BloomFilterIndexer.scala`, `udps-storage/src/main/scala/io/gbmm/udps/storage/indexing/BloomFilterReader.scala`
- **Risk**: low - Standard Bloom Filter implementation
- **dispatch_hint**: `implementer`

**UDPS-015: Bitmap Indexer**
- **Description**: Implement Bitmap indexing for low-cardinality categorical columns (e.g., status, category, country). Store compressed bitmaps (RoaringBitmap) in separate index files. Support AND/OR/NOT queries. Generate during write. Update incrementally on new data. Integrate with query planner to use for WHERE clause evaluation.
- **Acceptance Criteria**: Bitmap index generated for categorical column; compressed size is small (<5% of data size); AND/OR/NOT queries use bitmap operations; query with bitmap index is faster than column scan; incremental update works (add new rows without full rebuild).
- **Files**: `udps-storage/src/main/scala/io/gbmm/udps/storage/indexing/BitmapIndexer.scala`
- **Risk**: low - RoaringBitmap library handles complexity
- **dispatch_hint**: `implementer`

**UDPS-016: Skip List Indexer**
- **Description**: Implement Skip List indexing for ordered data (e.g., sorted by timestamp). Store skip pointers to row groups in separate index file. Use for range queries (e.g., `WHERE timestamp BETWEEN ... AND ...`). Generate during write if data is sorted. Integrate with query planner to jump to relevant row groups.
- **Acceptance Criteria**: Skip List index generated for sorted column; range query uses index to skip irrelevant row groups; query latency reduced for time-range queries; index file size reasonable (<1% of data size).
- **Files**: `udps-storage/src/main/scala/io/gbmm/udps/storage/indexing/SkipListIndexer.scala`
- **Risk**: low - Simple data structure
- **dispatch_hint**: `implementer`

**UDPS-017: Full-Text Search (Lucene)**
- **Description**: Implement full-text search using Apache Lucene 9.x. Index string columns marked for FTS. Support analyzers: StandardAnalyzer, NGramAnalyzer (2-3 grams), PhoneticAnalyzer (Metaphone), Language-specific (English, Spanish). Query types: Boolean, Phrase, Fuzzy (Levenshtein distance ≤2), Wildcard, Prefix. Ranking: TF-IDF and BM25. Real-time indexing (<1s latency). Store Lucene index in Hot tier. Integrate with catalog (mark columns as FTS-enabled).
- **Acceptance Criteria**: Index created for FTS-enabled columns; all 4 analyzers work; all 5 query types return correct results; ranking orders results by relevance; incremental indexing adds new rows in <1s; search query latency <100ms for 1M documents.
- **Files**: `udps-storage/src/main/scala/io/gbmm/udps/storage/fts/LuceneIndexer.scala`, `udps-storage/src/main/scala/io/gbmm/udps/storage/fts/LuceneSearcher.scala`, `udps-storage/src/main/scala/io/gbmm/udps/storage/fts/Analyzers.scala`
- **Risk**: medium - Lucene integration can be complex
- **dispatch_hint**: `implementer`

**UDPS-018: Materialized View Engine**
- **Description**: Implement materialized views with 4 refresh modes: ON_DEMAND (manual trigger), SCHEDULED (cron expression), INCREMENTAL (delta changes only), CONTINUOUS (streaming). Store view definition (SQL query) in PostgreSQL catalog. Store view data as Parquet in Hot tier. Automatic query rewriting (optimizer detects matching view). Partition-aware (view can be partitioned like base tables). Track dependencies (view invalidated if base table schema changes).
- **Acceptance Criteria**: Create materialized view with SQL definition; ON_DEMAND refresh computes and stores results; SCHEDULED refresh executes on cron schedule; INCREMENTAL refresh applies only changes since last refresh; query rewriter uses view when applicable (verified in explain plan); view data readable as regular table; view invalidated on schema change.
- **Files**: `udps-storage/src/main/scala/io/gbmm/udps/storage/views/MaterializedViewEngine.scala`, `udps-storage/src/main/scala/io/gbmm/udps/storage/views/ViewRefresher.scala`, `udps-storage/src/main/scala/io/gbmm/udps/storage/views/QueryRewriter.scala`
- **Risk**: medium - Query rewriting is complex
- **dispatch_hint**: `implementer`

**UDPS-019: ACID Transaction Coordinator**
- **Description**: Implement ACID transaction support with snapshot isolation. Use Two-Phase Commit (2PC) for distributed transactions across partitions. Transaction log stored in PostgreSQL (transaction ID, start time, commit time, status, affected partitions). Pessimistic locking (row-level locks in metadata DB). Timeout handling (abort after 5 minutes). Deadlock detection (abort younger transaction). Integrate with Parquet writer (buffered writes, commit on transaction success).
- **Acceptance Criteria**: Transaction starts and commits successfully; rollback discards buffered writes; concurrent transactions isolated (read committed snapshot); 2PC works across partitions; deadlock detected and resolved; transaction timeout aborts after 5 minutes; PostgreSQL transaction log updated correctly.
- **Files**: `udps-storage/src/main/scala/io/gbmm/udps/storage/txn/TransactionCoordinator.scala`, `udps-storage/src/main/scala/io/gbmm/udps/storage/txn/LockManager.scala`, `udps-storage/src/main/scala/io/gbmm/udps/storage/txn/TransactionLog.scala`
- **Risk**: high - Distributed transactions are complex
- **dispatch_hint**: `implementer`

**UDPS-020: Snapshot Manager (MVCC)**
- **Description**: Implement Multi-Version Concurrency Control (MVCC) for time-travel queries. Store multiple versions of data (versioned Parquet files with timestamp suffix). Snapshot metadata in PostgreSQL (snapshot ID, timestamp, file list). `SELECT ... AS OF TIMESTAMP '...'` syntax support. Garbage collection (delete old snapshots after retention period, default 30 days). Integrate with transaction coordinator (new version on commit). Read from specific snapshot without blocking writes.
- **Acceptance Criteria**: Write creates new snapshot; time-travel query reads from historical snapshot; concurrent read and write work (no blocking); snapshot metadata stored in PostgreSQL; GC deletes snapshots older than 30 days; snapshot ID matches commit transaction ID.
- **Files**: `udps-storage/src/main/scala/io/gbmm/udps/storage/mvcc/SnapshotManager.scala`, `udps-storage/src/main/scala/io/gbmm/udps/storage/mvcc/SnapshotGC.scala`
- **Risk**: high - MVCC complexity and GC coordination
- **dispatch_hint**: `implementer`

---

### Category 3: Query Engine (Program 2)

> Implement SQL query processing with Calcite, distributed execution, and optimizations.

| ID | Title | Size | Risk | Depends | dispatch_hint |
|----|-------|------|------|---------|---------------|
| UDPS-021 | Calcite Schema Adapter | medium | medium | UDPS-006, UDPS-008 | implementer |
| UDPS-022 | SQL Parser Integration | small | low | UDPS-021 | implementer |
| UDPS-023 | Logical Query Optimizer | medium | high | UDPS-022 | implementer |
| UDPS-024 | Cost-Based Optimizer | medium | high | UDPS-023 | implementer |
| UDPS-025 | Physical Plan Generator | medium | medium | UDPS-024 | implementer |
| UDPS-026 | Distributed Execution Engine | medium | high | UDPS-025 | implementer |
| UDPS-027 | Vectorized Executor | medium | high | UDPS-011, UDPS-026 | implementer |
| UDPS-028 | Query Cache | small | low | UDPS-026 | implementer |
| UDPS-029 | Adaptive Query Execution | medium | high | UDPS-026 | implementer |
| UDPS-030 | Data Source Federation | medium | medium | UDPS-021 | implementer |

**UDPS-021: Calcite Schema Adapter**
- **Description**: Implement Calcite Schema adapter that bridges UDPS metadata catalog to Calcite's schema model. Extend AbstractSchema, implement getTableMap() to return UDPS tables. Each table implements ScannableTable or FilterableTable. Map UDPS DataTypes to Calcite SqlTypeName. Support statistics (row count, column cardinality) for cost-based optimization. Lazy loading (fetch metadata on demand).
- **Acceptance Criteria**: Calcite can enumerate UDPS tables; table schema matches UDPS metadata; column types mapped correctly; statistics available for optimizer; lazy loading works (only queries tables used in query).
- **Files**: `udps-query/src/main/scala/io/gbmm/udps/query/calcite/UDPSSchema.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/calcite/UDPSTable.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/calcite/TypeMapper.scala`
- **Risk**: medium - Calcite API learning curve
- **dispatch_hint**: `implementer`

**UDPS-022: SQL Parser Integration**
- **Description**: Integrate Calcite SQL parser (SqlParser) with SQL:2016 grammar. Configure parser for UDPS-specific extensions: `AS OF TIMESTAMP` (time travel), `CREATE MATERIALIZED VIEW`, tier management DDL. Return SqlNode AST. Validate SQL syntax. Support prepared statements (parameterized queries). Error messages include line/column position.
- **Acceptance Criteria**: Parse valid SQL:2016 queries; parse UDPS extensions (AS OF, materialized views); syntax errors include line/column info; parameterized queries work (? placeholders); SqlNode AST available for optimizer.
- **Files**: `udps-query/src/main/scala/io/gbmm/udps/query/parser/SQLParser.scala`, `udps-query/src/main/resources/calcite-parser-config.properties`
- **Risk**: low - Calcite parser is mature
- **dispatch_hint**: `implementer`

**UDPS-023: Logical Query Optimizer**
- **Description**: Implement Calcite logical optimizer using HepPlanner and VolcanoPlanner. Apply rule sets: predicate pushdown, projection pushdown, constant folding, filter simplification, join reordering (commutativity), subquery decorrelation. Convert SqlNode to RelNode (relational algebra). Output optimized logical plan. Integrate with materialized view rewriter (UDPS-018).
- **Acceptance Criteria**: Logical plan optimized (predicates pushed down, projections minimized); join order improved for complex queries; subqueries decorrelated; constants folded; materialized view rewriter invoked; explain plan shows optimizations applied.
- **Files**: `udps-query/src/main/scala/io/gbmm/udps/query/optimizer/LogicalOptimizer.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/optimizer/OptimizationRules.scala`
- **Risk**: high - Optimizer tuning requires expertise
- **dispatch_hint**: `implementer`

**UDPS-024: Cost-Based Optimizer**
- **Description**: Implement cost-based optimization using Calcite VolcanoPlanner. Define cost model: I/O cost (bytes scanned), CPU cost (rows processed), memory cost (hash joins). Collect statistics from catalog (row count, distinct values, min/max, histograms). Estimate selectivity for predicates. Choose physical operators based on cost (hash join vs nested loop, index scan vs full scan). Integrate with index metadata (Zone Maps, Bloom Filters).
- **Acceptance Criteria**: Cost model estimates I/O, CPU, memory costs; statistics used for selectivity estimation; optimizer chooses hash join for large tables, nested loop for small tables; index scan chosen when beneficial; explain plan includes cost estimates.
- **Files**: `udps-query/src/main/scala/io/gbmm/udps/query/optimizer/CostBasedOptimizer.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/optimizer/CostModel.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/optimizer/StatisticsProvider.scala`
- **Risk**: high - Cost model accuracy critical for performance
- **dispatch_hint**: `implementer`

**UDPS-025: Physical Plan Generator**
- **Description**: Convert optimized RelNode to executable physical plan. Define physical operators: TableScan (read Parquet with predicate pushdown), Filter, Project, HashJoin, NestedLoopJoin, HashAggregate, Sort, Limit. Each operator extends Iterator[RecordBatch]. Operator chains as pipeline. Support partition-wise execution (parallel scans across partitions). Generate execution DAG.
- **Acceptance Criteria**: Physical plan generated from RelNode; all SQL operators supported (scan, filter, project, join, aggregate, sort, limit); partition-wise execution enabled for large tables; execution DAG correct (dependencies between operators); plan serializable (for distributed execution).
- **Files**: `udps-query/src/main/scala/io/gbmm/udps/query/physical/PhysicalPlanner.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/physical/Operators.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/physical/ExecutionDAG.scala`
- **Risk**: medium - Operator implementation complexity
- **dispatch_hint**: `implementer`

**UDPS-026: Distributed Execution Engine**
- **Description**: Implement distributed query execution using Akka Cluster. Master node (query coordinator) splits plan into stages (shuffle boundaries). Worker nodes execute stages. Shuffle data via Akka Cluster sharding. Work stealing for load balancing. Fault tolerance (retry failed stages on different worker). Combine results at coordinator. Support partition pruning (skip partitions based on predicates). Monitor execution via metrics.
- **Acceptance Criteria**: Query executes across 3-node cluster; stages distributed to workers; shuffle exchanges data correctly; work stealing balances load; failed worker retried on another node; results combined correctly; partition pruning reduces I/O (measured); metrics track stage execution time.
- **Files**: `udps-query/src/main/scala/io/gbmm/udps/query/execution/DistributedExecutor.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/execution/QueryCoordinator.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/execution/StageExecutor.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/execution/ShuffleExchange.scala`
- **Risk**: high - Distributed systems complexity
- **dispatch_hint**: `implementer`

**UDPS-027: Vectorized Executor**
- **Description**: Implement vectorized execution for filter, project, aggregate operators using Arrow compute kernels. Batch-at-a-time processing (process entire RecordBatch in one call). Use SIMD operations (Arrow C++ kernels via JNI). Specialize for common data types (INT64, DOUBLE, STRING). Benchmark against row-at-a-time execution (target 5-10x speedup). Integrate with physical operators (UDPS-025).
- **Acceptance Criteria**: Filter operator processes RecordBatch in vectorized mode; 5-10x speedup vs row-at-a-time (measured on benchmark); Arrow compute kernels used (verified via profiling); all common types supported (INT64, DOUBLE, STRING, BOOL); integration with physical plan works.
- **Files**: `udps-query/src/main/scala/io/gbmm/udps/query/vectorized/VectorizedFilter.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/vectorized/VectorizedProject.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/vectorized/VectorizedAggregate.scala`
- **Risk**: high - SIMD and JNI complexity
- **dispatch_hint**: `implementer`

**UDPS-028: Query Cache**
- **Description**: Implement query result caching using Apache Ignite. Cache key: SQL text + parameters (normalized). Cache value: serialized Arrow RecordBatches. TTL configurable (default 1 hour). Invalidation: on table write/update, manual invalidate. LRU eviction policy. Cache hit metric. Integrate with query executor (check cache before execution).
- **Acceptance Criteria**: Identical query returns cached result (2nd execution <10ms); cache hit metric increments; TTL expires after 1 hour (result recomputed); invalidation on write clears affected entries; LRU evicts least-used entries when cache full; cache miss executes query normally.
- **Files**: `udps-query/src/main/scala/io/gbmm/udps/query/cache/QueryCache.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/cache/CacheInvalidator.scala`
- **Risk**: low - Ignite simplifies distributed cache
- **dispatch_hint**: `implementer`

**UDPS-029: Adaptive Query Execution**
- **Description**: Implement runtime query adaptation based on actual data statistics. Monitor execution: rows processed, time per operator. Re-optimize mid-execution if estimates wrong (e.g., switch from hash join to broadcast join if one side is small). Dynamic partition pruning (prune partitions during join). Adaptive degree of parallelism (add workers if query slow). Log adaptation decisions.
- **Acceptance Criteria**: Runtime statistics collected (rows, time); re-optimization triggered when estimate off by >2x; join strategy switched mid-execution (verified in logs); dynamic partition pruning reduces partitions scanned; parallelism increased for slow queries; adaptation improves query time (measured).
- **Files**: `udps-query/src/main/scala/io/gbmm/udps/query/adaptive/AdaptiveExecutor.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/adaptive/RuntimeStatistics.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/adaptive/ReOptimizer.scala`
- **Risk**: high - Runtime re-optimization is complex
- **dispatch_hint**: `implementer`

**UDPS-030: Data Source Federation**
- **Description**: Implement federation to external data sources. Adapters for: PostgreSQL, MySQL, MongoDB, Elasticsearch, S3 (Parquet/CSV/JSON), Kafka. Each adapter implements Calcite Table interface. Pushdown: predicates, projections, limits (where supported by source). Use JDBC for relational DBs, REST APIs for NoSQL/search. Join UDPS tables with external sources (federated join). Catalog tracks external source connections.
- **Acceptance Criteria**: Query joins UDPS table with PostgreSQL table; predicate pushed down to PostgreSQL (verified in PostgreSQL logs); projection reduces columns fetched; MongoDB adapter uses aggregation pipeline; S3 adapter reads Parquet directly; Kafka adapter reads topics as streams; federation works with all 6 source types.
- **Files**: `udps-query/src/main/scala/io/gbmm/udps/query/federation/FederationAdapter.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/federation/adapters/PostgreSQLAdapter.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/federation/adapters/MySQLAdapter.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/federation/adapters/MongoDBAdapter.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/federation/adapters/ElasticsearchAdapter.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/federation/adapters/S3Adapter.scala`, `udps-query/src/main/scala/io/gbmm/udps/query/federation/adapters/KafkaAdapter.scala`
- **Risk**: medium - Each adapter has unique pushdown capabilities
- **dispatch_hint**: `implementer`

---

### Category 4: Catalog Engine (Program 3)

> Implement metadata management, schema discovery, lineage, profiling, and sampling.

| ID | Title | Size | Risk | Depends | dispatch_hint |
|----|-------|------|------|---------|---------------|
| UDPS-031 | PostgreSQL Metadata Schema | small | low | UDPS-005 | implementer |
| UDPS-032 | Metadata CRUD Operations | small | low | UDPS-031 | implementer |
| UDPS-033 | Schema Discovery Engine | medium | medium | UDPS-032 | implementer |
| UDPS-034 | Incremental Schema Scanner | medium | medium | UDPS-033 | implementer |
| UDPS-035 | Data Lineage Tracker | medium | medium | UDPS-032 | implementer |
| UDPS-036 | Lineage Graph Builder | small | low | UDPS-035 | implementer |
| UDPS-037 | Data Profiler | medium | medium | UDPS-032 | implementer |
| UDPS-038 | Quality Rules Engine | medium | medium | UDPS-037 | implementer |
| UDPS-039 | Data Sampling Engine | small | low | UDPS-032 | implementer |
| UDPS-040 | Query Builder Service | small | low | UDPS-032 | implementer |
| UDPS-041 | Query History Tracker | small | low | UDPS-032 | implementer |

(Continuing with detailed task specs for Category 4...)

**UDPS-031: PostgreSQL Metadata Schema**
- **Description**: Design and create PostgreSQL schema for metadata catalog. Tables: databases (id, name, description, created_at), schemas (id, database_id, name), tables (id, schema_id, name, row_count, size_bytes, tier, created_at, updated_at), columns (id, table_id, name, data_type, nullable, indexed, fts_enabled, pii_classified), partitions (id, table_id, partition_key, partition_value), lineage_edges (id, source_table_id, source_column_id, target_table_id, target_column_id, query_id), profiles (id, table_id, column_id, stats_json, created_at), tags (id, name, category), table_tags (table_id, tag_id), glossary_terms (id, term, definition, related_columns), snapshots (id, table_id, timestamp, file_paths_json). Indexes on all foreign keys, (name, schema_id) unique constraints. Migration script (Flyway).
- **Acceptance Criteria**: Schema created in PostgreSQL; all tables exist with correct columns and types; foreign keys and indexes created; unique constraints enforced; Flyway migration runs idempotently; sample queries perform well (<10ms for metadata lookups).
- **Files**: `udps-catalog/src/main/resources/db/migration/V001__create_metadata_schema.sql`
- **Risk**: low - Standard SQL DDL
- **dispatch_hint**: `implementer`

**UDPS-032: Metadata CRUD Operations**
- **Description**: Implement metadata CRUD using Slick (Scala SQL library). Repository pattern with methods: createDatabase, getDatabase, listDatabases, updateDatabase, deleteDatabase (and similar for schemas, tables, columns). Transaction support (Slick transactions). Connection pooling (HikariCP). Type-safe queries (Slick Table definitions). Error handling (database constraint violations).
- **Acceptance Criteria**: All CRUD operations work; createTable inserts row in PostgreSQL; getTable retrieves correct row; updateTable updates row; deleteTable removes row (cascade to columns); transactions commit/rollback correctly; connection pool configured (min 5, max 20 connections); type-safe queries compile.
- **Files**: `udps-catalog/src/main/scala/io/gbmm/udps/catalog/repository/MetadataRepository.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/repository/Tables.scala` (Slick table definitions)
- **Risk**: low - Slick is mature
- **dispatch_hint**: `implementer`

**UDPS-033: Schema Discovery Engine**
- **Description**: Implement automatic schema discovery for external data sources. Scan source (JDBC metadata for databases, S3 file listing + Parquet schema, Kafka schema registry). Infer column types, nullable, cardinality. Create TableMetadata and ColumnMetadata. Store in catalog (UDPS-032). Parallel scanning (N sources in parallel using Akka). Progress tracking (percentage complete). Support incremental scan (only new tables/columns).
- **Acceptance Criteria**: Scan PostgreSQL database creates UDPS catalog entries for all tables/columns; scan S3 bucket discovers Parquet files and extracts schemas; Kafka topics scanned from schema registry; parallel scan of 10 sources completes faster than sequential; progress reported (0-100%); incremental scan only adds new tables.
- **Files**: `udps-catalog/src/main/scala/io/gbmm/udps/catalog/discovery/SchemaDiscovery.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/discovery/sources/JDBCScanner.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/discovery/sources/S3Scanner.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/discovery/sources/KafkaScanner.scala`
- **Risk**: medium - Different sources have different metadata APIs
- **dispatch_hint**: `implementer`

**UDPS-034: Incremental Schema Scanner**
- **Description**: Implement incremental scanning with change detection. Store scan checkpoints (last scan time, file hashes for S3, JDBC table modification times). On rescan, compare checkpoints to detect: new tables/columns, deleted tables/columns, schema changes (type change, nullable change). Emit change events (SchemaAdded, SchemaRemoved, SchemaModified). Update catalog with changes. Validate schema evolution (compatible changes only, e.g., add column OK, drop column requires manual confirmation).
- **Acceptance Criteria**: Initial scan stores checkpoint; incremental scan detects new table (SchemaAdded event); detects dropped column (SchemaRemoved event); detects type change (SchemaModified event); compatible changes applied automatically; incompatible changes flagged for review; checkpoint updated after scan.
- **Files**: `udps-catalog/src/main/scala/io/gbmm/udps/catalog/discovery/IncrementalScanner.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/discovery/ChangeDetector.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/discovery/SchemaEvolution.scala`
- **Risk**: medium - Change detection logic can be tricky
- **dispatch_hint**: `implementer`

**UDPS-035: Data Lineage Tracker**
- **Description**: Implement lineage tracking by parsing SQL queries. Extract lineage from SELECT statements: source tables/columns -> derived table/column. Store lineage edges in PostgreSQL (lineage_edges table). Support multi-hop lineage (A -> B -> C). Track query ID for lineage (link to query history). Column-level lineage (track which source columns contribute to each output column). Integrate with Calcite query parser.
- **Acceptance Criteria**: Parse `SELECT col1, col2 FROM table1` and extract lineage (table1.col1 -> output.col1); INSERT INTO ... SELECT extracts lineage (source -> target); JOIN extracts lineage from both tables; multi-hop lineage traversal works (query upstream/downstream); column-level lineage accurate; lineage edges stored in PostgreSQL.
- **Files**: `udps-catalog/src/main/scala/io/gbmm/udps/catalog/lineage/LineageTracker.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/lineage/LineageExtractor.scala` (Calcite integration)
- **Risk**: medium - SQL parsing for lineage is complex
- **dispatch_hint**: `implementer`

**UDPS-036: Lineage Graph Builder**
- **Description**: Implement lineage graph visualization. Build DAG from lineage edges (nodes = tables/columns, edges = dependencies). Traversal methods: upstream (find all sources), downstream (find all consumers), impact analysis (what breaks if this table changes). Export formats: GraphML, JSON, Mermaid diagram. Interactive visualization (future: integrate with UI, for now just export). Support filtering (e.g., only show lineage for specific column).
- **Acceptance Criteria**: Build DAG from lineage edges; upstream traversal returns all sources; downstream traversal returns all consumers; impact analysis identifies affected tables/columns; export to GraphML succeeds; export to Mermaid generates valid diagram; filtering by column works.
- **Files**: `udps-catalog/src/main/scala/io/gbmm/udps/catalog/lineage/LineageGraph.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/lineage/GraphExporter.scala`
- **Risk**: low - Graph traversal is straightforward
- **dispatch_hint**: `implementer`

**UDPS-037: Data Profiler**
- **Description**: Implement data profiling with comprehensive statistics. For numeric columns: min, max, mean, median, stddev, quartiles, histogram (10 buckets), null count, distinct count, outliers (IQR method). For string columns: min/max length, pattern analysis (regex common patterns), null count, distinct count, top 10 values. For date columns: min, max, range, null count. For boolean: true/false/null counts. Store results in PostgreSQL (profiles table, stats as JSON). Sampling for large tables (profile random 100K rows). Parallel profiling (multiple columns in parallel).
- **Acceptance Criteria**: Profile numeric column produces all statistics (min, max, mean, etc.); histogram has 10 buckets; profile string column finds patterns; profile date column produces range; sampling works for >1M row table (profile 100K sample); parallel profiling faster than sequential; results stored in PostgreSQL as JSON.
- **Files**: `udps-catalog/src/main/scala/io/gbmm/udps/catalog/profiling/DataProfiler.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/profiling/NumericProfiler.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/profiling/StringProfiler.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/profiling/DateProfiler.scala`
- **Risk**: medium - Statistical calculations and pattern detection
- **dispatch_hint**: `implementer`

**UDPS-038: Quality Rules Engine**
- **Description**: Implement data quality rules engine. Rule types: completeness (% non-null), validity (value in range, regex match), uniqueness (distinct count = row count), consistency (referential integrity across tables). Define rules in JSON (e.g., `{"column": "age", "rule": "range", "min": 0, "max": 120}`). Evaluate rules against profiled data or live data. Store violations in PostgreSQL (quality_violations table). Alerting (emit event if violation rate > threshold). Auto-remediation suggestions (e.g., "consider adding NOT NULL constraint").
- **Acceptance Criteria**: Completeness rule detects nulls in non-null column; validity rule detects out-of-range values; uniqueness rule detects duplicates; consistency rule detects orphan foreign keys; violations stored in PostgreSQL; alert emitted when >10% violations; suggestions generated for common issues.
- **Files**: `udps-catalog/src/main/scala/io/gbmm/udps/catalog/quality/QualityRulesEngine.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/quality/RuleEvaluator.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/quality/RuleDefinitions.scala`
- **Risk**: medium - Rule evaluation performance for large tables
- **dispatch_hint**: `implementer`

**UDPS-039: Data Sampling Engine**
- **Description**: Implement data sampling with 5 strategies: random (uniform probability), stratified (sample equally from each stratum, e.g., by category), systematic (every Nth row), cluster (sample entire partitions), time-based (sample rows from specific time ranges). Configurable sample size (% or absolute count). Reproducible (seed for random sampling). Export to Parquet, CSV, JSON. Store sample metadata in catalog (sample ID, strategy, size, seed, created_at).
- **Acceptance Criteria**: Random sampling produces uniform distribution; stratified sampling balanced across strata; systematic sampling evenly spaced; cluster sampling selects entire partitions; time-based sampling within time range; reproducible with same seed; export to Parquet/CSV/JSON works; metadata stored in PostgreSQL.
- **Files**: `udps-catalog/src/main/scala/io/gbmm/udps/catalog/sampling/SamplingEngine.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/sampling/Strategies.scala`
- **Risk**: low - Sampling algorithms well-defined
- **dispatch_hint**: `implementer`

**UDPS-040: Query Builder Service**
- **Description**: Implement visual query builder that generates SQL. Components: table selector (choose tables from catalog), column selector (choose columns, aggregations), filter builder (WHERE clause with operators =, !=, <, >, IN, LIKE), join builder (specify join conditions), GROUP BY / ORDER BY / LIMIT builders. Generate SQL from builder state. Validate query (syntax check via Calcite parser). Cost estimation (estimate rows scanned, execution time based on statistics). Export builder state as JSON (save/load query).
- **Acceptance Criteria**: Select table and columns generates valid SELECT; add filter generates WHERE clause; add join generates JOIN clause; generated SQL is valid (parses correctly); cost estimate shows rows scanned; export/import builder state as JSON preserves query; query executes successfully.
- **Files**: `udps-catalog/src/main/scala/io/gbmm/udps/catalog/querybuilder/QueryBuilder.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/querybuilder/SQLGenerator.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/querybuilder/CostEstimator.scala`
- **Risk**: low - SQL generation is straightforward
- **dispatch_hint**: `implementer`

**UDPS-041: Query History Tracker**
- **Description**: Implement query history tracking. Store every executed query in PostgreSQL (query_history table: id, sql_text, user_id, start_time, end_time, duration_ms, rows_returned, bytes_scanned, status [success/failure], error_message). Index on user_id, start_time for fast lookups. Analytics: top 10 slowest queries, most frequent queries, queries by user, queries by table. Retention policy (delete queries >90 days old). Query optimization recommendations (e.g., "add index on column X").
- **Acceptance Criteria**: Every query logged to PostgreSQL; analytics queries return correct results (top 10 slowest, etc.); retention policy deletes old queries; query by user returns user's history; optimization recommendations generated for slow queries (based on explain plan); index on user_id and start_time speeds up lookups.
- **Files**: `udps-catalog/src/main/scala/io/gbmm/udps/catalog/history/QueryHistoryTracker.scala`, `udps-catalog/src/main/scala/io/gbmm/udps/catalog/history/QueryAnalytics.scala`
- **Risk**: low - Simple database logging
- **dispatch_hint**: `implementer`

---

Due to length constraints, I'll continue the EPIC document in the next write. The document is comprehensive and follows the 4-phase planning pipeline. Let me save the first part and continue.
