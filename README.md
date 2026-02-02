# Unified Data Platform Service (UDPS)

UDPS is an enterprise-grade data platform built in Scala 2.13 that consolidates columnar storage, SQL query execution, metadata cataloging, and data governance into a single high-performance service. It is part of the SeRi-SA platform ecosystem and integrates with the Unified Security Platform (USP) for authentication/authorization and the Unified Coordination & Control Plane (UCCP) for service discovery.

## Key Features

- **Columnar Storage** -- Apache Arrow/Parquet with 8 compression codecs, 4-tier storage (Hot/Warm/Cold/Archive), ACID transactions, and 500 MB/sec ingestion per node
- **SQL Query Engine** -- Apache Calcite-based SQL:2016 parser and optimizer with distributed execution, vectorized processing, and adaptive query planning
- **Data Source Federation** -- Query across PostgreSQL, MySQL, MongoDB, Elasticsearch, Kafka, S3, and more with intelligent predicate/projection pushdown
- **Metadata Catalog** -- Automatic schema discovery, versioning, data lineage, profiling, and sampling at 5,000 ops/sec
- **Data Governance** -- PII classification (16+ patterns), anonymization (10+ techniques), GDPR compliance, column-level security, and audit trails
- **Advanced Indexing** -- Zone Maps, Bloom Filters, Bitmap Indexes, Skip Lists, and Lucene-powered full-text search for 10-1000x query acceleration
- **Materialized Views** -- Pre-computed results with on-demand, scheduled, incremental, and continuous refresh modes

## Architecture

UDPS is organized as a multi-module SBT project with 7 modules:

```
                         +-----------+
                         |   api     |
                         +-----+-----+
                        /   |     |    \
                       v    v     v     v
              +-------+ +-----+ +----------+ +-------------+
              | query | |cata-| |governance| | integration |
              |       | | log |  +----+-----+ +------+------+
              +---+---+ +--+--+       |              |
                  |        |          v              |
                  v        v     (catalog)           |
              +--------+ +--------+                  |
              |storage | |storage |                  |
              +---+----+ +---+----+                  |
                  |          |                       |
                  v          v                       v
              +--------------------------------------+
              |              core                    |
              +--------------------------------------+
```

| Module | Directory | Responsibility |
|--------|-----------|---------------|
| **core** | `udps-core/` | Domain models, configuration, shared abstractions |
| **storage** | `udps-storage/` | Arrow/Parquet I/O, indexing, tiered storage, MVCC, full-text search, materialized views |
| **query** | `udps-query/` | SQL parsing (Calcite), optimization, distributed execution, federation, caching |
| **catalog** | `udps-catalog/` | Metadata repository, schema discovery, lineage, profiling, sampling, query builder |
| **governance** | `udps-governance/` | PII classification, anonymization, GDPR compliance, audit, column-level security |
| **api** | `udps-api/` | gRPC server, HTTP/REST endpoints, metrics, health checks |
| **integration** | `udps-integration/` | Protobuf definitions for USP and UCCP integration |

## Prerequisites

| Requirement | Version |
|-------------|---------|
| JDK | 17 (Eclipse Temurin recommended) |
| SBT | 1.9.x |
| Scala | 2.13.12 (managed by SBT) |
| Docker | 24.x+ |
| Docker Compose | v2.x+ |

## Quick Start (Local Development)

1. **Clone and enter the project:**

   ```bash
   cd /home/tshepo/projects/seri-sa/udps
   ```

2. **Create the shared platform network** (one-time, if not already created by another SeRi-SA service):

   ```bash
   docker network create seri-sa-platform 2>/dev/null || true
   ```

3. **Configure environment variables:**

   ```bash
   cp .env.example .env
   # Edit .env and fill in all [REQUIRED] values:
   #   UDPS_DB_HOST, UDPS_DB_NAME, UDPS_DB_USER, UDPS_DB_PASSWORD
   #   UDPS_MINIO_ENDPOINT, UDPS_MINIO_ACCESS_KEY, UDPS_MINIO_SECRET_KEY
   #   UDPS_KAFKA_BOOTSTRAP_SERVERS, UDPS_REDIS_HOST, UDPS_TRACING_ENDPOINT
   #   POSTGRES_PASSWORD, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
   ```

4. **Start infrastructure dependencies:**

   ```bash
   docker compose -f docker-compose.dev.yml up -d
   ```

   This starts PostgreSQL 16, MinIO, Redis 7, Kafka (with ZooKeeper), and Jaeger.

5. **Compile and run:**

   ```bash
   sbt compile
   sbt "api/run"
   ```

   The service will start on:
   - gRPC: `localhost:50060`
   - HTTPS: `localhost:8443`
   - Health: `localhost:8081`
   - Metrics: `localhost:9090`

## Build Instructions

```bash
# Compile all modules
sbt compile

# Run all tests
sbt test

# Build the fat JAR (assembly)
sbt "api/assembly"
# Output: udps-api/target/scala-2.13/udps-api-assembly-0.1.0-SNAPSHOT.jar

# Run a specific module's tests
sbt "storage/test"
sbt "query/test"
```

## Docker Build

The project uses a multi-stage Dockerfile based on `eclipse-temurin:17-jre-alpine`:

```bash
# Build the Docker image
docker build -t udps:latest .

# Run the container
docker run -p 50060:50060 -p 8443:8443 -p 8081:8081 -p 9090:9090 \
  --env-file .env \
  -v ./.tls:/etc/udps/tls:ro \
  udps:latest
```

The image runs as a non-root user (`udps:1001`), uses `tini` as PID 1 for proper signal handling, and ships at approximately 500 MB.

## Configuration

All configuration is managed through environment variables that override defaults in `udps-core/src/main/resources/application.conf`. See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for the complete reference.

**Port assignments:**

| Port | Protocol | Purpose |
|------|----------|---------|
| 50060 | gRPC (TLS) | Primary API -- StorageService, QueryService, CatalogService, HealthService |
| 8443 | HTTPS | REST API and GraphQL endpoint |
| 9090 | HTTP | Prometheus metrics (`/metrics`) |
| 8081 | HTTP | Health checks (`/health`, `/health/live`, `/health/ready`) |

## Deployment

UDPS supports three deployment modes:

- **Local development** -- `docker-compose.dev.yml` with infrastructure services only; run the application from SBT
- **Production Docker Compose** -- `docker-compose.prod.yml` with the full stack including the application container, TLS, resource limits, and security hardening
- **Kubernetes** -- Manifests in `k8s/` with Deployment, Service, HPA, PDB, Ingress, ConfigMap, and Secrets

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for detailed instructions.

## Project Structure

```
udps/
├── build.sbt                      # Multi-module SBT build definition
├── project/
│   ├── Dependencies.scala         # Centralized dependency management
│   ├── build.properties           # SBT version
│   └── plugins.sbt                # SBT plugins (ScalaPB, assembly)
├── udps-core/                     # Domain models and configuration
│   └── src/main/
│       ├── scala/io/gbmm/udps/core/
│       │   ├── config/UdpsConfig.scala
│       │   └── domain/            # DataType, SchemaInfo, StorageTier, etc.
│       └── resources/application.conf
├── udps-storage/                  # Storage engine
│   └── src/main/scala/io/gbmm/udps/storage/
│       ├── arrow/                 # SchemaAdapter
│       ├── batch/                 # RecordBatchProcessor, Transformations
│       ├── fts/                   # LuceneIndexer, LuceneSearcher, Analyzers
│       ├── indexing/              # ZoneMap, BloomFilter, Bitmap, SkipList
│       ├── mvcc/                  # SnapshotManager, SnapshotGC
│       ├── parquet/               # ParquetReader, ParquetWriter, PredicatePushdown
│       ├── tiering/               # TierManager, TierPolicy, MinIOClient
│       ├── txn/                   # TransactionCoordinator, LockManager
│       └── views/                 # MaterializedViewEngine, ViewRefresher
├── udps-query/                    # Query engine
│   └── src/main/scala/io/gbmm/udps/query/
│       ├── adaptive/              # AdaptiveExecutor, ReOptimizer
│       ├── cache/                 # QueryCache, CacheInvalidator
│       ├── calcite/               # UDPSSchema, UDPSTable, TypeMapper
│       ├── execution/             # DistributedExecutor, QueryCoordinator
│       ├── federation/            # FederationAdapter + 7 adapters
│       ├── optimizer/             # CostBasedOptimizer, LogicalOptimizer
│       ├── parser/                # SQLParser
│       ├── physical/              # PhysicalPlanner, ExecutionDAG, Operators
│       └── vectorized/            # VectorizedFilter, Aggregate, Project
├── udps-catalog/                  # Metadata catalog
│   └── src/main/scala/io/gbmm/udps/catalog/
│       ├── discovery/             # SchemaDiscovery, ChangeDetector, sources/
│       ├── history/               # QueryHistoryTracker, QueryAnalytics
│       ├── lineage/               # LineageTracker, LineageGraph, GraphExporter
│       ├── profiling/             # DataProfiler, NumericProfiler, StringProfiler
│       ├── quality/               # QualityRulesEngine, RuleEvaluator
│       ├── querybuilder/          # QueryBuilder, SQLGenerator, CostEstimator
│       ├── repository/            # MetadataRepository, Tables
│       └── sampling/              # SamplingEngine, Strategies
├── udps-governance/               # Data governance
│   └── src/main/scala/io/gbmm/udps/governance/
│       ├── access/                # AccessRequestWorkflow, WorkflowEngine
│       ├── anonymization/         # Anonymizer, Techniques
│       ├── audit/                 # AuditLogger, AuditSearch
│       ├── classification/        # PIIClassifier, MLClassifier, Patterns
│       ├── gdpr/                  # GDPRCompliance, ConsentManager, RetentionPolicy
│       └── security/              # ColumnSecurity, PolicyEnforcer
├── udps-api/                      # API layer (entrypoint)
│   └── src/main/scala/io/gbmm/udps/api/
├── udps-integration/              # External service integration
│   └── src/main/protobuf/
│       ├── usp/                   # USP proto definitions
│       │   ├── outpost.proto
│       │   ├── authentication.proto
│       │   ├── authorization.proto
│       │   ├── secrets.proto
│       │   ├── session.proto
│       │   └── common.proto
│       └── uccp/                  # UCCP proto definitions
│           ├── coordination.proto
│           └── common.proto
├── docker-compose.dev.yml         # Development infrastructure
├── docker-compose.prod.yml        # Production full stack
├── Dockerfile                     # Multi-stage build
├── k8s/                           # Kubernetes manifests
│   ├── namespace.yml
│   ├── configmap.yml
│   ├── secret.yml
│   ├── deployment.yml
│   ├── service.yml
│   ├── hpa.yml
│   ├── pdb.yml
│   └── ingress.yml
├── .env.example                   # Environment variable template
└── docs/
    ├── udps.md                    # Full specification
    ├── ARCHITECTURE.md            # Architecture guide
    ├── DEPLOYMENT.md              # Deployment guide
    ├── CONFIGURATION.md           # Configuration reference
    ├── API.md                     # API reference
    ├── INTEGRATION.md             # USP/UCCP/USTRP integration guide
    └── TROUBLESHOOTING.md         # Troubleshooting guide
```

## Contributing

1. Create a feature branch from `main`.
2. Follow existing code patterns -- Cats Effect `IO` for effects, `Resource` for lifecycle management, `FS2` streams for data pipelines.
3. Run `sbt compile` and `sbt test` before submitting a pull request.
4. Ensure all warnings are resolved (`-Xfatal-warnings` is enabled).
5. Update documentation if adding new modules, configuration, or API endpoints.

## License

Proprietary -- GBMM / Seri-SA. All rights reserved.
