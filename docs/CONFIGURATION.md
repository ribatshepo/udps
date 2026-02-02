# UDPS Configuration Reference

All configuration is defined in `udps-core/src/main/resources/application.conf` using Typesafe Config (HOCON format). Every setting can be overridden by an environment variable. The typed configuration is loaded into `io.gbmm.udps.core.config.UdpsConfig` via PureConfig at application startup.

## Environment Variables

### PostgreSQL -- Metadata Catalog

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_DB_HOST` | Yes | -- | PostgreSQL hostname |
| `UDPS_DB_PORT` | No | `5432` | PostgreSQL port |
| `UDPS_DB_NAME` | Yes | -- | Database name (e.g., `udps_catalog`) |
| `UDPS_DB_USER` | Yes | -- | Database user |
| `UDPS_DB_PASSWORD` | Yes | -- | Database password |
| `UDPS_DB_MAX_POOL_SIZE` | No | `20` | HikariCP connection pool size |

Config path: `udps.database`

### MinIO -- S3-Compatible Object Storage (Archive Tier)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_MINIO_ENDPOINT` | Yes | -- | MinIO endpoint URL (e.g., `http://minio:9000`) |
| `UDPS_MINIO_ACCESS_KEY` | Yes | -- | MinIO access key |
| `UDPS_MINIO_SECRET_KEY` | Yes | -- | MinIO secret key |
| `UDPS_MINIO_BUCKET` | No | `udps-archive` | Bucket name for archive tier storage |

Config path: `udps.minio`

### Kafka -- Event Streaming

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_KAFKA_BOOTSTRAP_SERVERS` | Yes | -- | Comma-separated broker addresses (e.g., `kafka:29092`) |
| `UDPS_KAFKA_GROUP_ID` | No | `udps-consumer-group` | Kafka consumer group ID |

Config path: `udps.kafka`

The `auto-offset-reset` is hardcoded to `earliest` in `application.conf`.

### Redis -- Distributed Cache

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_REDIS_HOST` | Yes | -- | Redis hostname |
| `UDPS_REDIS_PORT` | No | `6379` | Redis port |

Config path: `udps.redis`

### OpenTelemetry / Jaeger -- Distributed Tracing

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_TRACING_ENDPOINT` | Yes | -- | OTLP collector gRPC endpoint (e.g., `http://jaeger:4317`) |

Config path: `udps.tracing`

The tracing service name is hardcoded to `udps`.

### TLS Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_TLS_ENABLED` | No | `true` | Enable TLS on gRPC and HTTPS servers. Set to `false` for local development only. |
| `UDPS_TLS_CERT_PATH` | No | `/etc/udps/tls/tls.crt` | Path to the server TLS certificate (PEM format) |
| `UDPS_TLS_KEY_PATH` | No | `/etc/udps/tls/tls.key` | Path to the server TLS private key (PEM format) |
| `UDPS_TLS_CA_PATH` | No | `/etc/udps/tls/ca.crt` | Path to the CA certificate for client verification |

Config path: `udps.tls`

### gRPC Server

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_GRPC_HOST` | No | `0.0.0.0` | gRPC bind address |
| `UDPS_GRPC_PORT` | No | `50060` | gRPC listen port |

Config path: `udps.grpc`

The maximum gRPC message size is 100 MB (`max-message-size = 104857600`), configured in `application.conf` and not overridable via environment variable.

### HTTPS Server

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_HTTPS_HOST` | No | `0.0.0.0` | HTTPS bind address |
| `UDPS_HTTPS_PORT` | No | `8443` | HTTPS listen port |

Config path: `udps.https`

### Metrics Server

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_METRICS_HOST` | No | `0.0.0.0` | Metrics server bind address |
| `UDPS_METRICS_PORT` | No | `9090` | Prometheus metrics port |

Config path: `udps.metrics`

### Health Check Server

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_HEALTH_HOST` | No | `0.0.0.0` | Health check server bind address |
| `UDPS_HEALTH_PORT` | No | `8081` | Health check port |

Config path: `udps.health`

### Storage Tier Paths

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_STORAGE_HOT_PATH` | No | `/data/udps/hot` | Hot tier storage directory (NVMe/local SSD recommended) |
| `UDPS_STORAGE_WARM_PATH` | No | `/data/udps/warm` | Warm tier storage directory (network SSD recommended) |
| `UDPS_STORAGE_COLD_PATH` | No | `/data/udps/cold` | Cold tier storage directory (HDD acceptable) |
| `UDPS_DEFAULT_COMPRESSION` | No | `zstd` | Default Parquet compression codec. Options: `lz4`, `zstd`, `snappy`, `gzip`, `brotli`, `lzo`, `uncompressed` |
| `UDPS_STORAGE_WORKER_THREADS` | No | `8` | Number of storage I/O worker threads |

Config path: `udps.storage`

The default Parquet row group size is 128 MB (`default-row-group-size = 134217728`), configured in `application.conf`.

### Query Engine

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_MAX_CONCURRENT_QUERIES` | No | `100` | Maximum number of queries executing simultaneously |
| `UDPS_QUERY_CACHE_SIZE` | No | `10000` | Maximum number of cached query results in Redis |

Config path: `udps.query`

The default query timeout is 30 seconds (`default-timeout = 30s`), configured in `application.conf`.

### Docker Compose Infrastructure Variables

These variables are used by the Docker Compose files for the infrastructure containers, not by the UDPS application itself:

| Variable | Used by | Description |
|----------|---------|-------------|
| `POSTGRES_PASSWORD` | postgres container | PostgreSQL superuser password |
| `POSTGRES_DB` | postgres container | Database to create on first start |
| `POSTGRES_USER` | postgres container | Database user to create |
| `MINIO_ROOT_USER` | minio container | MinIO admin user |
| `MINIO_ROOT_PASSWORD` | minio container | MinIO admin password |
| `REDIS_PASSWORD` | redis container (prod) | Redis authentication password |
| `UDPS_VERSION` | udps container (prod) | Docker image tag |
| `JVM_HEAP_MIN` | udps container (prod) | JVM minimum heap (default: `512m`) |
| `JVM_HEAP_MAX` | udps container (prod) | JVM maximum heap (default: `2g`) |
| `LOG_LEVEL` | udps container (prod) | Application log level (default: `INFO`) |

## Port Assignments

| Port | Protocol | Server | Description |
|------|----------|--------|-------------|
| 50060 | gRPC | UDPS | Primary gRPC API (TLS when `UDPS_TLS_ENABLED=true`) |
| 8443 | HTTPS | UDPS | REST API and GraphQL endpoint |
| 9090 | HTTP | UDPS | Prometheus metrics |
| 8081 | HTTP | UDPS | Health check endpoints |
| 5432 | TCP | PostgreSQL | Metadata catalog database |
| 9000 | HTTP/S | MinIO | S3-compatible object storage API |
| 9001 | HTTP/S | MinIO | MinIO web console |
| 6379 | TCP | Redis | Distributed cache |
| 9092 | TCP | Kafka | Kafka broker (external listener) |
| 29092 | TCP | Kafka | Kafka broker (internal listener, Docker only) |
| 2181 | TCP | ZooKeeper | ZooKeeper client port |
| 16686 | HTTP | Jaeger | Jaeger tracing UI |
| 4317 | gRPC | Jaeger | OTLP gRPC collector |
| 4318 | HTTP | Jaeger | OTLP HTTP collector |

## application.conf Structure

The full configuration tree at `udps-core/src/main/resources/application.conf`:

```hocon
udps {
  service {
    name = "udps"
    version = "1.0.0"
  }

  grpc {
    host = "0.0.0.0"       # overridden by UDPS_GRPC_HOST
    port = 50060            # overridden by UDPS_GRPC_PORT
    max-message-size = 104857600  # 100 MB, not overridable
  }

  https {
    host = "0.0.0.0"       # overridden by UDPS_HTTPS_HOST
    port = 8443             # overridden by UDPS_HTTPS_PORT
  }

  metrics {
    host = "0.0.0.0"       # overridden by UDPS_METRICS_HOST
    port = 9090             # overridden by UDPS_METRICS_PORT
  }

  health {
    host = "0.0.0.0"       # overridden by UDPS_HEALTH_HOST
    port = 8081             # overridden by UDPS_HEALTH_PORT
  }

  database {
    host = ${UDPS_DB_HOST}          # REQUIRED
    port = 5432                     # overridden by UDPS_DB_PORT
    name = ${UDPS_DB_NAME}          # REQUIRED
    user = ${UDPS_DB_USER}          # REQUIRED
    password = ${UDPS_DB_PASSWORD}  # REQUIRED
    max-pool-size = 20              # overridden by UDPS_DB_MAX_POOL_SIZE
  }

  minio {
    endpoint = ${UDPS_MINIO_ENDPOINT}      # REQUIRED
    access-key = ${UDPS_MINIO_ACCESS_KEY}  # REQUIRED
    secret-key = ${UDPS_MINIO_SECRET_KEY}  # REQUIRED
    bucket = "udps-archive"                # overridden by UDPS_MINIO_BUCKET
  }

  kafka {
    bootstrap-servers = ${UDPS_KAFKA_BOOTSTRAP_SERVERS}  # REQUIRED
    group-id = "udps-consumer-group"  # overridden by UDPS_KAFKA_GROUP_ID
    auto-offset-reset = "earliest"
  }

  redis {
    host = ${UDPS_REDIS_HOST}  # REQUIRED
    port = 6379                # overridden by UDPS_REDIS_PORT
  }

  tls {
    enabled = true                         # overridden by UDPS_TLS_ENABLED
    cert-path = "/etc/udps/tls/tls.crt"    # overridden by UDPS_TLS_CERT_PATH
    key-path = "/etc/udps/tls/tls.key"     # overridden by UDPS_TLS_KEY_PATH
    ca-path = "/etc/udps/tls/ca.crt"       # overridden by UDPS_TLS_CA_PATH
  }

  storage {
    hot-path = "/data/udps/hot"     # overridden by UDPS_STORAGE_HOT_PATH
    warm-path = "/data/udps/warm"   # overridden by UDPS_STORAGE_WARM_PATH
    cold-path = "/data/udps/cold"   # overridden by UDPS_STORAGE_COLD_PATH
    default-compression = "zstd"    # overridden by UDPS_DEFAULT_COMPRESSION
    default-row-group-size = 134217728  # 128 MB, not overridable
    worker-threads = 8              # overridden by UDPS_STORAGE_WORKER_THREADS
  }

  query {
    max-concurrent-queries = 100  # overridden by UDPS_MAX_CONCURRENT_QUERIES
    default-timeout = 30s
    query-cache-size = 10000      # overridden by UDPS_QUERY_CACHE_SIZE
  }

  tracing {
    endpoint = ${UDPS_TRACING_ENDPOINT}  # REQUIRED
    service-name = "udps"
  }
}
```

## Storage Tier Configuration

UDPS manages four storage tiers. Data is automatically promoted or demoted between tiers by `io.gbmm.udps.storage.tiering.TierManager` based on policies defined in `io.gbmm.udps.storage.tiering.TierPolicy`.

| Tier | Path variable | Recommended media | Typical use |
|------|--------------|-------------------|------------|
| Hot | `UDPS_STORAGE_HOT_PATH` | NVMe / local SSD | Active data, recent ingestion, frequently queried |
| Warm | `UDPS_STORAGE_WARM_PATH` | Network SSD | Data accessed occasionally, recent history |
| Cold | `UDPS_STORAGE_COLD_PATH` | HDD / network HDD | Infrequently accessed data, compliance retention |
| Archive | `UDPS_MINIO_ENDPOINT` + `UDPS_MINIO_BUCKET` | S3 / MinIO | Long-term archival, rarely accessed |

### Compression codecs

The `UDPS_DEFAULT_COMPRESSION` variable controls the default Parquet compression. Per-table overrides are possible via the gRPC API.

| Codec | Ratio | Speed | Best for |
|-------|-------|-------|----------|
| `zstd` (default) | High | Medium | General purpose, best overall tradeoff |
| `lz4` | Medium | Very fast | Low-latency ingestion |
| `snappy` | Medium | Fast | Balanced workloads |
| `gzip` | Very high | Slow | Archive tier, maximize compression |
| `brotli` | Very high | Slow | Archive tier, text-heavy data |
| `lzo` | Medium | Fast | Legacy compatibility |
| `uncompressed` | None | Fastest | When CPU is the bottleneck |

## Security Settings

### TLS

TLS is enabled by default (`UDPS_TLS_ENABLED=true`). When enabled:

- The gRPC server on port 50060 requires TLS
- The HTTPS server on port 8443 uses TLS
- Certificate, key, and CA paths must be valid and readable

Disable TLS only for local development by setting `UDPS_TLS_ENABLED=false`.

### Authentication

Authentication is delegated to USP via the `AuthenticationService` gRPC client (defined in `udps-integration/src/main/protobuf/usp/authentication.proto`). Every API request includes a JWT token that is validated against USP.

### Authorization

Authorization is delegated to USP via the `AuthorizationService` gRPC client (defined in `udps-integration/src/main/protobuf/usp/authorization.proto`). Permission checks are performed before data access operations.

## Performance Tuning

### JVM tuning

The Dockerfile sets these JVM defaults via the `JAVA_OPTS` environment variable:

```
-XX:+UseContainerSupport
-XX:MaxRAMPercentage=75.0
-XX:InitialRAMPercentage=50.0
-XX:+UseG1GC
-XX:+HeapDumpOnOutOfMemoryError
```

For production Docker Compose, the `JAVA_OPTS` in `docker-compose.prod.yml` uses:

```
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200
-XX:+UseStringDeduplication
-XX:+OptimizeStringConcat
-Xms${JVM_HEAP_MIN:-512m}
-Xmx${JVM_HEAP_MAX:-2g}
```

### Key tuning parameters

| Parameter | Variable | Impact |
|-----------|----------|--------|
| Worker threads | `UDPS_STORAGE_WORKER_THREADS` | Controls parallelism for Parquet reads/writes. Set to CPU core count for I/O-bound workloads. |
| Concurrent queries | `UDPS_MAX_CONCURRENT_QUERIES` | Limits active queries to prevent memory exhaustion. Increase with available heap. |
| Query cache size | `UDPS_QUERY_CACHE_SIZE` | Number of cached results in Redis. Larger cache improves hit rate but uses more Redis memory. |
| Connection pool | `UDPS_DB_MAX_POOL_SIZE` | PostgreSQL connection pool. Increase if metadata operations are bottlenecked on connections. |
| Row group size | `default-row-group-size` (application.conf) | Parquet row group size. Larger groups improve scan performance; smaller groups reduce memory during writes. Default 128 MB. |
| Query timeout | `default-timeout` (application.conf) | Maximum query execution time. Default 30 seconds. |
