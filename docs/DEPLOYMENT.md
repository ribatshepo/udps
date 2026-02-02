# UDPS Deployment Guide

## Prerequisites

| Component | Required version | Notes |
|-----------|-----------------|-------|
| Docker | 24.x+ | Docker Engine or Docker Desktop |
| Docker Compose | v2.x+ | Compose plugin (`docker compose`) |
| JDK 17 | Eclipse Temurin 17 | Only for local development (SBT) |
| SBT | 1.9.x | Only for local development |
| kubectl | 1.28+ | Only for Kubernetes deployment |

## 1. Local Development

The local development setup runs infrastructure services in Docker while the UDPS application runs from SBT for fast iteration.

### 1.1 Create the shared network

The SeRi-SA platform uses a shared Docker network so that services can discover each other:

```bash
docker network create seri-sa-platform 2>/dev/null || true
```

### 1.2 Configure environment

```bash
cp .env.example .env
```

For local development, set these values in `.env`:

```bash
# PostgreSQL
UDPS_DB_HOST=localhost
UDPS_DB_PORT=5432
UDPS_DB_NAME=udps_catalog
UDPS_DB_USER=udps
UDPS_DB_PASSWORD=<choose-a-password>
POSTGRES_PASSWORD=<same-password>

# MinIO
UDPS_MINIO_ENDPOINT=http://localhost:9000
UDPS_MINIO_ACCESS_KEY=minioadmin
UDPS_MINIO_SECRET_KEY=<choose-a-password>
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=<same-password>

# Kafka
UDPS_KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Redis
UDPS_REDIS_HOST=localhost

# Tracing
UDPS_TRACING_ENDPOINT=http://localhost:4317

# Disable TLS for local dev (traffic stays on localhost)
UDPS_TLS_ENABLED=false
```

### 1.3 Start infrastructure

```bash
docker compose -f docker-compose.dev.yml up -d
```

This starts:

| Service | Container | Ports |
|---------|-----------|-------|
| PostgreSQL 16 | `udps-postgres` | 5432 |
| MinIO | `udps-minio` | 9000 (API), 9001 (Console) |
| Redis 7 | `udps-redis` | 6379 |
| ZooKeeper | `udps-zookeeper` | 2181 |
| Kafka | `udps-kafka` | 9092 |
| Jaeger | `udps-jaeger` | 16686 (UI), 4317 (OTLP gRPC), 4318 (OTLP HTTP) |

### 1.4 Verify infrastructure health

```bash
docker compose -f docker-compose.dev.yml ps
```

All services should show `healthy` status. PostgreSQL takes approximately 30 seconds, Kafka approximately 45 seconds.

### 1.5 Run the application

```bash
sbt "api/run"
```

### 1.6 Verify the application

```bash
# Health check
curl http://localhost:8081/health

# Liveness probe
curl http://localhost:8081/health/live

# Readiness probe
curl http://localhost:8081/health/ready

# Prometheus metrics
curl http://localhost:9090/metrics
```

### 1.7 Stop infrastructure

```bash
docker compose -f docker-compose.dev.yml down
# To also remove volumes:
docker compose -f docker-compose.dev.yml down -v
```

## 2. Production Docker Compose

The production compose file (`docker-compose.prod.yml`) runs the full stack including the UDPS application container with TLS, resource limits, security hardening, and persistent volumes.

### 2.1 Build the application image

```bash
docker build -t ghcr.io/gbmm/udps:0.1.0 .
```

### 2.2 Configure environment

Create a `.env` file with production values. All secrets should come from a secrets manager in a real deployment; the `.env` file is for initial bootstrapping only.

Required variables:

```bash
# Application version
UDPS_VERSION=0.1.0

# PostgreSQL
POSTGRES_DB=udps_catalog
POSTGRES_USER=udps
POSTGRES_PASSWORD=<strong-password>

# MinIO
MINIO_ROOT_USER=<access-key>
MINIO_ROOT_PASSWORD=<strong-password>

# Redis
REDIS_PASSWORD=<strong-password>

# JVM
JVM_HEAP_MIN=512m
JVM_HEAP_MAX=2g
LOG_LEVEL=INFO
```

### 2.3 TLS certificates

The production stack requires TLS certificates. Place them in the directory structure:

```
.tls/
├── tls.crt          # UDPS server certificate
├── tls.key          # UDPS server private key
├── ca.crt           # CA certificate
├── postgres/
│   ├── tls.crt      # PostgreSQL server certificate
│   └── tls.key      # PostgreSQL server private key
├── redis/
│   ├── tls.crt      # Redis server certificate
│   ├── tls.key      # Redis server private key
│   └── ca.crt       # Redis CA certificate
├── minio/
│   ├── public.crt   # MinIO server certificate
│   └── private.key  # MinIO server private key
└── kafka/
    ├── kafka.keystore.jks
    ├── kafka.truststore.jks
    ├── keystore-creds
    ├── key-creds
    └── truststore-creds
```

To generate self-signed certificates for testing:

```bash
mkdir -p .tls/postgres .tls/redis .tls/minio .tls/kafka

# Generate CA
openssl genrsa -out .tls/ca.key 4096
openssl req -x509 -new -nodes -key .tls/ca.key -sha256 -days 365 \
  -out .tls/ca.crt -subj "/CN=UDPS CA"

# Generate server cert
openssl genrsa -out .tls/tls.key 2048
openssl req -new -key .tls/tls.key -out .tls/tls.csr -subj "/CN=udps"
openssl x509 -req -in .tls/tls.csr -CA .tls/ca.crt -CAkey .tls/ca.key \
  -CAcreateserial -out .tls/tls.crt -days 365 -sha256
```

Repeat for each service. In production, use a proper PKI or cert-manager.

### 2.4 Start the stack

```bash
docker compose -f docker-compose.prod.yml up -d
```

### 2.5 Production resource limits

The `docker-compose.prod.yml` defines these resource constraints:

| Service | CPU limit | Memory limit | CPU reservation | Memory reservation |
|---------|-----------|-------------|-----------------|-------------------|
| udps | 2.0 | 4 GB | 0.5 | 1 GB |
| postgres | 2.0 | 2 GB | 0.25 | 512 MB |
| minio | 1.0 | 2 GB | 0.25 | 512 MB |
| redis | 0.5 | 1 GB | 0.1 | 128 MB |
| zookeeper | 0.5 | 512 MB | 0.1 | 128 MB |
| kafka | 2.0 | 2 GB | 0.5 | 1 GB |
| jaeger | 1.0 | 1 GB | 0.1 | 256 MB |

### 2.6 Security features in production compose

- `read_only: true` -- Container filesystem is read-only; only `/tmp` and `/var/log/udps` are writable via tmpfs
- `no-new-privileges: true` -- Prevents privilege escalation
- `--requirepass` on Redis -- Authentication required
- `--auth-host=scram-sha-256` on PostgreSQL -- Strong password authentication
- SSL enabled on Kafka and PostgreSQL
- Dangerous Redis commands (`FLUSHALL`, `FLUSHDB`, `DEBUG`) are disabled
- JSON file logging with rotation (50 MB max, 5 files)

## 3. Kubernetes Deployment

Kubernetes manifests are in the `k8s/` directory.

### 3.1 Manifest inventory

| File | Kind | Purpose |
|------|------|---------|
| `namespace.yml` | Namespace | `udps` namespace |
| `configmap.yml` | ConfigMap | Non-sensitive configuration |
| `secret.yml` | Secret | Credentials template (populate before applying) |
| `deployment.yml` | Deployment + ServiceAccount | Application pods |
| `service.yml` | Service (x2) | ClusterIP services for gRPC/HTTPS and health |
| `hpa.yml` | HorizontalPodAutoscaler | Auto-scaling (2-10 replicas) |
| `pdb.yml` | PodDisruptionBudget | Minimum 1 pod available during disruptions |
| `ingress.yml` | Ingress (x2) | HTTPS and gRPC ingress via nginx |

### 3.2 Apply manifests

```bash
# Create namespace
kubectl apply -f k8s/namespace.yml

# Create TLS secret (from cert-manager or manual)
kubectl create secret tls udps-tls \
  --cert=tls.crt --key=tls.key -n udps

# Edit secret.yml and replace all REPLACE_WITH_ACTUAL_VALUE entries
kubectl apply -f k8s/secret.yml

# Apply remaining resources
kubectl apply -f k8s/configmap.yml
kubectl apply -f k8s/deployment.yml
kubectl apply -f k8s/service.yml
kubectl apply -f k8s/hpa.yml
kubectl apply -f k8s/pdb.yml
kubectl apply -f k8s/ingress.yml
```

### 3.3 Deployment configuration

The Deployment is configured with:

- **Replicas:** 2 (minimum; HPA scales to 10)
- **Strategy:** RollingUpdate with `maxSurge: 1`, `maxUnavailable: 0`
- **Pod anti-affinity:** Prefers scheduling on different nodes
- **Init container:** Waits for PostgreSQL to be reachable before starting
- **Security context:**
  - `runAsNonRoot: true`
  - `runAsUser: 1000`
  - `readOnlyRootFilesystem: true`
  - All capabilities dropped
  - `seccompProfile: RuntimeDefault`
- **Volumes:**
  - TLS certificates from `udps-tls` secret
  - EmptyDir for `/tmp` (256 Mi) and `/var/log/udps` (512 Mi)

### 3.4 Health probes

| Probe | Endpoint | Initial delay | Period | Failure threshold |
|-------|----------|--------------|--------|-------------------|
| Startup | `GET /health/live` (port 8081) | 15s | 5s | 30 (= 2.5 min total) |
| Liveness | `GET /health/live` (port 8081) | 0s | 15s | 3 |
| Readiness | `GET /health/ready` (port 8081) | 0s | 10s | 3 |

### 3.5 Services

Two ClusterIP services are created:

| Service | Ports | Purpose |
|---------|-------|---------|
| `udps` | 50060 (gRPC), 8443 (HTTPS), 9090 (metrics) | Primary service |
| `udps-health` | 8081 | Health check service (for internal probes) |

### 3.6 Ingress

Two Ingress resources are configured for nginx ingress controller:

| Ingress | Host | Backend | Protocol |
|---------|------|---------|----------|
| `udps` | `udps.seri-sa.io` | `udps:8443` | HTTPS |
| `udps-grpc` | `grpc.udps.seri-sa.io` | `udps:50060` | GRPCS |

Security headers are set via nginx annotations: HSTS, X-Content-Type-Options, X-Frame-Options, X-XSS-Protection, Referrer-Policy.

### 3.7 Auto-scaling

The HPA is configured as:

| Metric | Target | Min replicas | Max replicas |
|--------|--------|-------------|-------------|
| CPU utilization | 70% | 2 | 10 |
| Memory utilization | 80% | 2 | 10 |

Scale-up: 2 pods per 60 seconds, 60-second stabilization window.
Scale-down: 1 pod per 120 seconds, 300-second stabilization window.

### 3.8 Pod disruption budget

Minimum 1 pod must remain available during voluntary disruptions (node drain, rolling upgrades).

## 4. Health Checks and Monitoring

### Health endpoints (port 8081)

| Endpoint | Purpose | Response |
|----------|---------|----------|
| `GET /health` | Overall health | 200 if healthy, 503 if not |
| `GET /health/live` | Liveness (is the process alive?) | 200 if alive |
| `GET /health/ready` | Readiness (can accept traffic?) | 200 if ready (database, Redis, Kafka connected) |

### Metrics (port 9090)

Prometheus metrics are exposed at `GET /metrics` on port 9090. Kubernetes pods are annotated for automatic Prometheus scraping:

```yaml
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "9090"
  prometheus.io/path: "/metrics"
```

Key metric families:

| Metric prefix | Description |
|---------------|-------------|
| `udps_ingest_*` | Ingestion throughput, latency, bytes |
| `udps_query_*` | Query count, duration, active queries, cache hits |
| `udps_catalog_*` | Scan count, table count, lineage nodes |
| `udps_jvm_*` | JVM memory, threads, GC |
| `udps_http_*` | HTTP request count and latency |
| `udps_grpc_*` | gRPC request count and latency |

### Tracing

OpenTelemetry traces are exported to Jaeger via OTLP gRPC (port 4317). Configure the collector endpoint via:

- Docker Compose: `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317`
- Kubernetes: `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.observability.svc.cluster.local:4317`

Jaeger UI is available at port 16686.

## 5. Scaling Guidelines

### Vertical scaling

| JVM parameter | Default | Recommended for heavy workloads |
|---------------|---------|-------------------------------|
| `JVM_HEAP_MIN` | 512m | 2g |
| `JVM_HEAP_MAX` | 2g | 8g |
| `UDPS_STORAGE_WORKER_THREADS` | 8 | Match CPU core count |
| `UDPS_MAX_CONCURRENT_QUERIES` | 100 | 200-500 depending on memory |
| `UDPS_QUERY_CACHE_SIZE` | 10000 | 50000+ with adequate Redis memory |

### Horizontal scaling

- Scale the `udps` Deployment replicas. Each replica is stateless for query processing.
- Storage tier paths should use shared storage (network SSD, NFS, or S3) when running multiple replicas.
- Redis provides distributed query cache consistency across replicas.
- Kafka consumer group automatically rebalances partitions across replicas.

### Infrastructure scaling

| Service | When to scale |
|---------|--------------|
| PostgreSQL | Catalog metadata exceeds 100k tables or metadata write throughput exceeds capacity |
| Redis | Query cache hit rate drops; increase `maxmemory` or add replicas |
| Kafka | Ingestion throughput exceeds single broker capacity; add brokers and partitions |
| MinIO | Archive tier storage exceeds single node capacity; switch to distributed mode |

## 6. Troubleshooting

### Application does not start

1. **Check logs:** `docker logs udps-app` or `kubectl logs -n udps deployment/udps`
2. **Missing environment variables:** The application fails fast if required variables (`UDPS_DB_HOST`, `UDPS_MINIO_ENDPOINT`, etc.) are not set. Check the error message for the specific missing variable.
3. **Database not reachable:** Verify PostgreSQL is healthy: `docker exec udps-postgres pg_isready -U udps -d udps_catalog`
4. **TLS certificate errors:** Verify certificate paths are correct and files are readable by the container user (UID 1001 in Docker, UID 1000 in Kubernetes).

### Health check failures

| Symptom | Likely cause | Resolution |
|---------|-------------|-----------|
| `/health/live` returns 503 | JVM is deadlocked or out of memory | Check JVM heap usage; increase `JVM_HEAP_MAX` |
| `/health/ready` returns 503 | Database, Redis, or Kafka connection failed | Check infrastructure service health; verify network connectivity |
| Startup probe fails | Application takes too long to initialize | Increase `failureThreshold` on the startup probe |

### Performance issues

| Symptom | Diagnosis | Resolution |
|---------|----------|-----------|
| Slow queries | Check `udps_query_duration_seconds` metrics; run `EXPLAIN` via gRPC | Add indexes; increase `UDPS_QUERY_CACHE_SIZE`; check `UDPS_MAX_CONCURRENT_QUERIES` |
| High memory usage | Check `udps_jvm_memory_used_bytes`; look for large result sets | Increase heap; add `LIMIT` to queries; enable query result pagination |
| Slow ingestion | Check `udps_ingest_bytes` throughput; look for storage I/O bottleneck | Increase `UDPS_STORAGE_WORKER_THREADS`; use faster storage tier; check compression codec |
| Redis connection errors | Check Redis health and connection pool | Increase Redis `maxmemory`; check `UDPS_REDIS_HOST` and `UDPS_REDIS_PORT` |

### Container-specific issues

| Issue | Resolution |
|-------|-----------|
| `read-only file system` errors | Application is writing outside `/tmp` and `/var/log/udps`; check application logs for the path |
| OOM killed | Increase memory limits in compose or Kubernetes resource spec |
| Permission denied on TLS files | Ensure certificate files are owned/readable by the container user (1001:1001 Docker, 1000:1000 Kubernetes) |
