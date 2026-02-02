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

Generate TLS certificates using the provided script:

```bash
# Local development (localhost SANs)
./scripts/generate-certs.sh

# With custom domains
./scripts/generate-certs.sh --domain udps.company.com --domain grpc.udps.company.com

# Custom output directory
./scripts/generate-certs.sh --output /path/to/certs
```

This generates CA (4096-bit), server (2048-bit), and client (2048-bit) certificates with SAN support. Generated files are placed in `deploy/docker/certs/` by default.

To enable TLS on all services, use the secrets overlay:

```bash
docker compose -f docker-compose.prod.yml -f deploy/docker/docker-compose.secrets.yml up -d
```

The secrets overlay mounts certificates into PostgreSQL, Redis, Kafka, and MinIO with proper TLS configuration.

In production, use a proper PKI or cert-manager instead of self-signed certificates.

### 2.4 Start the stack

```bash
docker compose -f docker-compose.prod.yml up -d
```

### 2.5 One-command deployment (recommended)

The deploy scripts in `deploy/docker/` provide USP-style one-command deployment:

```bash
cd deploy/docker

# Full deployment: build, setup, start, validate
./deploy.sh

# Quick start (skip build if images exist)
./deploy.sh --quick

# Check deployment status
./deploy.sh --status

# View live logs
./deploy.sh --logs

# Run validation tests
./deploy.sh --test
```

Additional scripts:

| Script | Purpose |
|--------|---------|
| `setup.sh` | Interactive setup: generates `.env`, TLS certs, validates configuration |
| `teardown.sh` | Stop services: `--full` removes volumes, `--purge` removes everything |
| `validate-config.sh` | Validates environment variables, secret strength, and certificates |

A `Makefile` provides convenient targets:

```bash
make deploy      # Full deployment
make quick       # Quick start
make setup       # Interactive setup
make build       # Build images
make start       # Start services
make stop        # Stop services
make logs        # View logs
make status      # Check status
make health      # Health checks
make clean       # Full cleanup
```

### 2.6 Production resource limits

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

### 2.7 Security features in production compose

- `read_only: true` -- Container filesystem is read-only; only `/tmp` and `/var/log/udps` are writable via tmpfs
- `no-new-privileges: true` -- Prevents privilege escalation
- `--requirepass` on Redis -- Authentication required
- `--auth-host=scram-sha-256` on PostgreSQL -- Strong password authentication
- SSL enabled on Kafka and PostgreSQL
- Dangerous Redis commands (`FLUSHALL`, `FLUSHDB`, `DEBUG`) are disabled
- JSON file logging with rotation (50 MB max, 5 files)

## 3. Kubernetes Deployment

### 3.1 Kustomize (recommended)

UDPS uses Kustomize with base and environment overlays in `deploy/kubernetes/`:

```
deploy/kubernetes/
├── base/                    # Base manifests (16 resources)
│   ├── kustomization.yaml
│   ├── namespace.yaml
│   ├── serviceaccount.yaml
│   ├── role.yaml
│   ├── rolebinding.yaml
│   ├── configmap.yaml
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── ingress.yaml
│   ├── hpa.yaml
│   ├── pdb.yaml
│   ├── networkpolicy.yaml
│   ├── resourcequota.yaml
│   ├── podsecuritystandard.yaml
│   └── servicemonitor.yaml
└── overlays/
    ├── development/         # 1 replica, DEBUG logging, dev hostnames
    ├── staging/             # 2 replicas, INFO logging, staging hostnames
    └── production/          # 3 replicas, WARN logging, prod hostnames, topology spread
```

Deploy with Kustomize:

```bash
# Development
kubectl apply -k deploy/kubernetes/overlays/development

# Staging
kubectl apply -k deploy/kubernetes/overlays/staging

# Production
kubectl apply -k deploy/kubernetes/overlays/production
```

#### Environment overlay differences

| Parameter | Development | Staging | Production |
|-----------|-------------|---------|------------|
| Namespace | `seri-sa-platform-dev` | `seri-sa-platform-staging` | `seri-sa-platform` |
| Replicas | 1 | 2 | 3 |
| JVM heap | 256m-1g | 512m-2g | 1g-3g |
| CPU request/limit | 250m/2000m | 500m/3000m | 1000m/4000m |
| Memory request/limit | 512Mi/2Gi | 1Gi/4Gi | 2Gi/6Gi |
| HPA max | 2 | 5 | 20 |
| Log level | DEBUG | INFO | WARN |
| Rate limiting | none | none | 500 rps (HTTP), 1000 rps (gRPC) |
| Topology spread | no | no | zone-aware |

### 3.2 Helm chart

A Helm chart is available in `deploy/helm/udps/`:

```bash
# Install with defaults
helm install udps deploy/helm/udps/

# Install with custom values
helm install udps deploy/helm/udps/ \
  --namespace seri-sa-platform \
  --set replicaCount=3 \
  --set image.tag=v0.1.0 \
  --set ingress.enabled=true

# Upgrade
helm upgrade udps deploy/helm/udps/ --set image.tag=v0.2.0
```

### 3.3 Base resources

The Kustomize base includes these Kubernetes resources:

| Resource | Purpose |
|----------|---------|
| Namespace | `seri-sa-platform` |
| ServiceAccount | Pod identity with `automountServiceAccountToken: false` |
| Role/RoleBinding | Least-privilege RBAC |
| ConfigMap | Non-sensitive configuration (ports, JVM settings, feature flags) |
| Deployment | 2 replicas, rolling update, init container for PostgreSQL readiness |
| Service | ClusterIP: gRPC (50060), HTTPS (8443), health (8081), metrics (9090) |
| Ingress | HTTPS and gRPC ingress with TLS, security headers |
| HPA | CPU (70%) and memory (80%) autoscaling, 2-10 replicas |
| PDB | MinAvailable: 1 |
| NetworkPolicy | Restricts ingress/egress to required ports and dependencies |
| ResourceQuota | Limits total namespace CPU (20 cores) and memory (40Gi) |
| PodSecurityStandard | Restricted profile enforcement |
| ServiceMonitor | Prometheus Operator metrics scraping (requires Prometheus Operator) |

### 3.4 Security features

- **Pod security:** `runAsNonRoot`, `readOnlyRootFilesystem`, all capabilities dropped, `seccompProfile: RuntimeDefault`
- **Network policy:** Only allows traffic from ingress controller and between required services (PostgreSQL, Redis, Kafka, MinIO, UCCP, OTEL)
- **RBAC:** ServiceAccount with minimal permissions
- **Resource quotas:** Prevents namespace resource exhaustion
- **Pod security standards:** Restricted profile enforcement

## 4. CI/CD Pipelines

GitHub Actions workflows in `.github/workflows/` provide automated deployment pipelines following USP patterns:

### 4.1 PR validation (`pr-validation.yml`)

Triggered on pull requests to `main`, `development`, or `staging`:

- Scala build and test with scoverage (60% coverage warning)
- `scalafmt` format check
- Dependency vulnerability scan (`sbt dependencyCheck`)
- Dependency license review (blocks GPL-3.0, AGPL-3.0)
- Dockerfile linting (hadolint)

### 4.2 Development pipeline (`development.yml`)

Triggered on push to `development`:

- Build and test with PostgreSQL/Redis services
- Security scan
- Docker image build, push, and Cosign signing (`dev-<sha>` tags)
- Kustomize deployment to development namespace
- Smoke tests
- Slack notification

### 4.3 Staging pipeline (`staging.yml`)

Triggered on push to `staging`:

- Build and test with **70% coverage hard fail**
- Trivy filesystem and container vulnerability scans
- Docker image build with `staging-<version>` tags
- Kustomize deployment to staging namespace
- Smoke tests, E2E tests, k6 performance baseline
- 14-day artifact retention

### 4.4 Production pipeline (`production.yml`)

Triggered on version tags (`v*.*.*`):

- Version validation (semver format)
- Build and test with **80% coverage hard fail**
- Trivy scans with **exit-code: 1** (blocks on CRITICAL/HIGH)
- Docker image with semantic version tags (`X.Y.Z`, `X.Y`, `X`, `latest`, `stable`)
- **Blue-green deployment** with automatic rollback
- Health check validation before traffic switch
- Automatic rollback on smoke test failure
- GitHub Release with auto-generated changelog
- 90-day artifact retention

## 5. Health Checks and Monitoring

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

## 6. Scaling Guidelines

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

## 7. Troubleshooting

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
