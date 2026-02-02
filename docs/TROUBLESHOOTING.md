# UDPS Troubleshooting Guide

This guide covers common issues encountered when developing, deploying, and operating UDPS. Each issue follows a consistent structure: symptom, cause, and solution with concrete commands. For configuration details, see [CONFIGURATION.md](CONFIGURATION.md). For deployment procedures, see [DEPLOYMENT.md](DEPLOYMENT.md). For observability setup, see [observability.md](observability.md).

---

## 1. Quick Diagnostic Checklist

Run through this checklist before investigating specific issues. Each command targets a different failure domain and can be executed independently.

```bash
# 1. Verify UDPS is running
docker ps --filter name=udps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# 2. Check overall health
curl -s http://localhost:8081/health | jq .

# 3. Check component health
curl -s http://localhost:8081/health/ready | jq .

# 4. Check liveness
curl -s http://localhost:8081/health/live | jq .

# 5. Verify all infrastructure services
docker compose -f docker-compose.dev.yml ps

# 6. Check UDPS logs for errors
docker logs udps --tail 100 2>&1 | grep -i "error\|exception\|fatal"

# 7. Verify environment variables are set
docker exec udps env | grep UDPS_

# 8. Check Prometheus metrics endpoint
curl -s http://localhost:9090/metrics | head -20

# 9. Verify port availability
ss -tlnp | grep -E '50060|8443|9090|8081'
```

If the health endpoint returns a non-200 status, inspect the component-level response. The `ready` endpoint reports individual component states for `query-engine`, `storage`, `catalog`, and `governance`.

---

## 2. Startup Failures

### 2.1 Missing Required Environment Variables

**Symptom**: UDPS exits immediately on startup with a PureConfig error referencing unresolved substitutions.

```
com.typesafe.config.ConfigException$UnresolvedSubstitution:
  application.conf: Could not resolve substitution to a value: ${UDPS_DB_HOST}
```

**Cause**: One or more required environment variables are not set. UDPS uses Typesafe Config substitution syntax (`${VAR}`) without defaults for required variables. The following variables have no fallback and must be present at startup:

- `UDPS_DB_HOST`, `UDPS_DB_NAME`, `UDPS_DB_USER`, `UDPS_DB_PASSWORD`
- `UDPS_MINIO_ENDPOINT`, `UDPS_MINIO_ACCESS_KEY`, `UDPS_MINIO_SECRET_KEY`
- `UDPS_KAFKA_BOOTSTRAP_SERVERS`
- `UDPS_REDIS_HOST`
- `UDPS_TRACING_ENDPOINT`

**Solution**: Ensure all required variables are defined in your `.env` file or passed to the container. Verify with:

```bash
# Check which required variables are missing
for var in UDPS_DB_HOST UDPS_DB_NAME UDPS_DB_USER UDPS_DB_PASSWORD \
           UDPS_MINIO_ENDPOINT UDPS_MINIO_ACCESS_KEY UDPS_MINIO_SECRET_KEY \
           UDPS_KAFKA_BOOTSTRAP_SERVERS UDPS_REDIS_HOST UDPS_TRACING_ENDPOINT; do
  docker exec udps printenv "$var" > /dev/null 2>&1 || echo "MISSING: $var"
done
```

### 2.2 Port Conflicts

**Symptom**: Container starts but immediately exits, or `docker compose up` reports `bind: address already in use`.

**Cause**: Another process is bound to one of the four UDPS ports (50060, 8443, 9090, 8081) or one of the infrastructure ports (5432, 9000, 6379, 9092, 2181, 16686).

**Solution**: Identify the conflicting process and stop it, or remap the port.

```bash
# Identify which process holds the port
ss -tlnp | grep -E '50060|8443|9090|8081|5432|9000|6379|9092|2181|16686'

# If another UDPS instance or stale container is running
docker compose -f docker-compose.dev.yml down
docker compose down

# Override ports via environment variables if remapping is necessary
export UDPS_GRPC_PORT=50061
export UDPS_HTTPS_PORT=8444
```

### 2.3 TLS Certificate Not Found

**Symptom**: UDPS logs show a startup failure referencing certificate paths.

```
java.io.FileNotFoundException: /etc/udps/tls/tls.crt (No such file or directory)
```

**Cause**: TLS is enabled by default (`UDPS_TLS_ENABLED=true`), but the certificate, key, or CA files are not mounted into the container at the expected paths.

**Solution**: Either mount the TLS files or disable TLS for local development.

```bash
# Option 1: Disable TLS for local development
export UDPS_TLS_ENABLED=false

# Option 2: Mount certificates (Docker Compose)
# Add to your compose service definition:
#   volumes:
#     - ./certs/tls.crt:/etc/udps/tls/tls.crt:ro
#     - ./certs/tls.key:/etc/udps/tls/tls.key:ro
#     - ./certs/ca.crt:/etc/udps/tls/ca.crt:ro

# Option 3: Override certificate paths
export UDPS_TLS_CERT_PATH=/custom/path/tls.crt
export UDPS_TLS_KEY_PATH=/custom/path/tls.key
export UDPS_TLS_CA_PATH=/custom/path/ca.crt
```

### 2.4 Database Connection Failure at Startup

**Symptom**: UDPS starts but the `catalog` component reports UNHEALTHY. Logs show HikariCP connection timeout errors.

```
com.zaxxer.hikari.pool.HikariPool - Connection is not available, request timed out after 30000ms
```

**Cause**: PostgreSQL is not ready when UDPS attempts to connect, or the credentials are incorrect.

**Solution**: Verify PostgreSQL is healthy and accepting connections before starting UDPS.

```bash
# Check PostgreSQL container health
docker exec postgres pg_isready -U udps -d udps_catalog

# Test connectivity manually
docker exec postgres psql -U udps -d udps_catalog -c "SELECT 1;"

# If using Docker Compose, ensure depends_on with health check is configured
# The postgres service should pass: pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}
```

---

## 3. Infrastructure Connectivity

### 3.1 PostgreSQL

**Symptom**: Queries fail with connection errors. The `catalog` health component shows DEGRADED or UNHEALTHY. Heartbeat metadata shows `databaseReachable: false`.

**Cause**: PostgreSQL is unreachable, overloaded, or the connection pool is exhausted.

**Solution**:

```bash
# Verify PostgreSQL is running and healthy
docker exec postgres pg_isready -U udps -d udps_catalog

# Check active connections against pool limit (default: 20)
docker exec postgres psql -U udps -d udps_catalog \
  -c "SELECT count(*) FROM pg_stat_activity WHERE datname = 'udps_catalog';"

# Check for long-running queries holding connections
docker exec postgres psql -U udps -d udps_catalog \
  -c "SELECT pid, now() - pg_stat_activity.query_start AS duration, query
      FROM pg_stat_activity
      WHERE datname = 'udps_catalog' AND state != 'idle'
      ORDER BY duration DESC LIMIT 10;"

# If the pool is exhausted, increase it
export UDPS_DB_MAX_POOL_SIZE=40
```

### 3.2 MinIO (S3-Compatible Storage)

**Symptom**: Archive tier operations fail. Storage component reports errors during tier transitions to archive.

**Cause**: MinIO is unreachable, credentials are invalid, or the target bucket does not exist.

**Solution**:

```bash
# Verify MinIO is healthy
curl -s http://localhost:9000/minio/health/live

# Check MinIO connectivity and bucket existence using the mc client
docker exec minio mc alias set local http://localhost:9000 minioadmin "${MINIO_ROOT_PASSWORD}"
docker exec minio mc ls local/

# Create the bucket if it does not exist (default: udps-archive)
docker exec minio mc mb local/udps-archive --ignore-existing

# Verify credentials match between UDPS and MinIO
# UDPS_MINIO_ACCESS_KEY must match MINIO_ROOT_USER
# UDPS_MINIO_SECRET_KEY must match MINIO_ROOT_PASSWORD
```

### 3.3 Redis

**Symptom**: Query cache misses on every request despite repeated identical queries. Logs show Redis connection errors.

**Cause**: Redis is unreachable or has evicted all cached entries due to memory pressure.

**Solution**:

```bash
# Verify Redis is running
docker exec redis redis-cli ping
# Expected: PONG

# Check Redis memory usage
docker exec redis redis-cli info memory | grep used_memory_human

# Check number of cached keys
docker exec redis redis-cli dbsize

# Flush cache if corruption is suspected
docker exec redis redis-cli flushdb

# Test connectivity from UDPS container
docker exec udps nc -zv "${UDPS_REDIS_HOST}" 6379
```

### 3.4 Kafka

**Symptom**: Event streaming fails. UDPS cannot produce or consume messages. Logs show broker disconnect errors.

**Cause**: Kafka or ZooKeeper is unhealthy, or the bootstrap server address is incorrect. Kafka depends on ZooKeeper and will not start if ZooKeeper is unavailable.

**Solution**:

```bash
# Verify ZooKeeper is healthy
echo ruok | nc localhost 2181
# Expected: imok

# Verify Kafka is healthy
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check consumer group lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group udps-consumer-group \
  --describe

# Verify the bootstrap address UDPS is configured to use
# Inside Docker network: kafka:29092
# From host: localhost:9092
echo "UDPS_KAFKA_BOOTSTRAP_SERVERS=${UDPS_KAFKA_BOOTSTRAP_SERVERS}"
```

### 3.5 Jaeger (Distributed Tracing)

**Symptom**: No traces appear in the Jaeger UI at `http://localhost:16686`. The `TracingClientInterceptor` logs no errors.

**Cause**: The OTLP collector endpoint is misconfigured, Jaeger is not running, or the tracing sample rate is set to zero.

**Solution**:

```bash
# Verify Jaeger is running
curl -s http://localhost:16686/ > /dev/null && echo "Jaeger UI OK" || echo "Jaeger UI unreachable"

# Verify OTLP gRPC collector is accepting connections
nc -zv localhost 4317

# Check tracing configuration
echo "UDPS_TRACING_ENDPOINT=${UDPS_TRACING_ENDPOINT}"
echo "UDPS_TRACING_SAMPLE_RATE=${UDPS_TRACING_SAMPLE_RATE:-1.0}"

# Ensure sample rate is not zero
export UDPS_TRACING_SAMPLE_RATE=1.0
```

---

## 4. Query Engine Issues

### 4.1 Query Timeout

**Symptom**: Queries return a timeout error after 30 seconds (the default `udps.query.default-timeout`).

**Cause**: The query is scanning too much data, missing indexes, or the storage tier is slow (e.g., reading from cold or archive tier).

**Solution**: Optimize the query or increase the timeout. Check which storage tier the data resides on.

```bash
# Check Prometheus metrics for query latency distribution
curl -s http://localhost:9090/metrics | grep "udps.query.latency"

# Check active query count
curl -s http://localhost:9090/metrics | grep "udps.query.active_count"
```

The query timeout is configured in `udps-core/src/main/resources/application.conf` at `udps.query.default-timeout`. This value is not overridable via environment variable and requires a configuration file change and restart.

### 4.2 Concurrent Query Limit Reached

**Symptom**: New queries are rejected with a capacity error. Logs indicate the maximum concurrent query count has been reached.

**Cause**: More than 100 queries (default `udps.query.max-concurrent-queries`) are executing simultaneously.

**Solution**:

```bash
# Check current active query count via metrics
curl -s http://localhost:9090/metrics | grep "udps.query.active_count"

# Increase the concurrent query limit
export UDPS_MAX_CONCURRENT_QUERIES=200
# Restart UDPS to apply
```

Increasing the limit requires sufficient heap memory. Monitor JVM heap usage after adjusting this value.

### 4.3 Query Cache Not Working

**Symptom**: Identical queries always hit the query engine instead of returning cached results. Cache hit metrics remain at zero.

**Cause**: Redis is unreachable, the cache size is set to zero, or query parameters differ slightly between requests (e.g., different whitespace or casing).

**Solution**: Verify Redis connectivity (see Section 3.3), then check the cache configuration.

```bash
# Verify cache size setting
echo "UDPS_QUERY_CACHE_SIZE=${UDPS_QUERY_CACHE_SIZE:-10000}"

# Check cache hit/miss ratio via Prometheus
curl -s http://localhost:9090/metrics | grep "udps.query.cache"
```

### 4.4 Apache Calcite SQL Parse Errors

**Symptom**: Queries fail with a parse error from Apache Calcite. The error message references unexpected tokens or unsupported SQL syntax.

**Cause**: The submitted SQL uses syntax not supported by the Calcite SQL parser, such as database-specific extensions.

**Solution**: Rewrite the query using ANSI SQL syntax. Calcite supports standard SQL but does not support all vendor-specific extensions. Common issues include:

- Using `LIMIT` without `ORDER BY` (valid but may produce non-deterministic results)
- Unsupported window function syntax
- Non-standard type casting (use `CAST(x AS type)` instead of `x::type`)
- Backtick quoting (use double quotes for identifiers instead)

---

## 5. Storage Issues

### 5.1 Disk Space Exhaustion

**Symptom**: Write operations fail. The `storage` health component reports UNHEALTHY. Heartbeat metadata shows high `storageUsagePercent`.

**Cause**: One or more storage tier paths have run out of disk space.

**Solution**:

```bash
# Check disk usage on storage tier paths
df -h /data/udps/hot /data/udps/warm /data/udps/cold

# Inside Docker, check from the container
docker exec udps df -h /data/udps/hot /data/udps/warm /data/udps/cold

# Check which tier is consuming the most space
docker exec udps du -sh /data/udps/hot /data/udps/warm /data/udps/cold
```

If the hot tier is full, trigger a tier transition to move older data to warm or cold storage. If all tiers are full, expand the underlying volumes or add archive tier capacity via MinIO.

### 5.2 Storage Tier Transition Failures

**Symptom**: Data remains on the hot tier despite meeting the criteria for demotion to warm or cold. Logs show errors from `TierManager`.

**Cause**: The target tier path is not writable, does not exist, or has insufficient space. For archive tier transitions, MinIO connectivity may be the issue.

**Solution**:

```bash
# Verify tier directories exist and are writable by the udps user (UID 1001)
docker exec udps ls -la /data/udps/
docker exec udps touch /data/udps/warm/.write-test && \
  docker exec udps rm /data/udps/warm/.write-test && echo "Warm tier writable"

# For archive tier, verify MinIO (see Section 3.2)
curl -s http://localhost:9000/minio/health/live
```

### 5.3 Compaction Issues

**Symptom**: Read performance degrades over time. Many small Parquet files accumulate in the storage directories.

**Cause**: The compaction process has fallen behind ingestion, leaving many small files that increase I/O overhead during scans.

**Solution**: Monitor the number of files per tier and check storage worker thread utilization.

```bash
# Count files per tier
docker exec udps find /data/udps/hot -name "*.parquet" | wc -l
docker exec udps find /data/udps/warm -name "*.parquet" | wc -l

# If file count is high, consider increasing worker threads
export UDPS_STORAGE_WORKER_THREADS=16
# Restart UDPS to apply
```

### 5.4 Index Corruption

**Symptom**: Queries return incorrect results or fail with internal errors referencing index files. Parquet footer reads fail.

**Cause**: Incomplete writes due to a crash, disk errors, or a container killed during compaction.

**Solution**: Identify and rebuild the affected indexes. Back up the data directory before taking corrective action.

```bash
# Check for truncated or zero-byte Parquet files
docker exec udps find /data/udps -name "*.parquet" -size 0

# Check file integrity (Parquet files must have a valid footer)
# Remove corrupted files after confirming they can be regenerated from source
docker exec udps find /data/udps -name "*.parquet" -size 0 -delete
```

---

## 6. Authentication and Authorization

### 6.1 JWT Validation Failure

**Symptom**: All API requests return `401 Unauthorized`. Logs show JWT validation errors.

**Cause**: The JWT token is expired, malformed, or signed with a key that USP does not recognize.

**Solution**: Verify the token and USP connectivity.

```bash
# Decode the JWT payload (without validation) to check expiry
echo "${TOKEN}" | cut -d. -f2 | base64 -d 2>/dev/null | jq .

# Check if the 'exp' claim is in the past
echo "${TOKEN}" | cut -d. -f2 | base64 -d 2>/dev/null | jq '.exp | todate'

# Verify USP is reachable from the UDPS container
docker exec udps nc -zv <USP_HOST> <USP_GRPC_PORT>
```

### 6.2 USP Connectivity

**Symptom**: Authentication and authorization calls fail. Logs show gRPC `UNAVAILABLE` status code for USP service calls.

**Cause**: USP is down, the gRPC endpoint is misconfigured, or the circuit breaker to USP has tripped.

**Solution**: Verify USP is running and reachable. Check the circuit breaker state in UDPS logs.

```bash
# Check UDPS logs for circuit breaker state
docker logs udps --tail 200 2>&1 | grep -i "circuit.*breaker\|usp"

# Verify network connectivity to USP
docker exec udps nc -zv <USP_HOST> <USP_GRPC_PORT>
```

The circuit breaker trips after 5 consecutive failures within a 30-second window. It will auto-close after the configured reset timeout.

### 6.3 Permission Denied

**Symptom**: Authenticated requests return `403 Forbidden`. The JWT is valid but the operation is denied.

**Cause**: The user's role or permissions in USP do not grant access to the requested UDPS resource or operation.

**Solution**: Verify the user's permissions in USP. Check the `AuthorizationService` response by enabling debug logging for the integration layer.

```bash
# Enable debug logging for authorization
export LOG_LEVEL=DEBUG
# Restart UDPS, then reproduce the request and check logs
docker logs udps --tail 200 2>&1 | grep -i "authorization\|permission"
```

---

## 7. Integration Issues

### 7.1 UCCP Circuit Breaker Trips

**Symptom**: Logs show repeated `Failed to push metrics` or `Failed to send health report to UCCP` messages, followed by circuit breaker open state.

**Cause**: UCCP's `MonitoringService` is unreachable. The circuit breaker (5 failures / 30 seconds) has tripped to prevent wasted resources.

**Solution**:

```bash
# Verify UCCP is running
docker ps --filter name=uccp

# Check UCCP monitoring endpoint connectivity
nc -zv "${UCCP_MONITORING_HOST:-localhost}" "${UCCP_MONITORING_PORT:-9100}"

# Verify environment variables
echo "UCCP_MONITORING_HOST=${UCCP_MONITORING_HOST}"
echo "UCCP_MONITORING_PORT=${UCCP_MONITORING_PORT}"
```

The circuit breaker auto-closes once UCCP becomes reachable. No restart of UDPS is required. Metrics and health reports that were skipped during the open state are not retroactively pushed.

### 7.2 Service Discovery Failure

**Symptom**: UDPS does not appear in the UCCP service registry. Other services cannot discover UDPS.

**Cause**: The `HealthReporter` heartbeat is failing, or UCCP is not receiving the `HeartbeatRequest`.

**Solution**: Check the heartbeat cycle in UDPS logs. The heartbeat sends every 15 seconds.

```bash
# Check for heartbeat-related log entries
docker logs udps --tail 300 2>&1 | grep -i "heartbeat\|health.*report"

# Verify UDPS health status (this is what gets reported)
curl -s http://localhost:8081/health | jq .
```

### 7.3 USTRP Streaming Integration Failure

**Symptom**: Streaming data from USTRP is not being ingested. Kafka consumer shows increasing lag.

**Cause**: The Kafka consumer group has stalled, the topic does not exist, or USTRP is not producing to the expected topic.

**Solution**:

```bash
# Check consumer group lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group udps-consumer-group \
  --describe

# List available topics to verify the expected topic exists
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Reset consumer offset if the group is stuck (use with caution)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group udps-consumer-group \
  --topic <TOPIC_NAME> \
  --reset-offsets --to-latest \
  --execute
```

---

## 8. Docker and Container Issues

### 8.1 Image Build Failure

**Symptom**: `docker build` fails during the SBT assembly stage or the JRE stage.

**Cause**: SBT dependency resolution failure, insufficient disk space for the build context, or a network issue pulling the `eclipse-temurin:17-jre-alpine` base image.

**Solution**:

```bash
# Clean SBT caches and rebuild
sbt clean assembly

# If Docker build cache is stale
docker build --no-cache -t udps:latest .

# Check available disk space for Docker
docker system df
docker system prune -f  # Remove unused images/containers/networks
```

### 8.2 OOM Kill

**Symptom**: The UDPS container is killed by the kernel OOM killer. `docker inspect` shows `OOMKilled: true`. A heap dump may be written (configured via `-XX:+HeapDumpOnOutOfMemoryError`).

**Cause**: The JVM heap exceeds the container memory limit. With `MaxRAMPercentage=75.0`, the JVM claims 75% of the container memory for heap, leaving 25% for off-heap, metaspace, and the OS. If the container memory limit is too low, or the workload creates excessive objects, the heap fills up.

**Solution**:

```bash
# Check if OOM killed
docker inspect udps --format '{{.State.OOMKilled}}'

# Check container memory limit
docker stats udps --no-stream --format "table {{.MemUsage}}\t{{.MemPerc}}"

# Increase container memory limit in Docker Compose
# mem_limit: 4g  (in your compose file)

# Or adjust JVM heap bounds for production compose
export JVM_HEAP_MIN=1g
export JVM_HEAP_MAX=3g

# Look for the heap dump file
docker exec udps find / -name "*.hprof" 2>/dev/null
```

### 8.3 Health Check Failure in Docker Compose

**Symptom**: Docker Compose reports the UDPS container as unhealthy. Dependent services fail to start.

**Cause**: The health check endpoint on port 8081 is not yet ready (startup in progress), or the health check command in the compose file has an incorrect URL or timeout.

**Solution**:

```bash
# Test the health endpoint manually
curl -sf http://localhost:8081/health/live || echo "Liveness check failed"
curl -sf http://localhost:8081/health/ready || echo "Readiness check failed"

# Check what the compose health check is configured to do
docker inspect udps --format '{{json .Config.Healthcheck}}' | jq .

# Increase startup grace period in compose if the issue is slow startup
# healthcheck:
#   start_period: 60s
#   interval: 10s
#   timeout: 5s
#   retries: 5
```

### 8.4 Volume Permission Errors

**Symptom**: UDPS fails to write to storage directories. Logs show `Permission denied` errors for paths under `/data/udps/`.

**Cause**: The container runs as the non-root `udps` user (UID 1001). The mounted volumes must be owned by or writable for UID 1001.

**Solution**:

```bash
# Check ownership inside the container
docker exec udps ls -la /data/udps/

# Fix ownership on the host (adjust host path as needed)
sudo chown -R 1001:1001 /path/to/host/data/udps

# Or set permissions to be world-writable (less secure, development only)
sudo chmod -R 777 /path/to/host/data/udps
```

---

## 9. Kubernetes Deployment Issues

### 9.1 Pod CrashLoopBackOff

**Symptom**: The UDPS pod enters `CrashLoopBackOff`. `kubectl describe pod` shows repeated container restarts.

**Cause**: Missing ConfigMap or Secret values for required environment variables, TLS certificate Secrets not mounted, or infrastructure services not reachable from the pod network.

**Solution**:

```bash
# Check pod status and events
kubectl describe pod -l app=udps

# Check container logs
kubectl logs -l app=udps --previous

# Verify all required env vars are present in the pod spec
kubectl get pod -l app=udps -o jsonpath='{.items[0].spec.containers[0].env[*].name}' | tr ' ' '\n' | sort

# Verify secrets are mounted
kubectl exec -it deploy/udps -- ls -la /etc/udps/tls/
```

### 9.2 Readiness Probe Failure

**Symptom**: The pod is Running but not Ready. Traffic is not routed to the pod. `kubectl describe pod` shows readiness probe failures.

**Cause**: One or more health components (`query-engine`, `storage`, `catalog`, `governance`) is reporting UNHEALTHY, causing the `/health/ready` endpoint to return a non-200 status.

**Solution**:

```bash
# Check readiness endpoint directly
kubectl exec -it deploy/udps -- wget -qO- http://localhost:8081/health/ready

# Check which component is unhealthy
kubectl exec -it deploy/udps -- wget -qO- http://localhost:8081/health | python3 -m json.tool

# Check pod resource usage
kubectl top pod -l app=udps
```

### 9.3 Horizontal Pod Autoscaler Not Scaling

**Symptom**: The HPA does not scale up despite high load. `kubectl describe hpa` shows `<unknown>` for current metrics.

**Cause**: The metrics server is not installed, or the UDPS pod does not have resource requests defined (HPA requires requests to calculate utilization percentages).

**Solution**:

```bash
# Verify metrics-server is running
kubectl get deployment metrics-server -n kube-system

# Check HPA status
kubectl describe hpa udps

# Ensure resource requests are set in the pod spec
kubectl get deploy udps -o jsonpath='{.spec.template.spec.containers[0].resources}' | jq .

# Resource requests should be set, for example:
# resources:
#   requests:
#     cpu: "500m"
#     memory: "1Gi"
#   limits:
#     cpu: "2"
#     memory: "4Gi"
```

### 9.4 Resource Limits Causing Throttling

**Symptom**: Query latency is significantly higher in Kubernetes than in Docker Compose. CPU throttling is observed.

**Cause**: CPU limits are too restrictive, causing the kernel CFS scheduler to throttle the JVM.

**Solution**:

```bash
# Check for CPU throttling
kubectl exec -it deploy/udps -- cat /sys/fs/cgroup/cpu/cpu.stat 2>/dev/null || \
kubectl exec -it deploy/udps -- cat /sys/fs/cgroup/cpu.stat

# Look for nr_throttled and throttled_time values
# If throttled_time is high, increase CPU limits

# Check current limits
kubectl get deploy udps -o jsonpath='{.spec.template.spec.containers[0].resources.limits}' | jq .
```

---

## 10. Performance Tuning

### 10.1 JVM Tuning

The default JVM configuration uses G1GC with 75% of container memory allocated to heap. Adjust based on workload characteristics.

```bash
# Current JVM flags (check from inside the container)
docker exec udps java -XX:+PrintFlagsFinal -version 2>&1 | grep -E "MaxRAMPercent|UseG1GC|MaxHeap"

# For write-heavy workloads, tune G1GC region size
# Add to JAVA_OPTS in your compose file:
#   -XX:G1HeapRegionSize=16m

# For large heaps (>8 GB), enable NUMA awareness
#   -XX:+UseNUMA

# Monitor GC behavior
docker exec udps jstat -gc $(docker exec udps pgrep java) 1000 5
```

Key JVM flags set by the Dockerfile:

| Flag | Value | Purpose |
|------|-------|---------|
| `-XX:+UseContainerSupport` | enabled | Respect container memory limits |
| `-XX:MaxRAMPercentage` | `75.0` | Heap as percentage of container memory |
| `-XX:InitialRAMPercentage` | `50.0` | Initial heap as percentage of container memory |
| `-XX:+UseG1GC` | enabled | G1 garbage collector |
| `-XX:+HeapDumpOnOutOfMemoryError` | enabled | Dump heap on OOM for post-mortem analysis |

### 10.2 Connection Pool Tuning

**Symptom**: Database operations queue behind connection acquisition. HikariCP logs warn about connection wait times.

**Solution**: Increase the pool size, but do not exceed the PostgreSQL `max_connections` setting.

```bash
# Check current PostgreSQL max connections
docker exec postgres psql -U udps -d udps_catalog -c "SHOW max_connections;"

# Check current pool size (default: 20)
echo "UDPS_DB_MAX_POOL_SIZE=${UDPS_DB_MAX_POOL_SIZE:-20}"

# Increase pool size (must be less than PostgreSQL max_connections)
export UDPS_DB_MAX_POOL_SIZE=40
```

A reasonable rule: set the pool size to `(2 * CPU_cores) + disk_spindles` for OLTP workloads. For UDPS metadata operations, 20-40 connections typically suffice.

### 10.3 Query Cache Tuning

**Symptom**: Low cache hit ratio despite repeated queries.

**Solution**: Increase the cache size and monitor Redis memory.

```bash
# Check cache metrics
curl -s http://localhost:9090/metrics | grep "udps.query.cache"

# Increase cache size (number of entries, stored in Redis)
export UDPS_QUERY_CACHE_SIZE=50000

# Monitor Redis memory after increasing
docker exec redis redis-cli info memory | grep used_memory_human
```

### 10.4 Storage Worker Threads

**Symptom**: Ingestion or query throughput is limited by I/O parallelism. Storage worker threads are saturated.

**Solution**: Increase worker threads to match available CPU cores and I/O capacity.

```bash
# Check current setting (default: 8)
echo "UDPS_STORAGE_WORKER_THREADS=${UDPS_STORAGE_WORKER_THREADS:-8}"

# Set to CPU core count for I/O-bound workloads
export UDPS_STORAGE_WORKER_THREADS=$(nproc)

# For NVMe storage with high IOPS, consider 2x core count
export UDPS_STORAGE_WORKER_THREADS=$(($(nproc) * 2))
```

### 10.5 Compression Codec Selection

Choose the compression codec based on workload priority. The default `zstd` provides the best general-purpose tradeoff.

| Priority | Recommended Codec | Set Via |
|----------|-------------------|---------|
| Balanced (default) | `zstd` | `UDPS_DEFAULT_COMPRESSION=zstd` |
| Low-latency ingestion | `lz4` | `UDPS_DEFAULT_COMPRESSION=lz4` |
| Maximum compression (archive) | `gzip` | `UDPS_DEFAULT_COMPRESSION=gzip` |
| CPU-constrained | `snappy` | `UDPS_DEFAULT_COMPRESSION=snappy` |

Per-table compression overrides can be set via the gRPC API without changing the global default.

---

## 11. Logging and Debugging

### 11.1 Log Levels

UDPS uses structured logging via Logback and `scala-logging`. The default log level is `INFO`.

```bash
# Set log level via environment variable
export LOG_LEVEL=DEBUG

# Available levels (from most to least verbose):
# TRACE, DEBUG, INFO, WARN, ERROR

# Restart UDPS to apply the change
```

The Logback configuration is at `udps-core/src/main/resources/logback.xml`.

### 11.2 Enabling Debug Logging for Specific Components

For targeted debugging without the noise of global `DEBUG`, modify the Logback configuration or use the log level environment variable for the entire application then filter output.

```bash
# Filter logs by component
docker logs udps 2>&1 | grep "io.gbmm.udps.query"       # Query engine
docker logs udps 2>&1 | grep "io.gbmm.udps.storage"      # Storage layer
docker logs udps 2>&1 | grep "io.gbmm.udps.integration"   # Integration (USP/UCCP/USTRP)
docker logs udps 2>&1 | grep "com.zaxxer.hikari"           # Connection pool
docker logs udps 2>&1 | grep "io.grpc"                     # gRPC framework
```

### 11.3 Tracing Sample Rate

Adjust the tracing sample rate to control the volume of traces sent to Jaeger via the OTLP collector.

| Environment | Recommended Rate | Variable |
|-------------|------------------|----------|
| Development | `1.0` (all traces) | `UDPS_TRACING_SAMPLE_RATE=1.0` |
| Staging | `0.5` (50%) | `UDPS_TRACING_SAMPLE_RATE=0.5` |
| Production (low traffic) | `0.1` (10%) | `UDPS_TRACING_SAMPLE_RATE=0.1` |
| Production (high traffic) | `0.01` (1%) | `UDPS_TRACING_SAMPLE_RATE=0.01` |

Setting the rate to `0.0` disables trace sampling entirely, though trace context headers are still propagated if `UDPS_TRACING_ENABLED=true`.

### 11.4 Prometheus Metrics Inspection

```bash
# View all UDPS-specific metrics
curl -s http://localhost:9090/metrics | grep "^udps\."

# View JVM metrics
curl -s http://localhost:9090/metrics | grep "^jvm\."

# Check specific metric families
curl -s http://localhost:9090/metrics | grep "udps.query"
curl -s http://localhost:9090/metrics | grep "udps.storage"
```

---

## 12. Common Error Messages

| Error Message | Cause | Solution |
|---------------|-------|----------|
| `ConfigException$UnresolvedSubstitution: ${UDPS_DB_HOST}` | Required environment variable not set | Set the variable in `.env` or container environment. See Section 2.1. |
| `Connection is not available, request timed out after 30000ms` | HikariCP pool exhausted or PostgreSQL unreachable | Check PostgreSQL health and increase `UDPS_DB_MAX_POOL_SIZE`. See Section 3.1. |
| `Failed to push metrics` | UCCP MonitoringService unreachable | Verify `UCCP_MONITORING_HOST` and `UCCP_MONITORING_PORT`. See Section 7.1. |
| `Failed to send health report to UCCP` | UCCP heartbeat endpoint unreachable | Same as above. Circuit breaker will auto-close when UCCP recovers. |
| `UNAVAILABLE: io exception` (gRPC) | Target gRPC service is down or network partition | Check connectivity to the target service. See Sections 6.2 and 7.1. |
| `PERMISSION_DENIED` (gRPC) | USP authorization check failed | Verify user permissions in USP. See Section 6.3. |
| `UNAUTHENTICATED` (gRPC) | JWT validation failed | Check token expiry and USP connectivity. See Section 6.1. |
| `FileNotFoundException: /etc/udps/tls/tls.crt` | TLS enabled but certificates not mounted | Mount certificates or set `UDPS_TLS_ENABLED=false`. See Section 2.3. |
| `Address already in use: bind` | Port conflict with another process | Find and stop the conflicting process. See Section 2.2. |
| `OOMKilled` (Docker) | Container exceeded memory limit | Increase container memory or tune `JVM_HEAP_MAX`. See Section 8.2. |
| `No metrics matched enabled prefixes, skipping push` | Metric names do not match configured prefixes | Ensure metric names start with `udps.` or `jvm.`. See [observability.md](observability.md). |
| `circuit breaker is open` | Repeated failures to an integration endpoint | The upstream service is unavailable. Wait for auto-recovery or investigate the target service. |
| `Query timeout after 30s` | Query exceeded `udps.query.default-timeout` | Optimize the query or check storage tier latency. See Section 4.1. |
| `Max concurrent queries reached` | Active queries at `UDPS_MAX_CONCURRENT_QUERIES` limit | Increase the limit or reduce query concurrency. See Section 4.2. |
| `Permission denied` on `/data/udps/*` | Container user (UID 1001) lacks write access | Fix volume ownership to UID 1001. See Section 8.4. |
| `ObservabilityConfig validation failed` | Invalid observability config values | Check `UCCP_MONITORING_HOST` is non-empty, port is 1-65535, sample rate is 0.0-1.0. |

---

## Appendix: Useful Diagnostic Commands

```bash
# Full infrastructure status (all containers)
docker compose -f docker-compose.dev.yml ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# UDPS resource consumption
docker stats udps --no-stream

# Follow UDPS logs in real time
docker logs -f udps 2>&1

# Export a heap dump from a running container (without OOM)
docker exec udps jmap -dump:live,format=b,file=/tmp/udps-heap.hprof $(docker exec udps pgrep java)
docker cp udps:/tmp/udps-heap.hprof ./udps-heap.hprof

# Thread dump for deadlock analysis
docker exec udps jstack $(docker exec udps pgrep java) > udps-threads.txt

# Check container filesystem usage
docker exec udps df -h

# Inspect network connectivity between containers
docker network inspect seri-sa-platform
```
