# UDPS Integration Guide

## Overview

The Unified Data Platform Service (UDPS) operates within a broader microservices ecosystem comprising three platform services. Each platform provides a distinct capability that UDPS depends on at runtime:

- **USP (Unified Security Platform)** -- Authentication, authorization, secret management, and outpost registration. UDPS delegates all identity verification and permission checks to USP rather than implementing its own security layer. USP also manages the secret lifecycle for credentials that UDPS consumes (database passwords, API keys, encryption keys).
- **UCCP (Unified Coordination & Control Plane)** -- Service discovery, distributed coordination, health reporting, metrics collection, and distributed tracing. UCCP serves as the control plane through which UDPS registers itself, discovers peer services, acquires distributed locks, and reports operational telemetry.
- **USTRP (Unified Stream Runtime Platform)** -- Stream ingestion via Kafka. USTRP manages the Kafka infrastructure, schema registry, and stream topology. UDPS consumes data streams through USTRP-managed topics with exactly-once semantics and dead-letter queue support.

All integration code lives in a single SBT module:

```
udps-integration/src/main/scala/io/gbmm/udps/integration/
```

This module depends only on `udps-core` and exposes typed clients that the `udps-api` module wires into the application lifecycle via `Resource[IO, _]`.

## Architecture

```
                          +-------------------------------------------+
                          |                 UDPS                      |
                          |                                           |
                          |  +----------+  +----------+  +--------+  |
                          |  |  query   |  | catalog  |  |  api   |  |
                          |  +----+-----+  +----+-----+  +---+----+  |
                          |       |             |             |       |
                          |       +-------+-----+-------------+       |
                          |               |                           |
                          |        +------+--------+                  |
                          |        | integration   |                  |
                          |        +--+----+----+--+                  |
                          +-----------|----|----|----------------------+
                                      |    |    |
                    +-----------------+    |    +------------------+
                    |                      |                       |
                    v                      v                       v
          +------------------+   +------------------+   +------------------+
          |       USP        |   |      UCCP        |   |      USTRP       |
          |                  |   |                  |   |                  |
          | - Auth (JWT)     |   | - Discovery      |   | - Kafka Brokers  |
          | - Authz (RBAC)   |   | - Locks          |   | - Schema Registry|
          | - Secrets        |   | - Health         |   | - DLQ Topics     |
          | - Outpost Reg.   |   | - Metrics        |   |                  |
          |                  |   | - Tracing        |   |                  |
          +------------------+   +------------------+   +------------------+

          Protocol: gRPC/mTLS   Protocol: gRPC/mTLS    Protocol: Kafka + HTTP
```

All gRPC communication uses mTLS with TLS 1.3. Kafka communication uses SASL/SSL. The Schema Registry client communicates over HTTPS.

Every gRPC client wraps its calls in a circuit breaker (`IntegrationCircuitBreaker`) to prevent cascading failures when a platform service is temporarily unavailable.

## USP Integration

**Source:** `udps-integration/src/main/scala/io/gbmm/udps/integration/`

UDPS integrates with USP for four capabilities: outpost registration, authentication, authorization, and secret management. Each capability has a dedicated client class.

### Outpost Registration

**Class:** `OutpostRegistrationClient`

UDPS registers itself with USP as a compute outpost on startup. This registration enables USP to track UDPS instances, route security policies, and enforce platform-wide governance.

**Configuration:**

```hocon
udps.integration.usp.outpost {
  usp-host = ${UDPS_USP_HOST}
  usp-port = ${UDPS_USP_PORT}
  outpost-name = "udps"
  outpost-type = 6              # OUTPOST_TYPE_COMPUTE
  capabilities = [
    "columnar_storage",
    "sql_query",
    "metadata_catalog",
    "data_governance"
  ]
  heartbeat-interval = 30 seconds
}
```

Config class: `OutpostRegistrationConfig(uspHost, uspPort, outpostName, outpostType, capabilities, heartbeatInterval)`

**Lifecycle:**

1. On startup, `registerOutpost` sends a registration RPC to USP. USP returns an `outpostId` that identifies this instance for the duration of its lifecycle.
2. A background `heartbeatStream` emits periodic heartbeats at `heartbeatInterval`, each carrying health details (uptime, resource utilization, active connections). USP uses these heartbeats to determine outpost liveness.
3. `sendHeartbeat` is the single-shot variant used by the streaming heartbeat loop.
4. On shutdown (resource finalization), `deregisterOutpost` notifies USP that this instance is going offline. This is implemented via `Resource[IO, _]` finalization to guarantee cleanup even on abnormal termination.

**Key behavior:** If the registration RPC fails, the client retries with exponential backoff through the circuit breaker. The heartbeat stream is resilient to transient failures -- a missed heartbeat does not terminate the stream; USP tolerates a configurable number of missed heartbeats before marking the outpost as unhealthy.

### Authentication

**Class:** `AuthenticationClient`

All inbound requests to UDPS carry a JWT issued by USP. The `AuthenticationClient` validates these tokens, caching validation results to avoid redundant RPCs.

**Configuration:**

```hocon
udps.integration.usp.auth {
  usp-host = ${UDPS_USP_HOST}
  usp-port = ${UDPS_USP_PORT}
  cache-ttl = 5 minutes
  max-cache-size = 10000
}
```

Config class: `AuthConfig(uspHost, uspPort, cacheTtl, maxCacheSize)`

**Validation flow:**

1. `validateToken(token)` first calls `getCachedValidation(token)` to check the in-memory cache.
2. If a cached result exists and the token has not expired (checked against the token's `exp` claim), the cached result is returned immediately.
3. If no cached result exists or the token has expired, the client sends a validation RPC to USP. The response contains the user identity, roles, and token metadata.
4. On successful validation, `cacheValidation` stores the result keyed by the raw token string. The cache entry has a TTL of `cacheTtl` and is also invalidated when the token's `exp` timestamp is reached, whichever comes first.

**Cache characteristics:** The cache is bounded by `maxCacheSize` entries with LRU eviction. The cache key is the full token string. This means distinct tokens for the same user occupy separate cache entries, which is the correct behavior since each token may carry different claims.

### Authorization

**Class:** `AuthorizationClient`

After authentication, UDPS checks whether the authenticated user has permission to perform the requested operation on the target resource. The `AuthorizationClient` delegates these checks to USP.

**Configuration:**

```hocon
udps.integration.usp.authz {
  usp-host = ${UDPS_USP_HOST}
  usp-port = ${UDPS_USP_PORT}
  cache-ttl = 5 minutes
  max-cache-size = 50000
}
```

Config class: `AuthzConfig(uspHost, uspPort, cacheTtl, maxCacheSize)`

**Methods:**

| Method | Signature | Description |
|--------|-----------|-------------|
| `checkPermission` | `(userId, resource, action) => IO[AuthzDecision]` | Full permission check with caching |
| `isAuthorized` | `(userId, resource, action) => IO[Boolean]` | Convenience wrapper returning a boolean |
| `getCachedDecision` | `(userId, resource, action) => IO[Option[AuthzDecision]]` | Cache lookup only |
| `cacheDecision` | `(userId, resource, action, decision) => IO[Unit]` | Explicit cache write |
| `invalidateCache` | `IO[Unit]` | Purge the entire authorization cache |

**Cache key:** The cache is keyed by the tuple `(userId, resource, action)`. For example, `("user-123", "catalog:table:orders", "read")` is a distinct cache entry from `("user-123", "catalog:table:orders", "write")`.

The `invalidateCache` method is called when UDPS receives a policy-change notification from USP (via the outpost heartbeat channel), ensuring that stale authorization decisions do not persist after a policy update.

### Secrets

**Class:** `SecretsClient`

UDPS retrieves sensitive values (database passwords, encryption keys, API tokens for external services) from USP's secret store rather than reading them from environment variables or configuration files.

**Configuration:**

```hocon
udps.integration.usp.secrets {
  usp-host = ${UDPS_USP_HOST}
  usp-port = ${UDPS_USP_PORT}
  cache-ttl = 10 minutes
  max-cache-size = 1000
  refresh-before-expiry = 2 minutes
}
```

Config class: `SecretsConfig(uspHost, uspPort, cacheTtl, maxCacheSize, refreshBeforeExpiry)`

**Methods:**

| Method | Signature | Description |
|--------|-----------|-------------|
| `getSecret` | `(name) => IO[SecretValue]` | Retrieve a secret by name, using cache when available |
| `encryptValue` | `(plaintext) => IO[CipherText]` | Encrypt a value using USP's KMS |
| `decryptValue` | `(ciphertext) => IO[PlainText]` | Decrypt a value using USP's KMS |
| `listSecrets` | `IO[List[SecretMetadata]]` | List available secrets (metadata only, no values) |
| `getCachedSecret` | `(name) => IO[Option[SecretValue]]` | Cache lookup only |
| `cacheSecret` | `(name, value) => IO[Unit]` | Explicit cache write |

**Proactive refresh:** The `proactiveRefreshStream` is a background FS2 stream that monitors cached secrets and refreshes them `refreshBeforeExpiry` before their TTL expires. This prevents a thundering-herd scenario where multiple concurrent requests all discover an expired secret simultaneously and all issue RPCs to USP. The stream runs for the lifetime of the `SecretsClient` resource.

## UCCP Integration

**Source:** `udps-integration/src/main/scala/io/gbmm/udps/integration/`

UDPS integrates with UCCP for service discovery, distributed locking, health reporting, metrics push, and distributed tracing.

### Service Discovery

**Class:** `ServiceDiscoveryClient`

UDPS registers itself with UCCP's service registry on startup and uses the same registry to discover other services in the ecosystem.

**Configuration:**

```hocon
udps.integration.uccp.discovery {
  uccp-host = ${UDPS_UCCP_HOST}
  uccp-port = ${UDPS_UCCP_PORT}
  service-name = "udps"
  service-type = "data-platform"
  version = ${UDPS_VERSION}
  address = ${UDPS_GRPC_HOST}
  port = ${UDPS_GRPC_PORT}
  protocol = "grpc"
  heartbeat-interval = 15 seconds
}
```

Config class: `ServiceDiscoveryConfig(uccpHost, uccpPort, serviceName, serviceType, version, address, port, protocol, heartbeatInterval)`

**Lifecycle:**

1. `registerService` registers UDPS with UCCP. UCCP returns a service instance ID.
2. `heartbeatStream` maintains liveness by sending periodic heartbeats at `heartbeatInterval`. `sendHeartbeat` is the single-shot variant.
3. `discoverService(name)` queries UCCP for healthy instances of a named service. Returns a list of endpoints.
4. `watchStream` returns an FS2 stream of service events (instance registered, deregistered, health changed) for a given service name. UDPS uses this to maintain a local view of peer instances.
5. `deregisterService` is called on shutdown via `Resource[IO, _]` finalization.

### Distributed Locks

**Class:** `DistributedLockClient`

UDPS uses distributed locks for coordinating exclusive operations across instances: storage compaction, schema migrations, tier-promotion jobs, and partition reassignments.

**Configuration:**

```hocon
udps.integration.uccp.locks {
  uccp-host = ${UDPS_UCCP_HOST}
  uccp-port = ${UDPS_UCCP_PORT}
  namespace = "udps"
  default-ttl-seconds = 30
  renew-interval-seconds = 10
  max-retries = 3
  retry-backoff-ms = 500
}
```

Config class: `LockConfig(uccpHost, uccpPort, namespace, defaultTtlSeconds, renewIntervalSeconds, maxRetries, retryBackoffMs)`

**Types:**

`LockHandle` contains the full lock state:

```scala
case class LockHandle(
  lockToken: String,
  lockName: String,
  owner: String,
  namespace: String,
  acquiredAt: Instant,
  expiresAt: Instant,
  fenceToken: Long
)
```

`LockEvent` is a sealed ADT representing lock state transitions:

```scala
sealed trait LockEvent
case class LockAcquired(handle: LockHandle) extends LockEvent
case class LockReleased(lockName: String)   extends LockEvent
case class LockExpired(lockName: String)     extends LockEvent
case class LockRenewed(handle: LockHandle)   extends LockEvent
```

**Methods:**

| Method | Signature | Description |
|--------|-----------|-------------|
| `acquireLock` | `(name, ttl) => IO[LockHandle]` | Acquire a named lock with TTL |
| `releaseLock` | `(handle) => IO[Unit]` | Release a held lock |
| `renewLock` | `(handle) => IO[LockHandle]` | Extend the TTL of a held lock |
| `tryLock` | `(name, ttl) => IO[Option[LockHandle]]` | Non-blocking acquire attempt |
| `withLock` | `[A](name, ttl)(body: IO[A]) => IO[A]` | Bracket pattern: acquire, execute, release |
| `lockResource` | `(name, ttl) => Resource[IO, LockHandle]` | Lock as a Cats Effect `Resource` with auto-release |
| `renewalStream` | `(handle) => Stream[IO, LockHandle]` | Background stream that renews the lock at `renewIntervalSeconds` |
| `watchLock` | `(name) => Stream[IO, LockEvent]` | Stream of events for a specific lock |
| `queryLockInfo` | `(name) => IO[Option[LockHandle]]` | Query current lock holder without acquiring |

**Fence tokens:** Every lock acquisition returns a monotonically increasing `fenceToken`. UDPS passes this token to storage operations (compaction, tier promotion) so that stale lock holders cannot corrupt data. If a lock expires and is re-acquired by another instance, the new holder receives a higher fence token. Storage operations that present a fence token lower than the current one are rejected.

**Typical usage -- compaction:**

```scala
lockClient.withLock("compaction:partition-42", 60.seconds) {
  storageEngine.compactPartition(partitionId = 42)
}
```

### Health and Metrics

**Classes:** `HealthReporter`, `MetricsPusher`

**HealthReporter** sends periodic health reports to UCCP so that the control plane can track UDPS instance status and trigger alerts or failovers.

**Configuration:**

```hocon
udps.integration.uccp.health {
  report-interval = 30 seconds
  unhealthy-disk-threshold = 0.95
  unhealthy-error-rate-threshold = 0.1
}
```

Config class: `HealthReportingConfig(reportInterval, unhealthyDiskThreshold, unhealthyErrorRateThreshold)`

**Types:**

```scala
sealed trait HealthReportStatus
case object Healthy   extends HealthReportStatus
case object Unhealthy extends HealthReportStatus
case object Degraded  extends HealthReportStatus
```

`HealthDetails` aggregates component-level health into a summary report. `ComponentHealth` represents the health of an individual subsystem (storage, query engine, catalog, etc.).

**Methods:**

| Method | Signature | Description |
|--------|-----------|-------------|
| `reportStream` | `Stream[IO, Unit]` | Background stream that calls `gatherAndSend` at `reportInterval` |
| `gatherAndSend` | `IO[Unit]` | Collect component health, determine overall status, send to UCCP |
| `determineStatus` | `(HealthDetails) => HealthReportStatus` | Derives aggregate status from component health |

`determineStatus` applies these rules:
- If any component reports `Unhealthy`, overall status is `Unhealthy`.
- If disk usage exceeds `unhealthyDiskThreshold` or error rate exceeds `unhealthyErrorRateThreshold`, overall status is `Unhealthy`.
- If any component reports `Degraded` but none are `Unhealthy`, overall status is `Degraded`.
- Otherwise, overall status is `Healthy`.

**MetricsPusher** pushes application metrics to UCCP at a configurable interval.

**Configuration:**

```hocon
udps.integration.uccp.metrics {
  push-interval = 15 seconds
  enabled-prefixes = ["udps.storage", "udps.query", "udps.catalog", "udps.api"]
}
```

Config class: `MetricsPushConfig(pushInterval, enabledPrefixes)`

The `pushStream` is a background FS2 stream that calls `pushOnce` at `pushInterval`. Before sending, it filters metrics to include only those whose names start with one of the configured `enabledPrefixes`. This prevents internal framework metrics from being pushed to UCCP.

### Tracing

**Class:** `TracingPropagator`

**Configuration:**

```hocon
udps.integration.uccp.tracing {
  enabled = true
  sample-rate = 1.0
}
```

Config class: `TracingConfig(enabled, sampleRate)`

`TracingPropagator` implements W3C Trace Context propagation for gRPC calls. It provides a gRPC interceptor (`interceptor(config)`) that:

1. Extracts incoming `traceparent` headers via `parseTraceparent`.
2. If no trace context is present, generates a new one via `generateTraceContext`.
3. Attaches the `traceparent` header to outgoing calls via `formatTraceparent`.

The `traceparent` format follows W3C Trace Context Level 2:

```
00-{traceId: 32 hex chars}-{spanId: 16 hex chars}-{flags: 2 hex chars}
```

Where `flags` encodes the sampling decision (`01` = sampled, `00` = not sampled).

### Load Balancing

**Class:** `LoadBalancer`

`LoadBalancer` provides client-side round-robin load balancing across healthy instances discovered via `ServiceDiscoveryClient`.

**Methods:**

| Method | Signature | Description |
|--------|-----------|-------------|
| `nextInstance` | `IO[ServiceInstance]` | Return the next healthy instance in round-robin order |
| `updateInstances` | `(List[ServiceInstance]) => IO[Unit]` | Replace the instance list (called from watch stream) |
| `markUnhealthy` | `(instanceId) => IO[Unit]` | Remove an instance from the healthy pool |
| `markHealthy` | `(instanceId) => IO[Unit]` | Restore an instance to the healthy pool |

The `ServiceDiscoveryClient.watchStream` feeds instance updates into `updateInstances`, keeping the load balancer's view current.

## USTRP Integration

**Source:** `udps-integration/src/main/scala/io/gbmm/udps/integration/`

UDPS consumes data streams from USTRP-managed Kafka topics. The integration layer provides batched consumption, exactly-once delivery, dead-letter queue handling, schema deserialization, and schema registry access.

### Kafka Consumer

**Class:** `UstrpKafkaConsumer`

The primary consumer implementation. It reads from one or more Kafka topics in batches, processes each batch, commits offsets, and routes failed records to a dead-letter queue (DLQ).

**Configuration:**

```hocon
udps.integration.ustrp.kafka {
  topics = ["udps.ingest.events", "udps.catalog.changes"]
  batch-size = 500
  batch-timeout = 5 seconds
  dlq-topic = "udps.dlq"
  max-poll-records = 1000
  commit-batch-size = 500
}
```

Config class: `KafkaConsumerConfig(topics, batchSize, batchTimeout, dlqTopic, maxPollRecords, commitBatchSize)`

**Methods:**

| Method | Signature | Description |
|--------|-----------|-------------|
| `stream` | `Stream[IO, ConsumerRecord]` | Main consumption stream, batched and committed |
| `processBatch` | `(Chunk[ConsumerRecord]) => IO[Unit]` | Process a batch of records |
| `commitOffsets` | `(offsets) => IO[Unit]` | Commit consumer offsets to Kafka |
| `sendToDlq` | `(record, error) => IO[Unit]` | Send a failed record to the DLQ topic |

**Batching:** Records are accumulated into batches of up to `batchSize` records or until `batchTimeout` elapses, whichever comes first. This balances throughput against latency.

**Dead-letter queue:** When `processBatch` fails for a specific record, the record is forwarded to `dlqTopic` with the following headers attached:

| Header | Description |
|--------|-------------|
| `dlq.error.message` | The exception message |
| `dlq.error.class` | The exception class name |
| `dlq.source.topic` | The original topic the record came from |
| `dlq.source.partition` | The original partition number |
| `dlq.source.offset` | The original offset within the partition |

These headers enable operators to trace DLQ records back to their source and understand the failure.

### Exactly-Once Semantics

**Class:** `ExactlyOnceConsumer`

For workflows that require exactly-once processing guarantees (financial data, audit events), the `ExactlyOnceConsumer` layers three mechanisms on top of the base consumer:

**Configuration:**

```hocon
udps.integration.ustrp.exactly-once {
  transactional-id = "udps-eo-producer-01"
  transaction-timeout = 60 seconds
  deduplicator {
    key-prefix = "udps:dedup:"
    ttl = 24 hours
  }
}
```

Config class: `ExactlyOnceConfig(transactionalId, transactionTimeout, deduplicator: DeduplicatorConfig)`

**Three-layer guarantee:**

1. **Transactional producer** -- The producer is configured with `transactional.id`, enabling Kafka's transactional protocol. Consumed offsets and produced records are committed atomically within a single Kafka transaction.
2. **Idempotent producer** -- The producer has `enable.idempotence=true`, ensuring that retries due to network errors do not produce duplicate records on the output side.
3. **Redis deduplication** -- Before processing a record, the consumer checks Redis (via `RedisDeduplicator`) to determine if the record has already been processed. This handles the case where a transaction commits to Kafka but the consumer crashes before updating its local state.

**Consumer configuration:** `isolation.level=read_committed` ensures the consumer only reads records that were part of a committed transaction.

**Deduplicator:**

**Class:** `RedisDeduplicator`

```hocon
udps.integration.ustrp.dedup {
  key-prefix = "udps:dedup:"
  ttl = 24 hours
}
```

Config class: `DeduplicatorConfig(keyPrefix, ttl)`

| Method | Signature | Description |
|--------|-----------|-------------|
| `isDuplicate` | `(recordId) => IO[Boolean]` | Check Redis with `EXISTS` on `{keyPrefix}{recordId}` |
| `markProcessed` | `(recordId) => IO[Unit]` | Write to Redis with `SETEX` using configured TTL |
| `markProcessedBatch` | `(List[recordId]) => IO[Unit]` | Batch variant using Redis pipeline |

The TTL controls how long deduplication state is retained. Records older than TTL are assumed to be no longer in the Kafka retention window and their dedup keys can be safely expired.

### Schema Registry

**Class:** `HttpSchemaRegistryClient`

UDPS deserializes records from Kafka using schemas fetched from a Confluent Schema Registry instance managed by USTRP.

**Configuration:**

```hocon
udps.integration.ustrp.schema-registry {
  base-url = ${UDPS_SCHEMA_REGISTRY_URL}
  cache-ttl = 1 hour
  max-cache-size = 5000
}
```

Config class: `SchemaRegistryConfig(baseUrl, cacheTtl, maxCacheSize)`

**Methods:**

| Method | Signature | Description |
|--------|-----------|-------------|
| `decode` | `(topic, data) => IO[GenericRecord]` | Decode a Kafka record using its embedded schema ID |
| `getSchema` | `(schemaId) => IO[Schema]` | Fetch a schema by ID (cached) |
| `registerSchema` | `(subject, schema) => IO[SchemaId]` | Register a new schema version |
| `getLatestSchema` | `(subject) => IO[Schema]` | Fetch the latest schema for a subject |

**Message deserialization:**

The `MessageDeserializer` trait has two implementations:

- **`AvroMessageDeserializer`** -- Validates the Confluent wire format: the first byte must be `0x0` (magic byte), followed by a 4-byte big-endian schema ID. The schema ID is used to fetch the schema from the registry. The remaining bytes are decoded as an Avro binary payload.
- **`RawMessageDeserializer`** -- Passes the raw bytes through without transformation. Used for topics that carry opaque payloads (binary blobs, pre-serialized data).

## Cross-Cutting Concerns

### Circuit Breaker

**Class:** `IntegrationCircuitBreaker`

Every gRPC client in the integration module wraps its remote calls through a circuit breaker to prevent cascading failures.

**Configuration:**

```hocon
udps.integration.circuit-breaker {
  max-failures = 5
  call-timeout = 10 seconds
  reset-timeout = 30 seconds
  exponential-backoff-factor = 2.0
  max-reset-timeout = 5 minutes
}
```

Config class: `CircuitBreakerConfig(maxFailures, callTimeout, resetTimeout, exponentialBackoffFactor, maxResetTimeout)`

**State machine:**

```
         success
  +----> Closed ----+
  |      (normal)   |
  |         |       |
  |  failure count  |
  |  >= maxFailures |
  |         |       |
  |         v       |
  |       Open -----+-----> timer expires
  |    (rejecting)         |
  |                        v
  +---- HalfOpen <---------+
        (probe one call)
```

- **Closed** -- All calls pass through. Failures are counted. When `maxFailures` consecutive failures occur within `callTimeout`, the breaker transitions to Open.
- **Open** -- All calls are rejected immediately with a `CircuitBreakerOpenException`. After `resetTimeout` elapses, the breaker transitions to HalfOpen. The `resetTimeout` doubles on each successive Open period (exponential backoff) up to `maxResetTimeout`.
- **HalfOpen** -- A single probe call is allowed through. If it succeeds, the breaker resets to Closed. If it fails, the breaker returns to Open.

**Usage:** All integration clients call `protect[A](call: IO[A]): IO[A]` to wrap their RPCs:

```scala
circuitBreaker.protect(grpcStub.validateToken(request))
```

**Metrics:** The circuit breaker emits metrics on state transitions, success counts, and failure counts. These are picked up by `MetricsPusher` and forwarded to UCCP.

### mTLS

**Class:** `MTLSConfig`

All gRPC channels between UDPS and platform services (USP, UCCP) use mutual TLS with TLS 1.3.

**Configuration:**

```hocon
udps.integration.mtls {
  cert-path = ${UDPS_MTLS_CERT_PATH}
  key-path = ${UDPS_MTLS_KEY_PATH}
  ca-path = ${UDPS_MTLS_CA_PATH}
  enabled = true
}
```

Config class: `MTLSClientConfig(certPath, keyPath, caPath, enabled)`

**Cipher suites (TLS 1.3 only):**

| Cipher | Notes |
|--------|-------|
| `TLS_AES_256_GCM_SHA384` | Primary cipher, AES-256 |
| `TLS_AES_128_GCM_SHA256` | Fallback, AES-128 |
| `TLS_CHACHA20_POLY1305_SHA256` | For environments without AES-NI hardware support |

The `MTLSConfig` validates certificate expiration on construction. If the certificate is expired or will expire within 24 hours, it logs a warning. If the certificate is expired and `enabled = true`, the client fails to start.

### Certificate Reloading

**Class:** `CertificateReloader`

In long-running deployments, certificates are rotated periodically. The `CertificateReloader` watches the filesystem paths configured in `MTLSClientConfig` and rebuilds the TLS context when certificates change.

**Behavior:**

1. Watches `certPath`, `keyPath`, and `caPath` for filesystem modification events.
2. Debounces changes with a 5-second window to avoid reloading during partial writes (certificate rotation tools often write the cert and key in separate operations).
3. On a detected change, attempts to build a new TLS context from the updated files.
4. If the new context builds successfully, it atomically replaces the active context.
5. If the new context fails to build (invalid certificate, mismatched key), it logs an error and retains the old context. The old context continues to serve connections until a valid replacement is loaded.

This ensures zero-downtime certificate rotation without restarting UDPS instances.

## Configuration Reference

All integration environment variables. These are resolved in `application.conf` and loaded into the respective config classes via PureConfig.

### USP

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_USP_HOST` | Yes | -- | USP gRPC hostname |
| `UDPS_USP_PORT` | Yes | -- | USP gRPC port |
| `UDPS_USP_OUTPOST_NAME` | No | `udps` | Name used in outpost registration |
| `UDPS_USP_HEARTBEAT_INTERVAL` | No | `30s` | Outpost heartbeat interval |
| `UDPS_USP_AUTH_CACHE_TTL` | No | `5m` | Authentication cache TTL |
| `UDPS_USP_AUTH_CACHE_MAX_SIZE` | No | `10000` | Authentication cache max entries |
| `UDPS_USP_AUTHZ_CACHE_TTL` | No | `5m` | Authorization cache TTL |
| `UDPS_USP_AUTHZ_CACHE_MAX_SIZE` | No | `50000` | Authorization cache max entries |
| `UDPS_USP_SECRETS_CACHE_TTL` | No | `10m` | Secrets cache TTL |
| `UDPS_USP_SECRETS_CACHE_MAX_SIZE` | No | `1000` | Secrets cache max entries |
| `UDPS_USP_SECRETS_REFRESH_BEFORE_EXPIRY` | No | `2m` | Proactive secret refresh window |

### UCCP

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_UCCP_HOST` | Yes | -- | UCCP gRPC hostname |
| `UDPS_UCCP_PORT` | Yes | -- | UCCP gRPC port |
| `UDPS_UCCP_DISCOVERY_HEARTBEAT_INTERVAL` | No | `15s` | Service discovery heartbeat interval |
| `UDPS_UCCP_LOCK_NAMESPACE` | No | `udps` | Lock namespace for UCCP |
| `UDPS_UCCP_LOCK_DEFAULT_TTL` | No | `30` | Default lock TTL in seconds |
| `UDPS_UCCP_LOCK_RENEW_INTERVAL` | No | `10` | Lock renewal interval in seconds |
| `UDPS_UCCP_LOCK_MAX_RETRIES` | No | `3` | Max lock acquisition retries |
| `UDPS_UCCP_LOCK_RETRY_BACKOFF_MS` | No | `500` | Retry backoff in milliseconds |
| `UDPS_UCCP_HEALTH_REPORT_INTERVAL` | No | `30s` | Health report push interval |
| `UDPS_UCCP_HEALTH_DISK_THRESHOLD` | No | `0.95` | Disk usage threshold for unhealthy status |
| `UDPS_UCCP_HEALTH_ERROR_RATE_THRESHOLD` | No | `0.1` | Error rate threshold for unhealthy status |
| `UDPS_UCCP_METRICS_PUSH_INTERVAL` | No | `15s` | Metrics push interval |
| `UDPS_UCCP_TRACING_ENABLED` | No | `true` | Enable W3C trace context propagation |
| `UDPS_UCCP_TRACING_SAMPLE_RATE` | No | `1.0` | Trace sampling rate (0.0 to 1.0) |

### USTRP

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_USTRP_KAFKA_TOPICS` | Yes | -- | Comma-separated list of Kafka topics to consume |
| `UDPS_USTRP_KAFKA_BATCH_SIZE` | No | `500` | Records per batch |
| `UDPS_USTRP_KAFKA_BATCH_TIMEOUT` | No | `5s` | Max time to wait for a full batch |
| `UDPS_USTRP_KAFKA_DLQ_TOPIC` | No | `udps.dlq` | Dead-letter queue topic |
| `UDPS_USTRP_KAFKA_MAX_POLL_RECORDS` | No | `1000` | Max records per Kafka poll |
| `UDPS_USTRP_KAFKA_COMMIT_BATCH_SIZE` | No | `500` | Offsets committed per batch |
| `UDPS_USTRP_EO_TRANSACTIONAL_ID` | No | `udps-eo-producer-01` | Kafka transactional ID for exactly-once |
| `UDPS_USTRP_EO_TRANSACTION_TIMEOUT` | No | `60s` | Kafka transaction timeout |
| `UDPS_USTRP_DEDUP_KEY_PREFIX` | No | `udps:dedup:` | Redis dedup key prefix |
| `UDPS_USTRP_DEDUP_TTL` | No | `24h` | Redis dedup key TTL |
| `UDPS_SCHEMA_REGISTRY_URL` | Yes | -- | Confluent Schema Registry base URL |
| `UDPS_SCHEMA_REGISTRY_CACHE_TTL` | No | `1h` | Schema cache TTL |
| `UDPS_SCHEMA_REGISTRY_CACHE_MAX_SIZE` | No | `5000` | Schema cache max entries |

### Cross-Cutting

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UDPS_MTLS_CERT_PATH` | Yes | -- | Client certificate path (PEM) |
| `UDPS_MTLS_KEY_PATH` | Yes | -- | Client private key path (PEM) |
| `UDPS_MTLS_CA_PATH` | Yes | -- | CA certificate path (PEM) |
| `UDPS_MTLS_ENABLED` | No | `true` | Enable mTLS for gRPC channels |
| `UDPS_CB_MAX_FAILURES` | No | `5` | Circuit breaker failure threshold |
| `UDPS_CB_CALL_TIMEOUT` | No | `10s` | Circuit breaker call timeout |
| `UDPS_CB_RESET_TIMEOUT` | No | `30s` | Circuit breaker reset timeout |
| `UDPS_CB_BACKOFF_FACTOR` | No | `2.0` | Circuit breaker exponential backoff factor |
| `UDPS_CB_MAX_RESET_TIMEOUT` | No | `5m` | Circuit breaker max reset timeout |

## Troubleshooting Integration Issues

### USP: Token validation fails with UNAUTHENTICATED

1. Verify that `UDPS_USP_HOST` and `UDPS_USP_PORT` are correct and that UDPS can reach USP over the network.
2. Check that the mTLS certificates are valid and not expired. The `CertificateReloader` logs warnings when certificates are near expiry.
3. Confirm that the USP outpost registration succeeded on startup. If registration failed, token validation RPCs may be rejected. Look for `OutpostRegistrationClient` log entries at startup.
4. If the error is intermittent, check the circuit breaker state. When the circuit breaker is open, all validation calls are rejected locally. Look for `CircuitBreakerOpenException` in the logs.

### USP: Authorization cache returns stale decisions

The authorization cache is invalidated when USP signals a policy change via the outpost heartbeat channel. If the heartbeat stream is disconnected, stale decisions may persist until their TTL expires. Check:

1. The outpost heartbeat stream is active (look for heartbeat log entries at `heartbeatInterval`).
2. `UDPS_USP_AUTHZ_CACHE_TTL` is set to an acceptable staleness window. Lower values reduce staleness at the cost of more RPCs to USP.
3. Call `invalidateCache` manually via the admin API if an immediate purge is required.

### UCCP: Service registration fails

1. Verify `UDPS_UCCP_HOST` and `UDPS_UCCP_PORT` connectivity.
2. Ensure the mTLS certificates are valid. UCCP rejects connections with expired or untrusted certificates.
3. Check that the `serviceName` is not already registered by a stale instance. UCCP may reject duplicate registrations if the previous instance did not deregister cleanly. The stale instance will eventually be evicted after its heartbeat TTL expires.

### UCCP: Distributed lock acquisition times out

1. Check if another UDPS instance holds the lock. Use `queryLockInfo` to inspect the current holder.
2. Verify that the lock holder's renewal stream is running. If an instance crashes without releasing its lock, the lock will expire after `defaultTtlSeconds`. Wait for expiration or manually release via UCCP's admin interface.
3. If lock contention is high, consider increasing `defaultTtlSeconds` to reduce renewal overhead, or sharding the locked resource into finer-grained locks.

### USTRP: Consumer lag increases

1. Check the batch processing throughput. If `processBatch` takes longer than `batchTimeout`, the consumer cannot keep up. Consider increasing `batchSize` to amortize per-batch overhead, or scaling horizontally by adding consumer instances.
2. Verify that the DLQ is not backing up. A high DLQ rate indicates a systemic processing failure. Check `dlq.error.message` headers on DLQ records for the root cause.
3. Confirm that the Schema Registry is reachable. If schema lookups fail, deserialization blocks and the consumer stalls.

### USTRP: Exactly-once consumer produces duplicates

1. Verify that `UDPS_USTRP_EO_TRANSACTIONAL_ID` is unique across all consumer instances. Duplicate transactional IDs cause Kafka to fence one of the producers, which may lead to reprocessing.
2. Check Redis connectivity. If the `RedisDeduplicator` cannot reach Redis, deduplication falls back to Kafka transactions only, which do not protect against consumer-side crashes during state updates.
3. Confirm that `UDPS_USTRP_DEDUP_TTL` is at least as long as the Kafka topic retention period. If dedup keys expire before their corresponding records are evicted from Kafka, a consumer group rebalance could reprocess records that are no longer deduplicated.

### Cross-Cutting: Circuit breaker is stuck open

1. Check the health of the target service (USP or UCCP). The circuit breaker opens after `maxFailures` consecutive failures.
2. The breaker automatically transitions to HalfOpen after `resetTimeout`. If it remains open, the probe call in HalfOpen is also failing. This indicates the target service is still unhealthy.
3. The `resetTimeout` doubles on each failure cycle up to `maxResetTimeout`. If the service has been down for an extended period, the reset timeout may be long. Check the circuit breaker metrics for the current reset timeout value.

### Cross-Cutting: Certificate reloading fails

1. The `CertificateReloader` logs errors when it cannot build a new TLS context from updated files. Check logs for the specific error (malformed PEM, mismatched key/cert pair, untrusted CA).
2. Ensure that certificate rotation writes the cert, key, and CA atomically (or within the 5-second debounce window). If the reloader detects a change to the cert file before the key file is updated, it may attempt to load a cert/key mismatch. The reloader retains the old context in this case and will retry on the next file change event.
3. Verify file permissions. The UDPS process must have read access to all three paths (`certPath`, `keyPath`, `caPath`).
