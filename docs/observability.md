# UDPS Observability Integration

## 1. Overview

UDPS pushes metrics, traces, and component health to UCCP's centralized monitoring infrastructure. UCCP serves as the central observability hub, aggregating data from all platform services and exposing it through standard interfaces.

Three pillars form the integration:

| Pillar | Component | Responsibility |
|--------|-----------|----------------|
| Metrics Push | `MetricsPusher` | Periodically collects and pushes metric samples to UCCP's `MonitoringService` over gRPC |
| Distributed Tracing | `TracingPropagator` / `TracingClientInterceptor` | Injects W3C Trace Context headers into all outgoing gRPC calls to UCCP |
| Component Health | `HealthReporter` | Reports per-component health status via UCCP heartbeat RPCs with structured metadata |

UCCP exposes UDPS metrics on its Prometheus endpoint (port 9100), making them available for scraping by Prometheus, Grafana dashboards, and alerting pipelines.

## 2. Architecture

### Metrics Flow

```
UDPS MetricsSource.gatherMetrics
        |
        v
MetricsPusher (fs2 Stream.fixedRate)
        |  filters by enabledPrefixes
        |  wraps in PushMetricsRequest (namespace="udps")
        v
gRPC PushMetrics ──── circuit breaker ────> UCCP MonitoringService
                                                    |
                                                    v
                                            MetricsCollector
                                                    |
                                                    v
                                            Prometheus /metrics (port 9100)
```

### Tracing Flow

```
UDPS outgoing gRPC call
        |
        v
TracingClientInterceptor.interceptCall
        |  generates TraceContext (traceId, spanId, flags)
        |  formats W3C traceparent header
        |  injects into gRPC Metadata
        v
Channel.newCall ──── traceparent header ────> UCCP
                                                |
                                                v
                                             Jaeger
```

### Health Flow

```
HealthCheck.check + HealthCheck.checkComponents
        |
        v
HealthReporter.gatherAndSend
        |  determines aggregate status (HEALTHY / DEGRADED / UNHEALTHY)
        |  formats componentHealth as JSON metadata
        |  builds HeartbeatRequest
        v
HeartbeatSender ──── circuit breaker ────> UCCP ServiceDiscovery.Heartbeat
                                                    |
                                                    v
                                            Service Registry
```

## 3. Configuration Reference

All observability settings live under `udps.api.observability` in `reference.conf`:

```hocon
udps.api {
  observability {
    uccp-monitoring-host = "localhost"
    uccp-monitoring-host = ${?UCCP_MONITORING_HOST}

    uccp-monitoring-port = 9100
    uccp-monitoring-port = ${?UCCP_MONITORING_PORT}

    metrics {
      push-interval = 15s
      enabled-prefixes = ["udps.", "jvm."]
    }

    tracing {
      enabled = true
      enabled = ${?UDPS_TRACING_ENABLED}

      sample-rate = 1.0
      sample-rate = ${?UDPS_TRACING_SAMPLE_RATE}
    }
  }
}
```

### Field Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `uccp-monitoring-host` | `String` | `"localhost"` | UCCP MonitoringService gRPC host |
| `uccp-monitoring-port` | `Int` | `9100` | UCCP MonitoringService gRPC port |
| `metrics.push-interval` | `FiniteDuration` | `15s` | Interval between metric push cycles |
| `metrics.enabled-prefixes` | `List[String]` | `["udps.", "jvm."]` | Only metrics whose names start with one of these prefixes are pushed |
| `tracing.enabled` | `Boolean` | `true` | Master switch for trace context propagation |
| `tracing.sample-rate` | `Double` | `1.0` | Sampling rate in `[0.0, 1.0]`. `1.0` = all traces sampled, `0.0` = none |

### Environment Variable Overrides

| Variable | Config Field | Example |
|----------|-------------|---------|
| `UCCP_MONITORING_HOST` | `uccp-monitoring-host` | `UCCP_MONITORING_HOST=uccp.prod.internal` |
| `UCCP_MONITORING_PORT` | `uccp-monitoring-port` | `UCCP_MONITORING_PORT=9100` |
| `UDPS_TRACING_ENABLED` | `tracing.enabled` | `UDPS_TRACING_ENABLED=false` |
| `UDPS_TRACING_SAMPLE_RATE` | `tracing.sample-rate` | `UDPS_TRACING_SAMPLE_RATE=0.1` |

### Validation

`ObservabilityConfig.validate` enforces at startup:

- `uccpMonitoringHost` must not be empty.
- `uccpMonitoringPort` must be in `[1, 65535]`.
- `tracing.sampleRate` must be in `[0.0, 1.0]`.

## 4. Metrics Push

### Lifecycle

`MetricsPusher` is created as a `Resource[IO, MetricsPusher]`. On acquisition, it starts a background `fs2.Stream` fiber that calls `pushOnce` at `metrics.push-interval` using `Stream.fixedRate[IO]`. On release, the fiber is cancelled.

### Push Cycle

Each push cycle:

1. Calls `MetricsSource.gatherMetrics` to collect all available `MetricSnapshot` values.
2. Filters snapshots to those whose `name` starts with any entry in `metrics.enabled-prefixes`.
3. If no metrics pass the filter, the cycle is skipped with a debug log.
4. Converts matching snapshots to `MetricSample` protobuf messages with the current timestamp.
5. Sends a `PushMetricsRequest` with `sourceId` (the UDPS instance ID), `metricNamespace = "udps"`, and the samples.
6. The call is wrapped in `IntegrationCircuitBreaker.protect` to handle UCCP unavailability.

### MetricsSource Trait

```scala
trait MetricsSource {
  def gatherMetrics: IO[List[MetricSnapshot]]
}
```

Implement this trait to expose custom metrics. The `MetricSnapshot` model:

```scala
final case class MetricSnapshot(
    name: String,        // e.g. "udps.query.latency_ms"
    value: Double,       // current value
    labels: Map[String, String]  // e.g. Map("engine" -> "vectorized")
)
```

### Namespace

All pushed metrics appear under the `"udps"` namespace in UCCP. In Prometheus, these are queryable as `udps_<metric_name>` on UCCP's `/metrics` endpoint.

### Circuit Breaker

The gRPC `PushMetrics` call is protected by `IntegrationCircuitBreaker`. When UCCP is unreachable, the circuit breaker trips to avoid wasting resources on failed calls. Metrics push resumes automatically when the circuit closes.

## 5. Distributed Tracing

### W3C Trace Context

UDPS uses the [W3C Trace Context](https://www.w3.org/TR/trace-context/) `traceparent` header for cross-service trace propagation. The header format:

```
00-{traceId}-{spanId}-{flags}
```

| Field | Format | Description |
|-------|--------|-------------|
| version | `00` | Fixed version identifier |
| traceId | 32 hex chars (128-bit) | Globally unique trace identifier |
| spanId | 16 hex chars (64-bit) | Span identifier for this hop |
| flags | 2 hex chars | `01` = sampled, `00` = not sampled |

### TracingClientInterceptor

`TracingClientInterceptor` is a gRPC `ClientInterceptor` that:

1. Generates a new `TraceContext` with random `traceId` and `spanId` using `ThreadLocalRandom`.
2. Applies the sampling decision: if `random.nextDouble() < config.sampleRate`, sets `traceFlags = 0x01` (sampled).
3. Formats the `traceparent` header value via `TracingPropagator.formatTraceparent`.
4. Injects the header into outgoing gRPC `Metadata` before the call starts.

When `tracing.enabled = false`, the interceptor passes calls through without injecting headers.

### TracingPropagator

Static utility methods on `TracingPropagator`:

| Method | Purpose |
|--------|---------|
| `generateTraceContext(config)` | `IO`-wrapped trace context generation |
| `generateTraceContextSync(config)` | Synchronous generation (used inside interceptors) |
| `formatTraceparent(ctx)` | Formats `TraceContext` to W3C header string |
| `parseTraceparent(header)` | Parses W3C header string to `Option[TraceContext]` |
| `interceptor(config)` | Factory for `TracingClientInterceptor` |

### Sampling

Controlled by `tracing.sample-rate`:

| Value | Behavior |
|-------|----------|
| `1.0` | All traces are sampled (flags = `01`) |
| `0.5` | 50% of traces are sampled |
| `0.0` | No traces are sampled (flags = `00`) |

Sampled traces flow through UCCP to Jaeger, where cross-service spans appear as `UDPS -> UCCP`.

## 6. Component Health

### Components

UDPS reports health for four components:

| Component | Description |
|-----------|-------------|
| `query-engine` | SQL query execution engine status |
| `storage` | Storage layer and tier management |
| `catalog` | Metadata catalog availability |
| `governance` | Data governance and access control |

### HealthCheck Trait

```scala
trait HealthCheck {
  def check: IO[HealthDetails]
  def checkComponents: IO[List[ComponentHealth]] = IO.pure(Nil)
}
```

The `check` method returns service-level metrics (`HealthDetails`). The `checkComponents` method returns per-component status. The default implementation returns an empty list, providing backward compatibility for services that do not implement component-level checks.

### Status Aggregation

`HealthReporter.determineComponentStatus` aggregates component statuses into a single service-level status:

1. If **any** component is `UNHEALTHY`, the service is `UNHEALTHY`.
2. Else if **any** component is `DEGRADED`, the service is `DEGRADED`.
3. Otherwise, the service is `HEALTHY`.

When `checkComponents` returns an empty list, `determineStatus` is used instead, which evaluates `HealthDetails` against configured thresholds (`unhealthyDiskThreshold`, `unhealthyErrorRateThreshold`, database reachability).

### Heartbeat Metadata

Component health details are serialized as JSON and included in the heartbeat metadata under the `"componentHealth"` key:

```json
[
  {"name":"query-engine","status":"HEALTHY"},
  {"name":"storage","status":"HEALTHY"},
  {"name":"catalog","status":"DEGRADED"},
  {"name":"governance","status":"HEALTHY"}
]
```

The full `HeartbeatRequest.metadataUpdates` map includes:

| Key | Value |
|-----|-------|
| `serviceVersion` | UDPS version string |
| `uptimeSeconds` | Seconds since service start |
| `activeQueries` | Current in-flight query count |
| `storageUsagePercent` | Disk usage percentage |
| `errorRate` | Current error rate |
| `databaseReachable` | `"true"` or `"false"` |
| `componentHealth` | JSON array of component statuses |
| `degradedReasons` | Semicolon-separated reasons (when DEGRADED) |
| `unhealthyReasons` | Semicolon-separated reasons (when UNHEALTHY) |

### Backward Compatibility

Services that do not override `checkComponents` continue to work. The default returns `Nil`, causing `HealthReporter` to fall back to `determineStatus(details)` for threshold-based health evaluation. The `componentHealth` key is omitted from metadata when the component list is empty.

## 7. Implementing a MetricsSource

Example implementation exposing query engine metrics:

```scala
package io.gbmm.udps.query.metrics

import cats.effect.{IO, Ref}
import io.gbmm.udps.integration.uccp.{MetricSnapshot, MetricsSource}

final class QueryEngineMetricsSource(
    latencyHistogram: Ref[IO, Double],
    cacheHitCounter: Ref[IO, Long],
    cacheMissCounter: Ref[IO, Long],
    activeQueryGauge: Ref[IO, Int]
) extends MetricsSource {

  override def gatherMetrics: IO[List[MetricSnapshot]] =
    for {
      latency     <- latencyHistogram.get
      cacheHits   <- cacheHitCounter.get
      cacheMisses <- cacheMissCounter.get
      active      <- activeQueryGauge.get
    } yield List(
      MetricSnapshot(
        name = "udps.query.latency_ms",
        value = latency,
        labels = Map("engine" -> "vectorized")
      ),
      MetricSnapshot(
        name = "udps.query.cache_hits_total",
        value = cacheHits.toDouble,
        labels = Map("cache" -> "metadata")
      ),
      MetricSnapshot(
        name = "udps.query.cache_misses_total",
        value = cacheMisses.toDouble,
        labels = Map("cache" -> "metadata")
      ),
      MetricSnapshot(
        name = "udps.query.active_count",
        value = active.toDouble,
        labels = Map.empty
      )
    )
}
```

To compose multiple sources:

```scala
final class CompositeMetricsSource(sources: List[MetricsSource]) extends MetricsSource {
  override def gatherMetrics: IO[List[MetricSnapshot]] =
    sources.traverse(_.gatherMetrics).map(_.flatten)
}
```

Wire the source into `MetricsPusher.resource` during application startup:

```scala
val metricsSource: MetricsSource = new CompositeMetricsSource(List(
  queryEngineMetrics,
  storageMetrics,
  jvmMetrics
))

MetricsPusher.resource(
  channel = monitoringChannel,
  circuitBreaker = circuitBreaker,
  config = observabilityConfig.metrics,
  metricsSource = metricsSource,
  sourceId = instanceId
)
```

## 8. Troubleshooting

### Circuit breaker trips repeatedly

**Symptom:** Logs show `Failed to push metrics` or `Failed to send health report to UCCP` errors, followed by circuit breaker open state.

**Diagnosis:**
1. Verify UCCP is running and reachable from the UDPS host.
2. Check `uccp-monitoring-host` and `uccp-monitoring-port` match the actual UCCP MonitoringService address.
3. Inspect network connectivity: `nc -zv <host> <port>`.
4. Review UCCP logs for gRPC server errors.

**Fix:** Correct the host/port configuration via `UCCP_MONITORING_HOST` and `UCCP_MONITORING_PORT` environment variables. The circuit breaker will auto-close once UCCP becomes reachable.

### No metrics visible in UCCP Prometheus endpoint

**Symptom:** UCCP `/metrics` endpoint does not show any `udps_*` metrics.

**Diagnosis:**
1. Verify `metrics.enabled-prefixes` includes the prefix of your metric names (default: `["udps.", "jvm."]`).
2. Confirm your `MetricsSource.gatherMetrics` returns a non-empty list.
3. Check UDPS logs for `"No metrics matched enabled prefixes, skipping push"` messages.
4. Verify the `push-interval` has elapsed since startup.

**Fix:** Ensure metric names use the `udps.` or `jvm.` prefix, or add the required prefix to `enabled-prefixes`. Verify the `MetricsSource` implementation returns data.

### Traces not appearing in Jaeger

**Symptom:** Jaeger UI does not show UDPS spans or cross-service traces to UCCP.

**Diagnosis:**
1. Confirm `tracing.enabled = true` (check `UDPS_TRACING_ENABLED` env var).
2. Confirm `tracing.sample-rate > 0.0` (check `UDPS_TRACING_SAMPLE_RATE` env var).
3. Look for `"Injected traceparent"` debug log messages in UDPS.
4. Verify UCCP is forwarding traces to Jaeger.

**Fix:** Set `UDPS_TRACING_ENABLED=true` and `UDPS_TRACING_SAMPLE_RATE=1.0` for full trace capture. Reduce the sample rate only after confirming traces flow end-to-end.

### Component health not included in heartbeat

**Symptom:** Heartbeat metadata in UCCP service registry does not contain the `componentHealth` key.

**Diagnosis:**
1. Verify your `HealthCheck` implementation overrides `checkComponents` and returns a non-empty list.
2. The default `checkComponents` returns `Nil`, which causes `HealthReporter` to omit the key entirely.
3. Check UDPS logs for the health report cycle output.

**Fix:** Override `checkComponents` in your `HealthCheck` implementation to return `List[ComponentHealth]` entries for each component. Example:

```scala
override def checkComponents: IO[List[ComponentHealth]] =
  IO.pure(List(
    ComponentHealth("query-engine", HealthReportStatus.Healthy, Map("version" -> "1.0")),
    ComponentHealth("storage", HealthReportStatus.Healthy, Map("tier" -> "hot")),
    ComponentHealth("catalog", HealthReportStatus.Healthy, Map.empty),
    ComponentHealth("governance", HealthReportStatus.Healthy, Map.empty)
  ))
```

### Health report shows DEGRADED or UNHEALTHY unexpectedly

**Symptom:** Service status is DEGRADED or UNHEALTHY despite all components appearing functional.

**Diagnosis:**
1. When `checkComponents` returns a non-empty list, component-level aggregation takes precedence over threshold-based evaluation.
2. A single UNHEALTHY component causes the entire service to report UNHEALTHY.
3. Check `unhealthyReasons` or `degradedReasons` in heartbeat metadata for specifics.

**Fix:** Investigate the component reporting the degraded status. The reason strings in the metadata identify which component and why.
