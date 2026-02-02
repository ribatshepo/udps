package io.gbmm.udps.integration.uccp

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

// ---------------------------------------------------------------------------
// Nested configurations
// ---------------------------------------------------------------------------

final case class MetricsPushConfig(
    pushInterval: FiniteDuration,
    enabledPrefixes: List[String]
)

object MetricsPushConfig {
  implicit val reader: ConfigReader[MetricsPushConfig] = deriveReader[MetricsPushConfig]
}

final case class TracingConfig(
    enabled: Boolean,
    sampleRate: Double
)

object TracingConfig {

  implicit val reader: ConfigReader[TracingConfig] = deriveReader[TracingConfig]

  def validate(config: TracingConfig): TracingConfig = {
    require(
      config.sampleRate >= 0.0 && config.sampleRate <= 1.0,
      s"tracingSampleRate must be in [0.0, 1.0], got ${config.sampleRate}"
    )
    config
  }
}

// ---------------------------------------------------------------------------
// Top-level observability configuration
// ---------------------------------------------------------------------------

final case class ObservabilityConfig(
    metrics: MetricsPushConfig,
    tracing: TracingConfig,
    uccpMonitoringHost: String,
    uccpMonitoringPort: Int
)

object ObservabilityConfig {

  implicit val reader: ConfigReader[ObservabilityConfig] = deriveReader[ObservabilityConfig]

  /** Validate all sub-configs and return the verified configuration. */
  def validate(config: ObservabilityConfig): ObservabilityConfig = {
    require(
      config.uccpMonitoringHost.nonEmpty,
      "uccpMonitoringHost must not be empty"
    )
    require(
      config.uccpMonitoringPort > 0 && config.uccpMonitoringPort <= 65535,
      s"uccpMonitoringPort must be in [1, 65535], got ${config.uccpMonitoringPort}"
    )
    TracingConfig.validate(config.tracing)
    config
  }
}
