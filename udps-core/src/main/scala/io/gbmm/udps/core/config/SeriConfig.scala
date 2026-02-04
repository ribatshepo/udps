package io.gbmm.udps.core.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

// ---------------------------------------------------------------------------
// TLS configuration for integration endpoints
// ---------------------------------------------------------------------------

final case class IntegrationTlsConfig(
  enabled: Boolean,
  certPath: String,
  keyPath: String,
  caPath: String
)

// ---------------------------------------------------------------------------
// Circuit breaker configuration for integration endpoints
// ---------------------------------------------------------------------------

final case class IntegrationCircuitBreakerConfig(
  enabled: Boolean,
  failureThreshold: Int,
  resetTimeoutMs: Long,
  halfOpenMaxCalls: Int
)

// ---------------------------------------------------------------------------
// Retry configuration for integration endpoints
// ---------------------------------------------------------------------------

final case class IntegrationRetryConfig(
  maxAttempts: Int,
  backoffMs: Long
)

// ---------------------------------------------------------------------------
// Per-integration endpoint configuration (UCCP, USP)
// ---------------------------------------------------------------------------

final case class IntegrationEndpointConfig(
  enabled: Boolean,
  host: String,
  port: Int,
  tls: IntegrationTlsConfig,
  circuitBreaker: IntegrationCircuitBreakerConfig,
  timeoutMs: Long,
  retry: IntegrationRetryConfig
)

// ---------------------------------------------------------------------------
// Integrations block
// ---------------------------------------------------------------------------

final case class IntegrationsConfig(
  uccp: IntegrationEndpointConfig,
  usp: IntegrationEndpointConfig
)

// ---------------------------------------------------------------------------
// Top-level seri configuration
// ---------------------------------------------------------------------------

final case class SeriConfig(
  mode: String,
  integrations: IntegrationsConfig
) {

  /** Returns true when running in standalone mode. */
  def isStandalone: Boolean = mode == SeriConfig.ModeStandalone

  /** Returns true when running in platform mode. */
  def isPlatform: Boolean = mode == SeriConfig.ModePlatform

  /** Returns the effective endpoint config, respecting standalone mode.
    *
    * In standalone mode, all integrations are forced to disabled regardless
    * of the configured value, unless the environment variable explicitly
    * enabled them (which overrides the default through HOCON substitution).
    */
  def effectiveUccp: IntegrationEndpointConfig =
    if (isStandalone) integrations.uccp.copy(enabled = false)
    else integrations.uccp

  def effectiveUsp: IntegrationEndpointConfig =
    if (isStandalone) integrations.usp.copy(enabled = false)
    else integrations.usp
}

object SeriConfig {

  val ModeStandalone: String = "standalone"
  val ModePlatform: String   = "platform"

  private val ValidModes: Set[String] = Set(ModeStandalone, ModePlatform)

  implicit val integrationTlsReader: ConfigReader[IntegrationTlsConfig] =
    deriveReader[IntegrationTlsConfig]

  implicit val integrationCbReader: ConfigReader[IntegrationCircuitBreakerConfig] =
    deriveReader[IntegrationCircuitBreakerConfig]

  implicit val integrationRetryReader: ConfigReader[IntegrationRetryConfig] =
    deriveReader[IntegrationRetryConfig]

  implicit val endpointReader: ConfigReader[IntegrationEndpointConfig] =
    deriveReader[IntegrationEndpointConfig]

  implicit val integrationsReader: ConfigReader[IntegrationsConfig] =
    deriveReader[IntegrationsConfig]

  implicit val reader: ConfigReader[SeriConfig] =
    deriveReader[SeriConfig]

  /** Validate the loaded configuration, returning the config if valid. */
  def validate(config: SeriConfig): SeriConfig = {
    require(
      ValidModes.contains(config.mode),
      s"seri.mode must be one of ${ValidModes.mkString(", ")}, got '${config.mode}'"
    )
    validateEndpoint("uccp", config.integrations.uccp)
    validateEndpoint("usp", config.integrations.usp)
    config
  }

  private val MinPort: Int = 1
  private val MaxPort: Int = 65535

  private def validateEndpoint(name: String, ep: IntegrationEndpointConfig): Unit = {
    require(
      ep.host.nonEmpty,
      s"seri.integrations.$name.host must not be empty"
    )
    require(
      ep.port >= MinPort && ep.port <= MaxPort,
      s"seri.integrations.$name.port must be in [$MinPort, $MaxPort], got ${ep.port}"
    )
    require(
      ep.timeoutMs > 0,
      s"seri.integrations.$name.timeout-ms must be positive, got ${ep.timeoutMs}"
    )
    require(
      ep.retry.maxAttempts >= 1,
      s"seri.integrations.$name.retry.max-attempts must be >= 1, got ${ep.retry.maxAttempts}"
    )
    require(
      ep.retry.backoffMs >= 0,
      s"seri.integrations.$name.retry.backoff-ms must be non-negative, got ${ep.retry.backoffMs}"
    )
    if (ep.circuitBreaker.enabled) {
      require(
        ep.circuitBreaker.failureThreshold >= 1,
        s"seri.integrations.$name.circuit-breaker.failure-threshold must be >= 1, got ${ep.circuitBreaker.failureThreshold}"
      )
      require(
        ep.circuitBreaker.resetTimeoutMs > 0,
        s"seri.integrations.$name.circuit-breaker.reset-timeout-ms must be positive, got ${ep.circuitBreaker.resetTimeoutMs}"
      )
      require(
        ep.circuitBreaker.halfOpenMaxCalls >= 1,
        s"seri.integrations.$name.circuit-breaker.half-open-max-calls must be >= 1, got ${ep.circuitBreaker.halfOpenMaxCalls}"
      )
    }
  }
}
