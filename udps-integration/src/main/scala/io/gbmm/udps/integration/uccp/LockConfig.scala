package io.gbmm.udps.integration.uccp

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

final case class LockConfig(
    uccpHost: String,
    uccpPort: Int,
    namespace: String,
    defaultTtlSeconds: Int,
    renewIntervalSeconds: Int,
    maxRetries: Int,
    retryBackoffMs: Long
)

object LockConfig {

  implicit val reader: ConfigReader[LockConfig] = deriveReader[LockConfig]

  private val DefaultTtlSeconds: Int = 30
  private val DefaultRenewIntervalSeconds: Int = 10
  private val DefaultMaxRetries: Int = 3
  private val DefaultRetryBackoffMs: Long = 500L

  def withDefaults(
      uccpHost: String,
      uccpPort: Int,
      namespace: String
  ): LockConfig =
    LockConfig(
      uccpHost = uccpHost,
      uccpPort = uccpPort,
      namespace = namespace,
      defaultTtlSeconds = DefaultTtlSeconds,
      renewIntervalSeconds = DefaultRenewIntervalSeconds,
      maxRetries = DefaultMaxRetries,
      retryBackoffMs = DefaultRetryBackoffMs
    )
}
