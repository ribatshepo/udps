package io.gbmm.udps.integration.usp

import cats.effect.{IO, Ref}
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.integration.circuitbreaker.IntegrationCircuitBreaker
import io.grpc.ManagedChannel
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import usp.secrets.secrets._

import java.time.{Duration, Instant}
import scala.concurrent.duration.FiniteDuration

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

final case class SecretsConfig(
    cacheTtl: FiniteDuration,
    refreshBeforeExpiry: FiniteDuration,
    maxCacheSize: Int
)

object SecretsConfig {
  implicit val reader: ConfigReader[SecretsConfig] = deriveReader[SecretsConfig]
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

final case class SecretValue(
    name: String,
    value: String,
    version: Int,
    expiresAt: Option[Instant]
) {
  override def toString: String =
    s"SecretValue(name=$name, value=***, version=$version, expiresAt=$expiresAt)"
}

private[usp] final case class CachedSecret(
    secret: SecretValue,
    cachedAt: Instant
)

// ---------------------------------------------------------------------------
// Exceptions
// ---------------------------------------------------------------------------

final class SecretNotFoundException(val secretName: String)
    extends RuntimeException(s"Secret not found: $secretName")

final class SecretServiceException(
    val errorCode: String,
    val serverMessage: String
) extends RuntimeException(
      s"Secrets service error: errorCode=$errorCode, message=$serverMessage"
    )

// ---------------------------------------------------------------------------
// SecretsClient
// ---------------------------------------------------------------------------

final class SecretsClient private[usp] (
    channel: ManagedChannel,
    circuitBreaker: IntegrationCircuitBreaker,
    config: SecretsConfig,
    cacheRef: Ref[IO, Map[String, CachedSecret]]
) extends LazyLogging {

  private val stub: SecretsServiceGrpc.SecretsServiceStub =
    SecretsServiceGrpc.stub(channel)

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /** Retrieve a secret by name, using cache with TTL and proactive refresh. */
  def getSecret(name: String): IO[SecretValue] =
    for {
      now    <- IO(Instant.now())
      cache  <- cacheRef.get
      result <- cache.get(name) match {
                  case Some(cached) if isValid(cached, now) =>
                    proactiveRefreshIfNeeded(cached, now).as(cached.secret)
                  case _ =>
                    fetchAndCache(name)
                }
    } yield result

  /** Force refresh a secret, bypassing cache. Used on 401 Unauthorized. */
  def refreshSecret(name: String): IO[SecretValue] = {
    logger.info("Forcing secret refresh for path={}", name)
    fetchAndCache(name)
  }

  /** Encrypt data using the transit engine. */
  def encrypt(data: Array[Byte], keyId: String): IO[Array[Byte]] = {
    val request = EncryptRequest(
      keyName = keyId,
      plaintext = ByteString.copyFrom(data)
    )

    val rpc: IO[EncryptResponse] =
      IO.fromFuture(IO(stub.encrypt(request)))

    circuitBreaker.protect(rpc).flatMap { response =>
      validateResponse(response.response, "encrypt").as {
        response.ciphertext.getBytes(java.nio.charset.StandardCharsets.UTF_8)
      }
    }
  }

  /** Decrypt data using the transit engine. */
  def decrypt(data: Array[Byte], keyId: String): IO[Array[Byte]] = {
    val ciphertextStr = new String(data, java.nio.charset.StandardCharsets.UTF_8)
    val request = DecryptRequest(
      keyName = keyId,
      ciphertext = ciphertextStr
    )

    val rpc: IO[DecryptResponse] =
      IO.fromFuture(IO(stub.decrypt(request)))

    circuitBreaker.protect(rpc).flatMap { response =>
      validateResponse(response.response, "decrypt").as {
        response.plaintext.toByteArray
      }
    }
  }

  /** List all secret keys at the configured path. */
  def listSecrets: IO[List[String]] = {
    val request = ListSecretsRequest()

    val rpc: IO[ListSecretsResponse] =
      IO.fromFuture(IO(stub.listSecrets(request)))

    circuitBreaker.protect(rpc).flatMap { response =>
      validateResponse(response.response, "listSecrets").as {
        response.keys.toList
      }
    }
  }

  // -------------------------------------------------------------------------
  // Internal
  // -------------------------------------------------------------------------

  private def isValid(cached: CachedSecret, now: Instant): Boolean = {
    val ttlMillis = config.cacheTtl.toMillis
    val elapsed   = Duration.between(cached.cachedAt, now).toMillis
    elapsed < ttlMillis
  }

  private def needsProactiveRefresh(cached: CachedSecret, now: Instant): Boolean = {
    val ttlMillis              = config.cacheTtl.toMillis
    val refreshBeforeMillis    = config.refreshBeforeExpiry.toMillis
    val elapsed                = Duration.between(cached.cachedAt, now).toMillis
    val remainingMillis        = ttlMillis - elapsed
    remainingMillis <= refreshBeforeMillis
  }

  private def proactiveRefreshIfNeeded(cached: CachedSecret, now: Instant): IO[Unit] =
    if (needsProactiveRefresh(cached, now)) {
      fetchAndCache(cached.secret.name)
        .void
        .handleErrorWith { err =>
          IO(logger.warn(
            "Proactive refresh failed for secret path={}: {}",
            cached.secret.name,
            err.getMessage
          ))
        }
        .start
        .void
    } else {
      IO.unit
    }

  private def fetchAndCache(name: String): IO[SecretValue] = {
    val request = GetSecretRequest(path = name)

    val rpc: IO[GetSecretResponse] =
      IO.fromFuture(IO(stub.getSecret(request)))

    circuitBreaker.protect(rpc).flatMap { response =>
      validateResponse(response.response, "getSecret") *>
        parseAndCacheResponse(name, response)
    }
  }

  private def parseAndCacheResponse(
      name: String,
      response: GetSecretResponse
  ): IO[SecretValue] = {
    val secretData = response.data
    val secretValue = SecretValue(
      name = name,
      value = secretData.getOrElse("value", ""),
      version = response.version,
      expiresAt = response.createdAt.map { ts =>
        Instant.ofEpochSecond(ts.seconds, ts.nanos.toLong)
          .plusMillis(config.cacheTtl.toMillis)
      }
    )

    val cached = CachedSecret(
      secret = secretValue,
      cachedAt = Instant.now()
    )

    cacheRef.modify { cache =>
      val updatedCache = if (cache.size >= config.maxCacheSize && !cache.contains(name)) {
        evictOldest(cache) + (name -> cached)
      } else {
        cache + (name -> cached)
      }
      (updatedCache, secretValue)
    }.flatTap { _ =>
      IO(logger.debug("Cached secret for path={}, version={}", name, secretValue.version.toString))
    }
  }

  private def evictOldest(cache: Map[String, CachedSecret]): Map[String, CachedSecret] =
    if (cache.isEmpty) {
      cache
    } else {
      val oldest = cache.minBy(_._2.cachedAt)
      cache - oldest._1
    }

  private def validateResponse(
      grpcResponse: Option[usp.common.common.GrpcResponse],
      operation: String
  ): IO[Unit] =
    grpcResponse match {
      case Some(resp) if resp.success =>
        IO.unit
      case Some(resp) =>
        IO.raiseError(
          new SecretServiceException(
            errorCode = resp.errorCode,
            serverMessage = resp.message
          )
        )
      case None =>
        IO(logger.debug("No response envelope for operation={}, treating as success", operation))
    }
}

// ---------------------------------------------------------------------------
// Companion -- factory
// ---------------------------------------------------------------------------

object SecretsClient extends LazyLogging {

  def create(
      channel: ManagedChannel,
      circuitBreaker: IntegrationCircuitBreaker,
      config: SecretsConfig
  ): IO[SecretsClient] =
    for {
      cacheRef <- Ref.of[IO, Map[String, CachedSecret]](Map.empty)
    } yield {
      logger.info(
        "Created SecretsClient: cacheTtl={}, refreshBeforeExpiry={}, maxCacheSize={}",
        config.cacheTtl.toString,
        config.refreshBeforeExpiry.toString,
        config.maxCacheSize.toString
      )
      new SecretsClient(channel, circuitBreaker, config, cacheRef)
    }
}
