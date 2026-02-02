package io.gbmm.udps.integration.usp

import cats.effect.{IO, Ref}
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.integration.circuitbreaker.IntegrationCircuitBreaker
import io.grpc.ManagedChannel
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import usp.authentication.authentication.{
  AuthenticationServiceGrpc,
  GetUserContextRequest,
  RefreshTokenRequest,
  RevokeSessionRequest,
  ValidateTokenRequest
}

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

final case class AuthenticationConfig(
    cacheTtl: FiniteDuration,
    maxCacheSize: Int
)

object AuthenticationConfig {
  implicit val reader: ConfigReader[AuthenticationConfig] = deriveReader[AuthenticationConfig]
}

// ---------------------------------------------------------------------------
// Authentication context
// ---------------------------------------------------------------------------

final case class AuthenticationContext(
    userId: String,
    roles: List[String],
    permissions: List[String],
    sessionId: String,
    tokenExpiresAt: Option[Instant]
)

// ---------------------------------------------------------------------------
// Cached entry
// ---------------------------------------------------------------------------

private[usp] final case class CachedAuthContext(
    context: AuthenticationContext,
    cachedAt: Instant
)

// ---------------------------------------------------------------------------
// Authentication failure
// ---------------------------------------------------------------------------

final class AuthenticationException(
    val errorCode: String,
    val serverMessage: String
) extends RuntimeException(
      s"Authentication failed: errorCode=$errorCode, message=$serverMessage"
    )

// ---------------------------------------------------------------------------
// AuthenticationClient
// ---------------------------------------------------------------------------

final class AuthenticationClient private[usp] (
    channel: ManagedChannel,
    circuitBreaker: IntegrationCircuitBreaker,
    config: AuthenticationConfig,
    cacheRef: Ref[IO, Map[String, CachedAuthContext]]
) extends LazyLogging {

  private val stub: AuthenticationServiceGrpc.AuthenticationServiceStub =
    AuthenticationServiceGrpc.stub(channel)

  private val sanitizedTokenLength: Int = 8

  /**
   * Validate a JWT token. Returns cached context if available and not expired,
   * otherwise calls the ValidateToken RPC and caches the result.
   */
  def validateToken(jwt: String): IO[AuthenticationContext] =
    getCachedContext(jwt).flatMap {
      case Some(ctx) =>
        IO(logger.debug("Token cache hit for token ending ...{}", tokenSuffix(jwt))).as(ctx)
      case None =>
        validateTokenRemote(jwt)
    }

  /**
   * Refresh an expired token using a refresh token.
   */
  def refreshToken(refreshTokenValue: String): IO[AuthenticationContext] = {
    val request = RefreshTokenRequest(refreshToken = refreshTokenValue)

    val rpc: IO[AuthenticationContext] =
      IO.fromFuture(IO(stub.refreshToken(request))).flatMap { response =>
        val grpcResponse = response.response
        val isSuccess = grpcResponse.exists(_.success)
        if (isSuccess) {
          val expiresAt = response.accessTokenExpiresAt.map { ts =>
            Instant.ofEpochSecond(ts.seconds, ts.nanos.toLong)
          }
          // Use an empty sessionId since RefreshTokenResponse doesn't carry it
          IO.pure(
            AuthenticationContext(
              userId = "",
              roles = List.empty,
              permissions = List.empty,
              sessionId = "",
              tokenExpiresAt = expiresAt
            )
          )
        } else {
          val errCode = grpcResponse.map(_.errorCode).getOrElse("UNKNOWN")
          val errMsg = grpcResponse.map(_.message).getOrElse("Token refresh failed")
          IO.raiseError(new AuthenticationException(errorCode = errCode, serverMessage = errMsg))
        }
      }

    circuitBreaker.protect(rpc).flatTap { ctx =>
      IO(logger.info("Token refreshed successfully"))
        .as(ctx)
    }
  }

  /**
   * Revoke a session, removing it from the cache as well.
   */
  def revokeSession(sessionId: String): IO[Unit] = {
    val request = RevokeSessionRequest(sessionId = sessionId)

    val rpc: IO[Unit] =
      IO.fromFuture(IO(stub.revokeSession(request))).void

    circuitBreaker.protect(rpc).flatMap { _ =>
      evictBySessionId(sessionId).flatMap { evicted =>
        IO(logger.info(
          "Session revoked: sessionId={}, cacheEntriesEvicted={}",
          sessionId,
          evicted.toString
        ))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Internal - remote validation
  // ---------------------------------------------------------------------------

  private def validateTokenRemote(jwt: String): IO[AuthenticationContext] = {
    val request = ValidateTokenRequest(token = jwt)

    val rpc: IO[AuthenticationContext] =
      IO.fromFuture(IO(stub.validateToken(request))).flatMap { response =>
        if (response.isValid) {
          val expiresAt = response.expiresAt.map { ts =>
            Instant.ofEpochSecond(ts.seconds, ts.nanos.toLong)
          }
          fetchUserContext(response.userId, expiresAt).map { ctx =>
            ctx.copy(
              roles = if (ctx.roles.isEmpty) response.roles.toList else ctx.roles,
              permissions = if (ctx.permissions.isEmpty) response.permissions.toList else ctx.permissions
            )
          }
        } else {
          IO.raiseError(
            new AuthenticationException(
              errorCode = response.errorCode,
              serverMessage = response.message
            )
          )
        }
      }

    circuitBreaker.protect(rpc).flatTap { ctx =>
      cacheContext(jwt, ctx).flatMap { _ =>
        IO(logger.debug(
          "Token validated and cached for userId={}, token ending ...{}",
          ctx.userId,
          tokenSuffix(jwt)
        ))
      }
    }
  }

  private def fetchUserContext(
      userId: String,
      tokenExpiresAt: Option[Instant]
  ): IO[AuthenticationContext] = {
    val request = GetUserContextRequest(userId = userId)

    IO.fromFuture(IO(stub.getUserContext(request))).map { response =>
      val userData = response.user
      AuthenticationContext(
        userId = userData.map(_.userId).getOrElse(userId),
        roles = userData.map(_.roles.toList).getOrElse(List.empty),
        permissions = userData.map(_.permissions.toList).getOrElse(List.empty),
        sessionId = "",
        tokenExpiresAt = tokenExpiresAt
      )
    }
  }

  // ---------------------------------------------------------------------------
  // Internal - cache operations
  // ---------------------------------------------------------------------------

  private def getCachedContext(jwt: String): IO[Option[AuthenticationContext]] =
    for {
      cache <- cacheRef.get
      now   <- IO(Instant.now())
      result = cache.get(jwt).flatMap { cached =>
        val ttlNanos  = config.cacheTtl.toNanos
        val elapsed   = java.time.Duration.between(cached.cachedAt, now).toNanos
        val expired   = elapsed >= ttlNanos

        val tokenExpired = cached.context.tokenExpiresAt.exists(exp => now.isAfter(exp))

        if (expired || tokenExpired) None
        else Some(cached.context)
      }
      _ <- if (result.isEmpty && cache.contains(jwt)) evictKey(jwt) else IO.unit
    } yield result

  private def cacheContext(jwt: String, ctx: AuthenticationContext): IO[Unit] =
    for {
      now <- IO(Instant.now())
      entry = CachedAuthContext(context = ctx, cachedAt = now)
      _ <- cacheRef.update { cache =>
        val pruned = if (cache.size >= config.maxCacheSize) evictExpiredEntries(cache, now) else cache
        val trimmed =
          if (pruned.size >= config.maxCacheSize) pruned - pruned.minBy(_._2.cachedAt)._1
          else pruned
        trimmed.updated(jwt, entry)
      }
    } yield ()

  private def evictKey(key: String): IO[Unit] =
    cacheRef.update(_ - key)

  private def evictBySessionId(sessionId: String): IO[Int] =
    cacheRef.modify { cache =>
      val toRemove = cache.filter { case (_, cached) => cached.context.sessionId == sessionId }
      (cache -- toRemove.keys, toRemove.size)
    }

  private def evictExpiredEntries(
      cache: Map[String, CachedAuthContext],
      now: Instant
  ): Map[String, CachedAuthContext] = {
    val ttlNanos = config.cacheTtl.toNanos
    cache.filter { case (_, cached) =>
      val elapsed      = java.time.Duration.between(cached.cachedAt, now).toNanos
      val ttlValid     = elapsed < ttlNanos
      val tokenValid   = cached.context.tokenExpiresAt.forall(exp => now.isBefore(exp))
      ttlValid && tokenValid
    }
  }

  // ---------------------------------------------------------------------------
  // Internal - logging helpers
  // ---------------------------------------------------------------------------

  private def tokenSuffix(token: String): String =
    if (token.length > sanitizedTokenLength) token.takeRight(sanitizedTokenLength)
    else "***"
}

// ---------------------------------------------------------------------------
// Companion
// ---------------------------------------------------------------------------

object AuthenticationClient extends LazyLogging {

  def create(
      channel: ManagedChannel,
      cb: IntegrationCircuitBreaker,
      config: AuthenticationConfig
  ): IO[AuthenticationClient] =
    for {
      cache <- Ref.of[IO, Map[String, CachedAuthContext]](Map.empty)
    } yield {
      logger.info(
        "Created AuthenticationClient: cacheTtl={}, maxCacheSize={}",
        config.cacheTtl.toString,
        config.maxCacheSize.toString
      )
      new AuthenticationClient(channel, cb, config, cache)
    }
}
