package io.gbmm.udps.integration.usp

import cats.effect.{IO, Ref}
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.integration.circuitbreaker.IntegrationCircuitBreaker
import io.grpc.ManagedChannel
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import usp.authorization.authorization.{
  AuthorizationServiceGrpc,
  CheckPermissionRequest,
  CheckPermissionsRequest,
  EvaluatePolicyRequest,
  GetUserPermissionsRequest,
  GetUserRolesRequest,
  PermissionCheck
}

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

final case class AuthorizationConfig(
    cacheTtl: FiniteDuration,
    maxCacheSize: Int
)

object AuthorizationConfig {
  implicit val reader: ConfigReader[AuthorizationConfig] = deriveReader[AuthorizationConfig]
}

// ---------------------------------------------------------------------------
// Authorization decision ADT
// ---------------------------------------------------------------------------

sealed trait AuthorizationDecision extends Product with Serializable

object AuthorizationDecision {
  case object Allowed extends AuthorizationDecision
  final case class Denied(reason: String) extends AuthorizationDecision
}

// ---------------------------------------------------------------------------
// Authorization request
// ---------------------------------------------------------------------------

final case class AuthorizationRequest(
    userId: String,
    resource: String,
    action: String,
    attributes: Map[String, String]
)

// ---------------------------------------------------------------------------
// Cache internals
// ---------------------------------------------------------------------------

private[usp] final case class CacheKey(
    userId: String,
    resource: String,
    action: String
)

private[usp] final case class CachedDecision(
    decision: AuthorizationDecision,
    cachedAt: Instant
)

// ---------------------------------------------------------------------------
// AuthorizationClient
// ---------------------------------------------------------------------------

final class AuthorizationClient private[usp] (
    channel: ManagedChannel,
    circuitBreaker: IntegrationCircuitBreaker,
    config: AuthorizationConfig,
    cacheRef: Ref[IO, Map[CacheKey, CachedDecision]]
) extends LazyLogging {

  private val stub: AuthorizationServiceGrpc.AuthorizationServiceStub =
    AuthorizationServiceGrpc.stub(channel)

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  def authorize(request: AuthorizationRequest): IO[AuthorizationDecision] = {
    val key = CacheKey(request.userId, request.resource, request.action)
    lookupCache(key).flatMap {
      case Some(decision) =>
        IO(logger.debug("Cache hit for authorization: userId={}, resource={}, action={}",
          request.userId, request.resource, request.action)) *>
          IO.pure(decision)
      case None =>
        callCheckPermission(request).flatTap(d => storeInCache(key, d))
    }
  }

  def authorizeAll(
      requests: List[AuthorizationRequest]
  ): IO[List[(AuthorizationRequest, AuthorizationDecision)]] = {
    val keyed = requests.map { req =>
      val key = CacheKey(req.userId, req.resource, req.action)
      (req, key)
    }

    for {
      cacheSnapshot <- cacheRef.get
      now           <- IO(Instant.now())
      ttlMillis      = config.cacheTtl.toMillis
      partitioned    = keyed.map { case (req, key) =>
        cacheSnapshot.get(key).filter(e => isValid(e, now, ttlMillis)) match {
          case Some(entry) => Left((req, entry.decision))
          case None        => Right(req)
        }
      }
      alreadyCached  = partitioned.collect { case Left(pair) => pair }
      toFetch        = partitioned.collect { case Right(req) => req }
      fetched       <- batchCheckPermissions(toFetch)
      _             <- storeBatchInCache(fetched)
    } yield alreadyCached ++ fetched
  }

  def evaluatePolicy(
      userId: String,
      policyId: String,
      context: Map[String, String]
  ): IO[AuthorizationDecision] = {
    val request = EvaluatePolicyRequest(
      policyName = policyId,
      userId = userId,
      attributes = context
    )

    val rpc: IO[AuthorizationDecision] =
      IO.fromFuture(IO(stub.evaluatePolicy(request))).map { response =>
        if (response.isAllowed) AuthorizationDecision.Allowed
        else AuthorizationDecision.Denied(response.reason)
      }

    circuitBreaker.protect(rpc).flatTap { decision =>
      IO(logger.info("Policy evaluation: userId={}, policyId={}, decision={}",
        userId, policyId, decision.toString))
    }
  }

  def getUserPermissions(userId: String): IO[List[String]] = {
    val request = GetUserPermissionsRequest(userId = userId)
    val rpc: IO[List[String]] =
      IO.fromFuture(IO(stub.getUserPermissions(request))).map(_.permissions.toList)

    circuitBreaker.protect(rpc).flatTap { permissions =>
      IO(logger.debug("Retrieved {} permissions for userId={}", permissions.size.toString, userId))
    }
  }

  def getUserRoles(userId: String): IO[List[String]] = {
    val request = GetUserRolesRequest(userId = userId)
    val rpc: IO[List[String]] =
      IO.fromFuture(IO(stub.getUserRoles(request))).map(_.roles.map(_.name).toList)

    circuitBreaker.protect(rpc).flatTap { roles =>
      IO(logger.debug("Retrieved {} roles for userId={}", roles.size.toString, userId))
    }
  }

  def invalidateCache: IO[Unit] =
    cacheRef.set(Map.empty).flatTap(_ =>
      IO(logger.info("Authorization decision cache invalidated"))
    )

  def invalidateCacheForUser(userId: String): IO[Unit] =
    cacheRef.update(_.filterNot { case (key, _) => key.userId == userId }).flatTap(_ =>
      IO(logger.info("Authorization decision cache invalidated for userId={}", userId))
    )

  // -------------------------------------------------------------------------
  // Internal — single permission check
  // -------------------------------------------------------------------------

  private def callCheckPermission(
      request: AuthorizationRequest
  ): IO[AuthorizationDecision] = {
    val resourceParts = parseResource(request.resource)
    val grpcRequest = CheckPermissionRequest(
      userId = request.userId,
      resourceType = resourceParts._1,
      resourceId = resourceParts._2,
      action = request.action,
      context = request.attributes
    )

    val rpc: IO[AuthorizationDecision] =
      IO.fromFuture(IO(stub.checkPermission(grpcRequest))).map { response =>
        if (response.isAuthorized) AuthorizationDecision.Allowed
        else AuthorizationDecision.Denied(response.reason)
      }

    circuitBreaker.protect(rpc).flatTap { decision =>
      IO(logger.debug("Authorization check: userId={}, resource={}, action={}, decision={}",
        request.userId, request.resource, request.action, decision.toString))
    }
  }

  // -------------------------------------------------------------------------
  // Internal — batch permission check
  // -------------------------------------------------------------------------

  private def batchCheckPermissions(
      requests: List[AuthorizationRequest]
  ): IO[List[(AuthorizationRequest, AuthorizationDecision)]] =
    if (requests.isEmpty) IO.pure(List.empty)
    else {
      val groupedByUser = requests.groupBy(_.userId)
      groupedByUser.toList.flatTraverse { case (userId, userRequests) =>
        val checks = userRequests.zipWithIndex.map { case (req, idx) =>
          val resourceParts = parseResource(req.resource)
          PermissionCheck(
            checkId = idx.toString,
            resourceType = resourceParts._1,
            resourceId = resourceParts._2,
            action = req.action,
            context = req.attributes
          )
        }

        val grpcRequest = CheckPermissionsRequest(
          userId = userId,
          checks = checks
        )

        val rpc: IO[List[(AuthorizationRequest, AuthorizationDecision)]] =
          IO.fromFuture(IO(stub.checkPermissions(grpcRequest))).map { response =>
            val resultMap = response.results.map(r => r.checkId -> r).toMap
            userRequests.zipWithIndex.map { case (req, idx) =>
              val decision = resultMap.get(idx.toString) match {
                case Some(result) if result.isAuthorized => AuthorizationDecision.Allowed
                case Some(result)                        => AuthorizationDecision.Denied(result.reason)
                case None                                => AuthorizationDecision.Denied("No result returned for check")
              }
              (req, decision)
            }
          }

        circuitBreaker.protect(rpc)
      }
    }

  // -------------------------------------------------------------------------
  // Internal — cache operations
  // -------------------------------------------------------------------------

  private def lookupCache(key: CacheKey): IO[Option[AuthorizationDecision]] =
    for {
      cache <- cacheRef.get
      now   <- IO(Instant.now())
      ttlMs  = config.cacheTtl.toMillis
    } yield cache.get(key).filter(e => isValid(e, now, ttlMs)).map(_.decision)

  private def storeInCache(key: CacheKey, decision: AuthorizationDecision): IO[Unit] =
    for {
      now <- IO(Instant.now())
      entry = CachedDecision(decision, now)
      _   <- cacheRef.update { cache =>
        val evicted = if (cache.size >= config.maxCacheSize) evictOldest(cache) else cache
        evicted.updated(key, entry)
      }
    } yield ()

  private def storeBatchInCache(
      results: List[(AuthorizationRequest, AuthorizationDecision)]
  ): IO[Unit] =
    if (results.isEmpty) IO.unit
    else
      for {
        now <- IO(Instant.now())
        _   <- cacheRef.update { cache =>
          val newEntries = results.map { case (req, decision) =>
            CacheKey(req.userId, req.resource, req.action) -> CachedDecision(decision, now)
          }.toMap
          val merged = cache ++ newEntries
          if (merged.size > config.maxCacheSize) trimCache(merged, config.maxCacheSize)
          else merged
        }
      } yield ()

  private def isValid(entry: CachedDecision, now: Instant, ttlMillis: Long): Boolean = {
    val elapsed = java.time.Duration.between(entry.cachedAt, now).toMillis
    elapsed < ttlMillis
  }

  private def evictOldest(
      cache: Map[CacheKey, CachedDecision]
  ): Map[CacheKey, CachedDecision] =
    if (cache.isEmpty) cache
    else {
      val instantOrdering: Ordering[Instant] = _ compareTo _
      val oldest = cache.minBy(_._2.cachedAt)(instantOrdering)
      cache - oldest._1
    }

  private def trimCache(
      cache: Map[CacheKey, CachedDecision],
      maxSize: Int
  ): Map[CacheKey, CachedDecision] = {
    val instantOrdering: Ordering[Instant] = _ compareTo _
    val sorted = cache.toList.sortBy(_._2.cachedAt)(instantOrdering.reverse)
    sorted.take(maxSize).toMap
  }

  // -------------------------------------------------------------------------
  // Internal — resource parsing
  // -------------------------------------------------------------------------

  private def parseResource(resource: String): (String, String) = {
    val parts = resource.split("/", 2)
    if (parts.length >= 2) (parts(0), parts(1))
    else (resource, "")
  }
}

// ---------------------------------------------------------------------------
// Companion — factory
// ---------------------------------------------------------------------------

object AuthorizationClient extends LazyLogging {

  def create(
      channel: ManagedChannel,
      circuitBreaker: IntegrationCircuitBreaker,
      config: AuthorizationConfig
  ): IO[AuthorizationClient] =
    Ref.of[IO, Map[CacheKey, CachedDecision]](Map.empty).map { cacheRef =>
      logger.info("Created AuthorizationClient: cacheTtl={}, maxCacheSize={}",
        config.cacheTtl.toString, config.maxCacheSize.toString)
      new AuthorizationClient(channel, circuitBreaker, config, cacheRef)
    }
}
