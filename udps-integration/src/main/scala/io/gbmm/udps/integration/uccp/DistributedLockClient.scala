package io.gbmm.udps.integration.uccp

import cats.effect.{IO, Resource}
import cats.effect.std.Queue
import cats.syntax.all._
import com.google.protobuf.duration.{Duration => ProtoDuration}
import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.LazyLogging
import fs2.Stream
import io.gbmm.udps.integration.circuitbreaker.IntegrationCircuitBreaker
import io.grpc.ManagedChannel
import uccp.coordination.coordination._

import java.time.Instant

// ---------------------------------------------------------------------------
// Domain model
// ---------------------------------------------------------------------------

final case class LockHandle(
    lockToken: String,
    lockName: String,
    owner: String,
    namespace: String,
    acquiredAt: Instant,
    expiresAt: Instant,
    fenceToken: Long
)

sealed trait LockEvent extends Product with Serializable

object LockEvent {
  final case class LockAcquired(lockName: String, owner: String, fenceToken: Long, expiresAt: Instant)
      extends LockEvent
  final case class LockReleased(lockName: String, owner: String)          extends LockEvent
  final case class LockExpired(lockName: String, owner: String)           extends LockEvent
  final case class LockRenewed(lockName: String, owner: String, expiresAt: Instant) extends LockEvent
}

// ---------------------------------------------------------------------------
// Exceptions
// ---------------------------------------------------------------------------

final class LockAcquisitionException(val lockName: String, val reason: String)
    extends RuntimeException(s"Failed to acquire lock '$lockName': $reason")

final class LockNotAcquiredException(val lockName: String)
    extends RuntimeException(s"Lock '$lockName' was not acquired -- another holder exists")

final class LockReleaseException(val lockName: String, val reason: String)
    extends RuntimeException(s"Failed to release lock '$lockName': $reason")

final class LockRenewalException(val lockName: String, val reason: String)
    extends RuntimeException(s"Failed to renew lock '$lockName': $reason")

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

final class DistributedLockClient private (
    channel: ManagedChannel,
    circuitBreaker: IntegrationCircuitBreaker,
    config: LockConfig
) extends LazyLogging {

  private[uccp] val stub: LockServiceGrpc.LockServiceStub =
    new LockServiceGrpc.LockServiceStub(channel)

  private val fenceTokenCounter = new java.util.concurrent.atomic.AtomicLong(0L)

  /** Acquire a distributed lock, blocking until acquired or the server-side wait timeout expires. */
  def acquireLock(lockName: String, owner: String, ttlSeconds: Int): IO[LockHandle] =
    circuitBreaker.protect {
      val request = AcquireLockRequest(
        lockName = lockName,
        namespace = config.namespace,
        owner = owner,
        ttl = Some(ProtoDuration(seconds = ttlSeconds.toLong)),
        waitTimeout = Some(ProtoDuration(seconds = ttlSeconds.toLong))
      )
      IO.fromFuture(IO(stub.acquireLock(request))).flatMap { response =>
        if (response.acquired) {
          val handle = toHandle(lockName, owner, response.lockToken, response.expiresAt)
          IO {
            logger.info(
              s"Acquired lock '$lockName' for owner='$owner', " +
                s"token=${handle.lockToken}, fenceToken=${handle.fenceToken}, expiresAt=${handle.expiresAt}"
            )
          }.as(handle)
        } else {
          IO.raiseError(new LockNotAcquiredException(lockName))
        }
      }
    }

  /** Release a previously acquired lock. */
  def releaseLock(handle: LockHandle): IO[Unit] =
    circuitBreaker.protect {
      val request = ReleaseLockRequest(
        lockName = handle.lockName,
        namespace = handle.namespace,
        lockToken = handle.lockToken
      )
      IO.fromFuture(IO(stub.releaseLock(request))).flatMap { response =>
        if (response.success) {
          IO(logger.info(s"Released lock '${handle.lockName}' for owner='${handle.owner}', token=${handle.lockToken}"))
        } else {
          val reason = response.message
          IO.raiseError(new LockReleaseException(handle.lockName, reason))
        }
      }
    }

  /** Renew the TTL of an existing lock. */
  def renewLock(handle: LockHandle, ttlSeconds: Int): IO[LockHandle] =
    circuitBreaker.protect {
      val request = RenewLockRequest(
        lockName = handle.lockName,
        namespace = handle.namespace,
        lockToken = handle.lockToken,
        ttl = Some(ProtoDuration(seconds = ttlSeconds.toLong))
      )
      IO.fromFuture(IO(stub.renewLock(request))).flatMap { response =>
        if (response.success) {
          val newExpiresAt = Instant.now().plusSeconds(ttlSeconds.toLong)
          val renewed = handle.copy(expiresAt = newExpiresAt)
          IO {
            logger.info(
              s"Renewed lock '${handle.lockName}' for owner='${handle.owner}', " +
                s"newExpiresAt=$newExpiresAt"
            )
          }.as(renewed)
        } else {
          val reason = response.message
          IO.raiseError(new LockRenewalException(handle.lockName, reason))
        }
      }
    }

  /** Try to acquire a lock without blocking; returns None if not acquired within timeoutMs. */
  def tryLock(lockName: String, owner: String, ttlSeconds: Int, timeoutMs: Long): IO[Option[LockHandle]] =
    circuitBreaker.protect {
      val request = TryLockRequest(
        lockName = lockName,
        namespace = config.namespace,
        owner = owner,
        ttl = Some(ProtoDuration(seconds = ttlSeconds.toLong))
      )
      IO.fromFuture(IO(stub.tryLock(request))).map { response =>
        if (response.acquired) {
          val handle = toHandle(lockName, owner, response.lockToken, response.expiresAt)
          logger.info(
            s"TryLock acquired lock '$lockName' for owner='$owner', " +
              s"token=${handle.lockToken}, fenceToken=${handle.fenceToken}"
          )
          Some(handle)
        } else {
          logger.debug(s"TryLock failed for lock '$lockName', owner='$owner' -- lock not available")
          None
        }
      }
    }.timeoutTo(
      scala.concurrent.duration.FiniteDuration(timeoutMs, scala.concurrent.duration.MILLISECONDS),
      IO {
        logger.debug(s"TryLock timed out for lock '$lockName', owner='$owner' after ${timeoutMs}ms")
        None
      }
    )

  /**
   * Execute an action while holding a lock, with automatic renewal and guaranteed release.
   * The lock is acquired before the action runs and released when the action completes
   * (whether by success, failure, or cancellation).
   */
  def withLock[A](lockName: String, owner: String, ttlSeconds: Int)(action: IO[A]): IO[A] =
    lockResource(lockName, owner, ttlSeconds).use(_ => action)

  /** Expose the lock lifecycle as a cats-effect Resource for composability. */
  def lockResource(lockName: String, owner: String, ttlSeconds: Int): Resource[IO, LockHandle] =
    Resource.make(acquireLock(lockName, owner, ttlSeconds)) { handle =>
      releaseLock(handle).handleErrorWith { err =>
        IO(logger.error(
          s"Failed to release lock '${handle.lockName}' on resource finalization: ${err.getMessage}",
          err
        ))
      }
    }

  /**
   * Automatic renewal stream that periodically renews a lock handle.
   * Emits LockRenewed events on each successful renewal. Terminates on renewal failure.
   */
  def renewalStream(handle: LockHandle, ttlSeconds: Int): Stream[IO, LockEvent] = {
    val intervalSeconds = config.renewIntervalSeconds
    Stream
      .awakeEvery[IO](scala.concurrent.duration.FiniteDuration(intervalSeconds.toLong, scala.concurrent.duration.SECONDS))
      .evalMapAccumulate(handle) { case (currentHandle, _) =>
        renewLock(currentHandle, ttlSeconds).map { renewed =>
          val event: LockEvent = LockEvent.LockRenewed(
            lockName = renewed.lockName,
            owner = renewed.owner,
            expiresAt = renewed.expiresAt
          )
          (renewed, event)
        }
      }
      .map(_._2)
  }

  /**
   * Watch for lock state changes by polling the lock list endpoint.
   * Emits LockEvent instances reflecting observed state transitions.
   */
  def watchLock(lockName: String): Stream[IO, LockEvent] =
    Stream.eval(Queue.unbounded[IO, Option[LockEvent]]).flatMap { queue =>
      val pollInterval = scala.concurrent.duration.FiniteDuration(
        config.renewIntervalSeconds.toLong,
        scala.concurrent.duration.SECONDS
      )

      val poller: Stream[IO, Unit] =
        Stream
          .awakeEvery[IO](pollInterval)
          .evalMapAccumulate(Option.empty[LockInfo]) { case (previousState, _) =>
            queryLockInfo(lockName).flatMap { currentState =>
              val events = deriveEvents(lockName, previousState, currentState)
              events.traverse_(evt => queue.offer(Some(evt))).as((currentState, ()))
            }
          }
          .map(_._2)
          .handleErrorWith { err =>
            Stream.eval(
              IO(logger.error(s"Watch stream error for lock '$lockName': ${err.getMessage}", err)) *>
                queue.offer(None)
            ).drain
          }

      Stream.fromQueueNoneTerminated(queue).concurrently(poller)
    }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private def queryLockInfo(lockName: String): IO[Option[LockInfo]] =
    circuitBreaker.protect {
      val request = ListLocksRequest(namespace = config.namespace)
      IO.fromFuture(IO(stub.listLocks(request))).map { response =>
        response.locks.find(_.lockName == lockName)
      }
    }

  private def deriveEvents(
      lockName: String,
      previous: Option[LockInfo],
      current: Option[LockInfo]
  ): List[LockEvent] =
    (previous, current) match {
      case (None, Some(info)) =>
        List(LockEvent.LockAcquired(
          lockName = lockName,
          owner = info.owner,
          fenceToken = fenceTokenCounter.incrementAndGet(),
          expiresAt = toInstant(info.expiresAt)
        ))
      case (Some(prev), None) =>
        val prevExpires = toInstant(prev.expiresAt)
        if (Instant.now().isAfter(prevExpires)) {
          List(LockEvent.LockExpired(lockName = lockName, owner = prev.owner))
        } else {
          List(LockEvent.LockReleased(lockName = lockName, owner = prev.owner))
        }
      case (Some(prev), Some(curr)) if prev.owner != curr.owner =>
        List(
          LockEvent.LockReleased(lockName = lockName, owner = prev.owner),
          LockEvent.LockAcquired(
            lockName = lockName,
            owner = curr.owner,
            fenceToken = fenceTokenCounter.incrementAndGet(),
            expiresAt = toInstant(curr.expiresAt)
          )
        )
      case (Some(prev), Some(curr)) if toInstant(prev.expiresAt) != toInstant(curr.expiresAt) =>
        List(LockEvent.LockRenewed(
          lockName = lockName,
          owner = curr.owner,
          expiresAt = toInstant(curr.expiresAt)
        ))
      case _ =>
        List.empty
    }

  private def toHandle(
      lockName: String,
      owner: String,
      lockToken: String,
      expiresAt: Option[Timestamp]
  ): LockHandle = {
    val now = Instant.now()
    val expiry = expiresAt
      .map(ts => Instant.ofEpochSecond(ts.seconds, ts.nanos.toLong))
      .getOrElse(now.plusSeconds(config.defaultTtlSeconds.toLong))
    LockHandle(
      lockToken = lockToken,
      lockName = lockName,
      owner = owner,
      namespace = config.namespace,
      acquiredAt = now,
      expiresAt = expiry,
      fenceToken = fenceTokenCounter.incrementAndGet()
    )
  }

  private def toInstant(ts: Option[Timestamp]): Instant =
    ts.map(t => Instant.ofEpochSecond(t.seconds, t.nanos.toLong))
      .getOrElse(Instant.EPOCH)
}

// ---------------------------------------------------------------------------
// Companion -- Resource factory
// ---------------------------------------------------------------------------

object DistributedLockClient extends LazyLogging {

  def resource(
      channel: ManagedChannel,
      circuitBreaker: IntegrationCircuitBreaker,
      config: LockConfig
  ): Resource[IO, DistributedLockClient] =
    Resource.make(
      IO {
        logger.info(
          s"Creating DistributedLockClient for namespace='${config.namespace}' " +
            s"targeting ${config.uccpHost}:${config.uccpPort}"
        )
        new DistributedLockClient(channel, circuitBreaker, config)
      }
    ) { _ =>
      IO(logger.info("Shutting down DistributedLockClient"))
    }
}
