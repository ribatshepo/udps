package io.gbmm.udps.integration.circuitbreaker

import akka.actor.ActorSystem
import akka.pattern.{CircuitBreaker => AkkaCircuitBreaker}
import cats.effect.{IO, Resource}
import cats.effect.std.Dispatcher
import com.typesafe.scalalogging.LazyLogging
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import java.time.Instant
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.concurrent.duration.{Duration, FiniteDuration}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

final case class CircuitBreakerConfig(
    maxFailures: Int,
    callTimeout: FiniteDuration,
    resetTimeout: FiniteDuration,
    exponentialBackoffFactor: Double,
    maxResetTimeout: FiniteDuration
)

object CircuitBreakerConfig {
  implicit val reader: ConfigReader[CircuitBreakerConfig] = deriveReader[CircuitBreakerConfig]
}

// ---------------------------------------------------------------------------
// State ADT
// ---------------------------------------------------------------------------

sealed trait CircuitBreakerState extends Product with Serializable

object CircuitBreakerState {
  case object Closed   extends CircuitBreakerState
  case object Open     extends CircuitBreakerState
  case object HalfOpen extends CircuitBreakerState
}

// ---------------------------------------------------------------------------
// Event ADT
// ---------------------------------------------------------------------------

sealed trait CircuitBreakerEvent extends Product with Serializable

object CircuitBreakerEvent {
  final case class StateTransition(
      from: CircuitBreakerState,
      to: CircuitBreakerState,
      timestamp: Instant
  ) extends CircuitBreakerEvent

  final case class CallSuccess(duration: FiniteDuration) extends CircuitBreakerEvent
  final case class CallFailure(duration: FiniteDuration, error: Throwable) extends CircuitBreakerEvent
  final case class CircuitOpened(failureCount: Long) extends CircuitBreakerEvent
}

// ---------------------------------------------------------------------------
// Metrics trait
// ---------------------------------------------------------------------------

trait CircuitBreakerMetrics {
  def record(event: CircuitBreakerEvent): Unit
}

// ---------------------------------------------------------------------------
// Default metrics implementation (atomic counters, lock-free)
// ---------------------------------------------------------------------------

final class DefaultCircuitBreakerMetrics(name: String) extends CircuitBreakerMetrics with LazyLogging {

  private val _stateTransitions = new AtomicLong(0L)
  private val _successCount     = new AtomicLong(0L)
  private val _failureCount     = new AtomicLong(0L)
  private val _currentState     = new AtomicReference[CircuitBreakerState](CircuitBreakerState.Closed)

  def stateTransitions: Long            = _stateTransitions.get()
  def successCount: Long                = _successCount.get()
  def failureCount: Long                = _failureCount.get()
  def currentState: CircuitBreakerState = _currentState.get()

  override def record(event: CircuitBreakerEvent): Unit = event match {
    case CircuitBreakerEvent.StateTransition(from, to, ts) =>
      val _ = _stateTransitions.incrementAndGet()
      _currentState.set(to)
      logger.info(s"CircuitBreaker[$name] state transition: $from -> $to at $ts")

    case CircuitBreakerEvent.CallSuccess(dur) =>
      val _ = _successCount.incrementAndGet()
      logger.debug(s"CircuitBreaker[$name] call succeeded in ${dur.toMillis}ms")

    case CircuitBreakerEvent.CallFailure(dur, error) =>
      val _ = _failureCount.incrementAndGet()
      logger.warn(s"CircuitBreaker[$name] call failed in ${dur.toMillis}ms: ${error.getMessage}")

    case CircuitBreakerEvent.CircuitOpened(count) =>
      logger.error(s"CircuitBreaker[$name] OPENED after $count failures -- rejecting calls")
  }
}

// ---------------------------------------------------------------------------
// Circuit breaker exception for open-state rejections
// ---------------------------------------------------------------------------

final class CircuitBreakerOpenException(val name: String)
    extends RuntimeException(s"CircuitBreaker[$name] is open -- call rejected")

// ---------------------------------------------------------------------------
// IntegrationCircuitBreaker
// ---------------------------------------------------------------------------

final class IntegrationCircuitBreaker private[circuitbreaker] (
    val name: String,
    private val underlying: AkkaCircuitBreaker,
    val metrics: DefaultCircuitBreakerMetrics,
    private val dispatcher: Dispatcher[IO]
) extends LazyLogging {

  /** Protect an arbitrary IO[A] call with this circuit breaker. */
  def protect[A](call: IO[A]): IO[A] =
    for {
      start  <- IO(System.nanoTime())
      result <- IO
                  .fromFuture(IO {
                    val future = dispatcher.unsafeToFuture(call)
                    underlying.callWithCircuitBreaker(() => future)
                  })
                  .flatTap(_ => recordSuccess(start))
                  .handleErrorWith(err => recordFailure(start, err) *> IO.raiseError(err))
    } yield result

  private def recordSuccess(startNanos: Long): IO[Unit] =
    IO {
      val elapsed = elapsedSince(startNanos)
      metrics.record(CircuitBreakerEvent.CallSuccess(elapsed))
    }

  private def recordFailure(startNanos: Long, error: Throwable): IO[Unit] =
    IO {
      val elapsed = elapsedSince(startNanos)
      metrics.record(CircuitBreakerEvent.CallFailure(elapsed, error))
    }

  private def elapsedSince(startNanos: Long): FiniteDuration = {
    val nanos = Math.max(0L, System.nanoTime() - startNanos)
    Duration.fromNanos(nanos)
  }
}

// ---------------------------------------------------------------------------
// Companion -- factory via Resource[IO, *]
// ---------------------------------------------------------------------------

object IntegrationCircuitBreaker extends LazyLogging {

  def create(
      config: CircuitBreakerConfig,
      name: String
  ): Resource[IO, IntegrationCircuitBreaker] = {

    val acquireSystem: Resource[IO, ActorSystem] =
      Resource.make(
        IO(ActorSystem(s"circuit-breaker-${name.replaceAll("[^a-zA-Z0-9-]", "-")}"))
      )(sys => IO.fromFuture(IO(sys.terminate())).void)

    val acquireDispatcher: Resource[IO, Dispatcher[IO]] =
      Dispatcher.parallel[IO]

    for {
      system <- acquireSystem
      disp   <- acquireDispatcher
      cb     <- Resource.eval(buildBreaker(config, name, system, disp))
    } yield cb
  }

  private def buildBreaker(
      config: CircuitBreakerConfig,
      name: String,
      system: ActorSystem,
      disp: Dispatcher[IO]
  ): IO[IntegrationCircuitBreaker] = IO {

    val metrics = new DefaultCircuitBreakerMetrics(name)

    val akkaBreaker = new AkkaCircuitBreaker(
      scheduler = system.scheduler,
      maxFailures = config.maxFailures,
      callTimeout = config.callTimeout,
      resetTimeout = config.resetTimeout,
      maxResetTimeout = config.maxResetTimeout,
      exponentialBackoffFactor = config.exponentialBackoffFactor
    )(system.dispatcher)

    val failureCounter = new AtomicLong(0L)

    registerCallbacks(akkaBreaker, name, metrics, failureCounter)

    new IntegrationCircuitBreaker(
      name = name,
      underlying = akkaBreaker,
      metrics = metrics,
      dispatcher = disp
    )
  }

  private def registerCallbacks(
      breaker: AkkaCircuitBreaker,
      name: String,
      metrics: DefaultCircuitBreakerMetrics,
      failureCounter: AtomicLong
  ): Unit = {
    val _ = breaker
      .addOnOpenListener(() => {
        val count = failureCounter.get()
        metrics.record(CircuitBreakerEvent.CircuitOpened(count))
        metrics.record(
          CircuitBreakerEvent.StateTransition(
            from = CircuitBreakerState.Closed,
            to = CircuitBreakerState.Open,
            timestamp = Instant.now()
          )
        )
      })
      .addOnHalfOpenListener(() => {
        metrics.record(
          CircuitBreakerEvent.StateTransition(
            from = CircuitBreakerState.Open,
            to = CircuitBreakerState.HalfOpen,
            timestamp = Instant.now()
          )
        )
      })
      .addOnCloseListener(() => {
        failureCounter.set(0L)
        metrics.record(
          CircuitBreakerEvent.StateTransition(
            from = CircuitBreakerState.HalfOpen,
            to = CircuitBreakerState.Closed,
            timestamp = Instant.now()
          )
        )
      })
      .addOnCallSuccessListener((_: Long) => {
        failureCounter.set(0L)
        logger.debug(s"CircuitBreaker[$name] underlying call succeeded")
      })
      .addOnCallFailureListener((_: Long) => {
        val _ = failureCounter.incrementAndGet()
        logger.debug(s"CircuitBreaker[$name] underlying call failed")
      })
      .addOnCallBreakerOpenListener(() => {
        logger.warn(s"CircuitBreaker[$name] call rejected -- breaker is open")
      })
      .addOnCallTimeoutListener((_: Long) => {
        val _ = failureCounter.incrementAndGet()
        logger.warn(s"CircuitBreaker[$name] call timed out")
      })
  }
}
