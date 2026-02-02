package io.gbmm.udps.integration.uccp

import cats.effect.{IO, Resource}
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import fs2.Stream
import io.gbmm.udps.integration.circuitbreaker.IntegrationCircuitBreaker
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import uccp.common.common.{HealthStatus => ProtoHealthStatus}
import uccp.coordination.coordination.HeartbeatRequest

import scala.concurrent.duration.FiniteDuration

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

final case class HealthReportingConfig(
    reportInterval: FiniteDuration,
    unhealthyDiskThreshold: Double,
    unhealthyErrorRateThreshold: Double
)

object HealthReportingConfig {
  implicit val reader: ConfigReader[HealthReportingConfig] = deriveReader[HealthReportingConfig]
}

// ---------------------------------------------------------------------------
// Domain model
// ---------------------------------------------------------------------------

sealed trait HealthReportStatus extends Product with Serializable

object HealthReportStatus {
  case object Healthy                                extends HealthReportStatus
  final case class Unhealthy(reasons: List[String])  extends HealthReportStatus
  final case class Degraded(reasons: List[String])   extends HealthReportStatus
}

final case class HealthDetails(
    serviceVersion: String,
    uptimeSeconds: Long,
    activeQueries: Int,
    storageUsagePercent: Double,
    errorRate: Double,
    databaseReachable: Boolean
)

// ---------------------------------------------------------------------------
// HealthCheck trait -- abstraction for gathering health metrics
// ---------------------------------------------------------------------------

trait HealthCheck {
  def check: IO[HealthDetails]
}

// ---------------------------------------------------------------------------
// HeartbeatSender -- abstraction over the UCCP heartbeat RPC
// ---------------------------------------------------------------------------

trait HeartbeatSender {
  def sendHeartbeat(request: HeartbeatRequest): IO[Unit]
}

object HeartbeatSender {

  /** Build a HeartbeatSender from a ServiceDiscoveryClient and circuit breaker.
    *
    * Accesses the gRPC stub via the client's existing heartbeat channel.
    * The caller is responsible for providing the registered serviceId.
    */
  def fromClient(
      client: ServiceDiscoveryClient,
      circuitBreaker: IntegrationCircuitBreaker
  ): HeartbeatSender =
    new HeartbeatSender {
      override def sendHeartbeat(request: HeartbeatRequest): IO[Unit] =
        circuitBreaker.protect {
          IO.fromFuture(IO(client.stub.heartbeat(request))).void
        }
    }
}

// ---------------------------------------------------------------------------
// HealthReporter
// ---------------------------------------------------------------------------

final class HealthReporter private (
    heartbeatSender: HeartbeatSender,
    config: HealthReportingConfig,
    healthCheck: HealthCheck,
    serviceIdProvider: IO[String]
) extends LazyLogging {

  private val unhealthyDiskThreshold: Double    = config.unhealthyDiskThreshold
  private val unhealthyErrorRateThreshold: Double = config.unhealthyErrorRateThreshold

  /** Evaluate health rules against the gathered details. */
  def determineStatus(details: HealthDetails): HealthReportStatus = {
    val reasons = List.newBuilder[String]

    if (!details.databaseReachable)
      reasons += "Database unreachable"
    if (details.storageUsagePercent > unhealthyDiskThreshold)
      reasons += s"Disk usage ${details.storageUsagePercent}% exceeds threshold ${unhealthyDiskThreshold}%"
    if (details.errorRate > unhealthyErrorRateThreshold)
      reasons += s"Error rate ${details.errorRate}% exceeds threshold ${unhealthyErrorRateThreshold}%"

    val built = reasons.result()
    if (built.isEmpty) HealthReportStatus.Healthy
    else if (!details.databaseReachable) HealthReportStatus.Unhealthy(built)
    else HealthReportStatus.Degraded(built)
  }

  /** Periodic health reporting stream using fs2 fixedRate. */
  def reportStream: Stream[IO, Unit] =
    Stream
      .fixedRate[IO](config.reportInterval)
      .evalMap(_ => reportOnce)

  /** Execute a single health report cycle. */
  private def reportOnce: IO[Unit] =
    for {
      serviceId <- serviceIdProvider
      _         <- if (serviceId.isEmpty)
                     IO(logger.debug("Skipping health report -- no registered serviceId"))
                   else
                     gatherAndSend(serviceId)
    } yield ()

  private def gatherAndSend(serviceId: String): IO[Unit] =
    for {
      details <- healthCheck.check
      status   = determineStatus(details)
      _       <- IO(logStatus(status, details))
      _       <- sendReport(serviceId, status, details)
    } yield ()

  private def sendReport(
      serviceId: String,
      status: HealthReportStatus,
      details: HealthDetails
  ): IO[Unit] = {
    val protoHealth = status match {
      case HealthReportStatus.Healthy       => ProtoHealthStatus.HEALTH_STATUS_HEALTHY
      case HealthReportStatus.Degraded(_)   => ProtoHealthStatus.HEALTH_STATUS_DEGRADED
      case HealthReportStatus.Unhealthy(_)  => ProtoHealthStatus.HEALTH_STATUS_UNHEALTHY
    }

    val metadata = Map(
      "serviceVersion"      -> details.serviceVersion,
      "uptimeSeconds"       -> details.uptimeSeconds.toString,
      "activeQueries"       -> details.activeQueries.toString,
      "storageUsagePercent" -> f"${details.storageUsagePercent}%.2f",
      "errorRate"           -> f"${details.errorRate}%.4f",
      "databaseReachable"   -> details.databaseReachable.toString
    ) ++ statusReasons(status)

    val request = HeartbeatRequest(
      serviceId = serviceId,
      health = protoHealth,
      metadataUpdates = metadata
    )

    heartbeatSender.sendHeartbeat(request).handleErrorWith { err =>
      IO(logger.error(s"Failed to send health report to UCCP: ${err.getMessage}", err))
    }
  }

  private def statusReasons(status: HealthReportStatus): Map[String, String] =
    status match {
      case HealthReportStatus.Healthy          => Map.empty
      case HealthReportStatus.Degraded(rs)     => Map("degradedReasons" -> rs.mkString("; "))
      case HealthReportStatus.Unhealthy(rs)    => Map("unhealthyReasons" -> rs.mkString("; "))
    }

  private def logStatus(status: HealthReportStatus, details: HealthDetails): Unit =
    status match {
      case HealthReportStatus.Healthy =>
        logger.debug(
          s"Health report: HEALTHY " +
            s"(version=${details.serviceVersion}, uptime=${details.uptimeSeconds}s, " +
            s"queries=${details.activeQueries}, disk=${details.storageUsagePercent}%, " +
            s"errorRate=${details.errorRate}%)"
        )
      case HealthReportStatus.Degraded(reasons) =>
        logger.warn(
          s"Health report: DEGRADED reasons=[${reasons.mkString(", ")}] " +
            s"(version=${details.serviceVersion}, uptime=${details.uptimeSeconds}s)"
        )
      case HealthReportStatus.Unhealthy(reasons) =>
        logger.error(
          s"Health report: UNHEALTHY reasons=[${reasons.mkString(", ")}] " +
            s"(version=${details.serviceVersion}, uptime=${details.uptimeSeconds}s)"
        )
    }

  /** Start the health reporting stream as a background fiber.
    * On shutdown, deregisters the service from UCCP.
    */
  def start(deregister: IO[Unit]): Resource[IO, Unit] =
    Resource.make(
      reportStream.compile.drain.start.flatMap { fiber =>
        IO(logger.info(
          s"Health reporter started with interval=${config.reportInterval}"
        )).as(fiber)
      }
    ) { fiber =>
      fiber.cancel *>
        deregister.handleErrorWith { err =>
          IO(logger.error(s"Failed to deregister service on shutdown: ${err.getMessage}", err))
        } *>
        IO(logger.info("Health reporter stopped and service deregistered"))
    }.void
}

// ---------------------------------------------------------------------------
// Companion -- Resource factory
// ---------------------------------------------------------------------------

object HealthReporter extends LazyLogging {

  /** Create a HealthReporter as a Resource that automatically starts
    * reporting and deregisters on shutdown.
    */
  def resource(
      client: ServiceDiscoveryClient,
      circuitBreaker: IntegrationCircuitBreaker,
      config: HealthReportingConfig,
      healthCheck: HealthCheck,
      serviceIdProvider: IO[String]
  ): Resource[IO, HealthReporter] = {
    val sender = HeartbeatSender.fromClient(client, circuitBreaker)
    val reporter = new HealthReporter(sender, config, healthCheck, serviceIdProvider)

    reporter.start(client.deregisterService).as(reporter)
  }

  /** Create a HealthReporter without auto-starting.
    * Useful when the caller manages the lifecycle externally.
    */
  def create(
      heartbeatSender: HeartbeatSender,
      config: HealthReportingConfig,
      healthCheck: HealthCheck,
      serviceIdProvider: IO[String]
  ): HealthReporter =
    new HealthReporter(heartbeatSender, config, healthCheck, serviceIdProvider)
}
