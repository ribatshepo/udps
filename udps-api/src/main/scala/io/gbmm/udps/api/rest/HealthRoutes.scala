package io.gbmm.udps.api.rest

import cats.effect.IO
import cats.syntax.parallel._
import com.typesafe.scalalogging.LazyLogging
import io.circe.{Encoder, Json}
import io.circe.syntax._
import io.gbmm.udps.core.config.SeriConfig
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.io._

// ---------------------------------------------------------------------------
// Health status ADT
// ---------------------------------------------------------------------------

/** Health status of a component or the overall system. */
sealed trait HealthStatus
object HealthStatus {
  case object Up extends HealthStatus
  case object Down extends HealthStatus
  case object Degraded extends HealthStatus

  implicit val encoder: Encoder[HealthStatus] = Encoder.encodeString.contramap {
    case Up       => "UP"
    case Down     => "DOWN"
    case Degraded => "DEGRADED"
  }
}

// ---------------------------------------------------------------------------
// Component health
// ---------------------------------------------------------------------------

/** Health information for a single component. */
final case class ComponentHealth(
  name: String,
  status: HealthStatus,
  details: Option[String]
)

object ComponentHealth {
  implicit val encoder: Encoder[ComponentHealth] = Encoder.instance { c =>
    Json.obj(
      "name"    -> c.name.asJson,
      "status"  -> c.status.asJson,
      "details" -> c.details.asJson
    )
  }
}

// ---------------------------------------------------------------------------
// Integration status model
// ---------------------------------------------------------------------------

/** Connection status for an integration endpoint. */
sealed trait ConnectionStatus
object ConnectionStatus {
  case object Connected    extends ConnectionStatus
  case object Disconnected extends ConnectionStatus
  case object Disabled     extends ConnectionStatus

  implicit val encoder: Encoder[ConnectionStatus] = Encoder.encodeString.contramap {
    case Connected    => "CONNECTED"
    case Disconnected => "DISCONNECTED"
    case Disabled     => "DISABLED"
  }
}

/** Status of a single platform integration (UCCP or USP). */
final case class IntegrationStatus(
  name: String,
  enabled: Boolean,
  connection: ConnectionStatus,
  circuitBreakerState: Option[String],
  critical: Boolean
)

object IntegrationStatus {
  implicit val encoder: Encoder[IntegrationStatus] = Encoder.instance { i =>
    val base = Json.obj(
      "name"       -> i.name.asJson,
      "enabled"    -> i.enabled.asJson,
      "connection" -> i.connection.asJson,
      "critical"   -> i.critical.asJson
    )
    i.circuitBreakerState match {
      case Some(state) => base.deepMerge(Json.obj("circuitBreakerState" -> state.asJson))
      case None        => base
    }
  }
}

// ---------------------------------------------------------------------------
// Integration health check
// ---------------------------------------------------------------------------

/** Trait for integration-level health probes. */
trait IntegrationHealthCheck {
  def name: String
  def critical: Boolean
  def checkStatus: IO[IntegrationStatus]
}

// ---------------------------------------------------------------------------
// Enhanced health response
// ---------------------------------------------------------------------------

/** Aggregated health response for the system. */
final case class HealthResponse(
  service: String,
  version: String,
  mode: String,
  status: HealthStatus,
  components: List[ComponentHealth],
  integrations: List[IntegrationStatus]
)

object HealthResponse {
  implicit val encoder: Encoder[HealthResponse] = Encoder.instance { r =>
    Json.obj(
      "service"      -> r.service.asJson,
      "version"      -> r.version.asJson,
      "mode"         -> r.mode.asJson,
      "status"       -> r.status.asJson,
      "components"   -> r.components.asJson,
      "integrations" -> r.integrations.asJson
    )
  }
}

// ---------------------------------------------------------------------------
// Health check trait (infrastructure)
// ---------------------------------------------------------------------------

/** Trait for infrastructure components to implement health checking. */
trait HealthCheck {
  def name: String
  def check: IO[ComponentHealth]
}

// ---------------------------------------------------------------------------
// Health routes
// ---------------------------------------------------------------------------

/** Kubernetes-compatible health check routes with integration awareness.
  *
  * Provides three endpoints:
  *   - GET /health          -- full health with component and integration details
  *   - GET /health/readiness -- returns 503 when the service is DOWN
  *   - GET /health/liveness  -- lightweight JVM-alive probe
  *
  * Status logic depends on the deployment mode:
  *   - standalone: only infrastructure component status affects overall status
  *   - platform: integration status also affects overall status
  *     - USP is critical (DOWN if disconnected)
  *     - UCCP is non-critical (DEGRADED if disconnected)
  *
  * @param checks               registered infrastructure health-check implementations
  * @param integrationChecks    registered integration health-check implementations
  * @param seriConfig           seri platform configuration for mode/service metadata
  * @param serviceName          service name for the health response
  * @param version              application version string
  */
class HealthRoutes(
    checks: List[HealthCheck],
    integrationChecks: List[IntegrationHealthCheck],
    seriConfig: SeriConfig,
    serviceName: String,
    version: String
) extends LazyLogging {

  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "health" =>
      performHealthCheck.flatMap(r => Ok(r.asJson))

    case GET -> Root / "health" / "readiness" =>
      performHealthCheck.flatMap { r =>
        r.status match {
          case HealthStatus.Down => ServiceUnavailable(r.asJson)
          case _                 => Ok(r.asJson)
        }
      }

    case GET -> Root / "health" / "liveness" =>
      Ok(Json.obj(
        "status"  -> Json.fromString("UP"),
        "service" -> Json.fromString(serviceName),
        "version" -> Json.fromString(version)
      ))
  }

  private def performHealthCheck: IO[HealthResponse] =
    for {
      componentResults   <- checks.parTraverse(_.check)
      integrationResults <- integrationChecks.parTraverse(_.checkStatus)
      overallStatus       = computeOverallStatus(componentResults, integrationResults)
    } yield HealthResponse(
      service = serviceName,
      version = version,
      mode = seriConfig.mode,
      status = overallStatus,
      components = componentResults,
      integrations = integrationResults
    )

  /** Compute the overall system status from infrastructure components and integrations.
    *
    * Infrastructure components always affect the status. Integration status
    * only affects the overall status in platform mode:
    *   - A critical integration (USP) being disconnected results in DOWN
    *   - A non-critical integration (UCCP) being disconnected results in DEGRADED
    */
  private def computeOverallStatus(
      components: List[ComponentHealth],
      integrations: List[IntegrationStatus]
  ): HealthStatus = {
    val infraStatus = computeInfraStatus(components)

    if (seriConfig.isStandalone) {
      infraStatus
    } else {
      val integrationStatus = computeIntegrationStatus(integrations)
      mergeStatuses(infraStatus, integrationStatus)
    }
  }

  private def computeInfraStatus(components: List[ComponentHealth]): HealthStatus =
    if (components.forall(_.status == HealthStatus.Up)) HealthStatus.Up
    else if (components.exists(_.status == HealthStatus.Down)) HealthStatus.Down
    else HealthStatus.Degraded

  private def computeIntegrationStatus(integrations: List[IntegrationStatus]): HealthStatus = {
    val enabledIntegrations = integrations.filter(_.enabled)

    val criticalDown = enabledIntegrations.exists { i =>
      i.critical && i.connection == ConnectionStatus.Disconnected
    }
    val nonCriticalDown = enabledIntegrations.exists { i =>
      !i.critical && i.connection == ConnectionStatus.Disconnected
    }

    if (criticalDown) HealthStatus.Down
    else if (nonCriticalDown) HealthStatus.Degraded
    else HealthStatus.Up
  }

  /** Merge infrastructure and integration statuses, taking the worst. */
  private def mergeStatuses(infra: HealthStatus, integration: HealthStatus): HealthStatus =
    (infra, integration) match {
      case (HealthStatus.Down, _)     => HealthStatus.Down
      case (_, HealthStatus.Down)     => HealthStatus.Down
      case (HealthStatus.Degraded, _) => HealthStatus.Degraded
      case (_, HealthStatus.Degraded) => HealthStatus.Degraded
      case _                          => HealthStatus.Up
    }
}

object HealthRoutes {

  /** Convenience constructor for backward compatibility when no integration checks are configured. */
  def apply(
      checks: List[HealthCheck],
      seriConfig: SeriConfig,
      serviceName: String,
      version: String
  ): HealthRoutes =
    new HealthRoutes(checks, Nil, seriConfig, serviceName, version)
}
