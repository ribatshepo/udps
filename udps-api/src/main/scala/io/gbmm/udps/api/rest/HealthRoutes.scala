package io.gbmm.udps.api.rest

import cats.effect.IO
import cats.syntax.parallel._
import com.typesafe.scalalogging.LazyLogging
import io.circe.{Encoder, Json}
import io.circe.syntax._
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.io._

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

/** Aggregated health response for the system. */
final case class HealthResponse(
  status: HealthStatus,
  components: List[ComponentHealth],
  version: String
)

object HealthResponse {
  implicit val encoder: Encoder[HealthResponse] = Encoder.instance { r =>
    Json.obj(
      "status"     -> r.status.asJson,
      "components" -> r.components.asJson,
      "version"    -> r.version.asJson
    )
  }
}

/** Trait for components to implement health checking. */
trait HealthCheck {
  def name: String
  def check: IO[ComponentHealth]
}

/** Kubernetes-compatible health check routes.
  *
  * Provides three endpoints:
  *   - GET /health          – full health with component details
  *   - GET /health/readiness – returns 503 when any component is DOWN
  *   - GET /health/liveness  – lightweight JVM-alive probe
  *
  * @param checks  registered health-check implementations
  * @param version application version string
  */
class HealthRoutes(checks: List[HealthCheck], version: String) extends LazyLogging {

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
        "version" -> Json.fromString(version)
      ))
  }

  private def performHealthCheck: IO[HealthResponse] =
    checks.parTraverse(_.check).map { componentResults =>
      val overallStatus =
        if (componentResults.forall(_.status == HealthStatus.Up)) HealthStatus.Up
        else if (componentResults.exists(_.status == HealthStatus.Down)) HealthStatus.Down
        else HealthStatus.Degraded
      HealthResponse(overallStatus, componentResults, version)
    }
}
