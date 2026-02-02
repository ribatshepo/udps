package io.gbmm.udps.api.rest

import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import cats.syntax.applicativeError._
import com.typesafe.scalalogging.LazyLogging
import io.circe.{Encoder, Json}
import io.circe.syntax._
import io.gbmm.udps.integration.usp.AuthenticationException
import org.http4s._
import org.http4s.circe._
import org.http4s.dsl.io._

/** Standardised API error response body. */
final case class ApiError(
  code: String,
  message: String,
  details: Option[String]
)

object ApiError {
  implicit val encoder: Encoder[ApiError] = Encoder.instance { e =>
    Json.obj(
      "code" -> e.code.asJson,
      "message" -> e.message.asJson,
      "details" -> e.details.asJson
    )
  }
}

/** Converts exceptions to appropriate HTTP error responses. */
object ErrorHandler extends LazyLogging {

  /** Map a Throwable to an HTTP response with the correct status and JSON body. */
  def toResponse(err: Throwable): IO[Response[IO]] = err match {
    case e: IllegalArgumentException =>
      BadRequest(ApiError("BAD_REQUEST", e.getMessage, None).asJson)

    case e: AuthenticationException =>
      Forbidden(ApiError(e.errorCode, e.serverMessage, None).asJson)

    case _: NoSuchElementException =>
      NotFound(ApiError("NOT_FOUND", "The requested resource was not found", None).asJson)

    case e: SecurityException =>
      Forbidden(ApiError("FORBIDDEN", e.getMessage, None).asJson)

    case e =>
      logger.error("Unhandled error in API request", e)
      InternalServerError(
        ApiError(
          "INTERNAL_ERROR",
          "An internal error occurred",
          None
        ).asJson
      )
  }

  /** Middleware that wraps HttpRoutes with error handling.
    *
    * Catches exceptions thrown during route handling and converts
    * them to structured JSON error responses.
    */
  def middleware(routes: HttpRoutes[IO]): HttpRoutes[IO] =
    Kleisli { req =>
      routes.run(req).recoverWith {
        case err: Throwable =>
          OptionT.liftF(toResponse(err))
      }
    }
}
