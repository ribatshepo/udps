package io.gbmm.udps.api.auth

import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.circe.syntax._
import io.gbmm.udps.api.rest.ApiError
import io.gbmm.udps.integration.usp.{AuthenticationClient, AuthenticationException}
import org.http4s._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.io._
import org.http4s.headers.{Authorization, `WWW-Authenticate`}

/** http4s middleware that validates JWT bearer tokens via the USP AuthenticationClient.
  *
  * On success the resolved [[io.gbmm.udps.integration.usp.AuthenticationContext]] is
  * stored in the request vault under [[AuthContext.key]] so downstream routes and
  * the authorization middleware can access it without a second remote call.
  */
object AuthMiddleware extends LazyLogging {

  private val bearerChallenge = `WWW-Authenticate`(Challenge("Bearer", "udps"))

  /** Wrap existing routes with JWT authentication.
    *
    * @param authClient the USP authentication client used to validate tokens
    * @return a function that transforms unauthenticated routes into authenticated ones
    */
  def apply(authClient: AuthenticationClient): HttpRoutes[IO] => HttpRoutes[IO] =
    (routes: HttpRoutes[IO]) =>
      Kleisli { (req: Request[IO]) =>
        extractBearerToken(req) match {
          case None =>
            OptionT.liftF(
              Unauthorized(bearerChallenge).map(_.withEntity(
                ApiError("UNAUTHORIZED", "Missing or malformed Authorization header", None).asJson
              ))
            )

          case Some(token) =>
            OptionT(
              authClient
                .validateToken(token)
                .map { authCtx =>
                  val enrichedReq = req.withAttribute(AuthContext.key, authCtx)
                  Some(enrichedReq)
                }
                .handleErrorWith {
                  case e: AuthenticationException =>
                    logger.warn("Token validation failed: errorCode={}, message={}", e.errorCode, e.serverMessage)
                    IO.pure(None: Option[Request[IO]])
                  case e =>
                    logger.error("Unexpected error during token validation", e)
                    IO.pure(None: Option[Request[IO]])
                }
            ).flatMap { enrichedReq =>
                routes.run(enrichedReq)
            }.orElse(
              OptionT.liftF(
                Unauthorized(bearerChallenge).map(_.withEntity(
                  ApiError("UNAUTHORIZED", "Invalid or expired authentication token", None).asJson
                ))
              )
            )
        }
      }

  private def extractBearerToken(req: Request[IO]): Option[String] =
    req.headers.get[Authorization].collect {
      case Authorization(Credentials.Token(AuthScheme.Bearer, token)) => token
    }
}
