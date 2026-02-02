package io.gbmm.udps.api.auth

import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.circe.syntax._
import io.gbmm.udps.api.rest.ApiError
import io.gbmm.udps.integration.usp.{
  AuthorizationClient,
  AuthorizationDecision,
  AuthorizationRequest
}
import org.http4s._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.io._
import org.http4s.Challenge
import org.http4s.headers.`WWW-Authenticate`

/** http4s middleware that enforces permission checks via the USP AuthorizationClient.
  *
  * Expects the request vault to already contain an
  * [[io.gbmm.udps.integration.usp.AuthenticationContext]] under [[AuthContext.key]]
  * (i.e. [[AuthMiddleware]] must run first).
  */
object AuthorizationMiddleware extends LazyLogging {

  /** Wrap routes with a permission check for a specific resource and action.
    *
    * @param authzClient the USP authorization client
    * @param resource    the resource identifier (e.g. "catalog/databases")
    * @param action      the action being performed (e.g. "read", "write", "delete")
    * @return a function that transforms routes into authorization-guarded routes
    */
  def withAuthorization(
      authzClient: AuthorizationClient,
      resource: String,
      action: String
  ): HttpRoutes[IO] => HttpRoutes[IO] =
    (routes: HttpRoutes[IO]) =>
      Kleisli { (req: Request[IO]) =>
        req.attributes.lookup(AuthContext.key) match {
          case None =>
            OptionT.liftF(
              Unauthorized(`WWW-Authenticate`(Challenge("Bearer", "udps"))).map(_.withEntity(
                ApiError("UNAUTHORIZED", "Authentication context not found", None).asJson
              ))
            )

          case Some(authCtx) =>
            val authzReq = AuthorizationRequest(
              userId = authCtx.userId,
              resource = resource,
              action = action,
              attributes = Map.empty
            )

            OptionT(
              authzClient
                .authorize(authzReq)
                .flatMap {
                  case AuthorizationDecision.Allowed =>
                    logger.debug(
                      "Authorization allowed: userId={}, resource={}, action={}",
                      authCtx.userId,
                      resource,
                      action
                    )
                    routes.run(req).value

                  case AuthorizationDecision.Denied(reason) =>
                    logger.warn(
                      "Authorization denied: userId={}, resource={}, action={}, reason={}",
                      authCtx.userId,
                      resource,
                      action,
                      reason
                    )
                    Forbidden(
                      ApiError("FORBIDDEN", "Access denied", Some(reason)).asJson
                    ).map(Some(_))
                }
                .handleErrorWith { e =>
                  logger.error("Authorization check failed unexpectedly", e)
                  InternalServerError(
                    ApiError("AUTHORIZATION_ERROR", "Unable to verify permissions", None).asJson
                  ).map(Some(_))
                }
            )
        }
      }

  /** Convenience for composing authorization with additional request attributes.
    *
    * @param authzClient the USP authorization client
    * @param resource    the resource identifier
    * @param action      the action being performed
    * @param attributes  extra context attributes for policy evaluation
    * @return a function that transforms routes into authorization-guarded routes
    */
  def withAuthorizationAndAttributes(
      authzClient: AuthorizationClient,
      resource: String,
      action: String,
      attributes: Map[String, String]
  ): HttpRoutes[IO] => HttpRoutes[IO] =
    (routes: HttpRoutes[IO]) =>
      Kleisli { (req: Request[IO]) =>
        req.attributes.lookup(AuthContext.key) match {
          case None =>
            OptionT.liftF(
              Unauthorized(`WWW-Authenticate`(Challenge("Bearer", "udps"))).map(_.withEntity(
                ApiError("UNAUTHORIZED", "Authentication context not found", None).asJson
              ))
            )

          case Some(authCtx) =>
            val authzReq = AuthorizationRequest(
              userId = authCtx.userId,
              resource = resource,
              action = action,
              attributes = attributes
            )

            OptionT(
              authzClient
                .authorize(authzReq)
                .flatMap {
                  case AuthorizationDecision.Allowed =>
                    routes.run(req).value

                  case AuthorizationDecision.Denied(reason) =>
                    logger.warn(
                      "Authorization denied: userId={}, resource={}, action={}, reason={}",
                      authCtx.userId,
                      resource,
                      action,
                      reason
                    )
                    Forbidden(
                      ApiError("FORBIDDEN", "Access denied", Some(reason)).asJson
                    ).map(Some(_))
                }
                .handleErrorWith { e =>
                  logger.error("Authorization check failed unexpectedly", e)
                  InternalServerError(
                    ApiError("AUTHORIZATION_ERROR", "Unable to verify permissions", None).asJson
                  ).map(Some(_))
                }
            )
        }
      }
}
