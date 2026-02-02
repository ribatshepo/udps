package io.gbmm.udps.api.rest

import cats.effect.{IO, Resource}
import com.comcast.ip4s._
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.api.config.ApiConfig
import org.http4s.HttpRoutes
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import org.http4s.server.middleware.{CORS, Logger => Http4sLogger}
import org.typelevel.ci.CIString

/** Builds and configures the http4s Ember REST server. */
object RestServer extends LazyLogging {

  /** Build a managed server resource.
    *
    * The resource acquires the server on start and shuts it down
    * gracefully when the enclosing scope is released.
    *
    * @param config  API configuration (HTTP bind, CORS, etc.)
    * @param routes  composed HttpRoutes to serve
    * @return a cats-effect Resource that manages the server lifecycle
    */
  def build(config: ApiConfig, routes: HttpRoutes[IO]): Resource[IO, Server] = {
    val host = Host.fromString(config.http.host).getOrElse(ipv4"0.0.0.0")
    val port = Port.fromInt(config.http.port).getOrElse(port"8080")

    val allowedOriginsCi = config.cors.allowedOrigins.map(o => CIString(o))

    val corsPolicy = CORS.policy
      .withAllowOriginHostCi(origin =>
        allowedOriginsCi.exists(allowed =>
          allowed == CIString("*") || origin == allowed
        )
      )
      .withAllowMethodsIn(
        config.cors.allowedMethods.flatMap(m =>
          org.http4s.Method.fromString(m.toUpperCase).toOption
        ).toSet
      )
      .withAllowHeadersIn(
        config.cors.allowedHeaders.map(CIString(_)).toSet
      )
      .withMaxAge(config.cors.maxAge)

    val withErrorHandling = ErrorHandler.middleware(routes)

    val withLogging = Http4sLogger.httpRoutes[IO](
      logHeaders = true,
      logBody = false,
      logAction = Some((msg: String) => IO(logger.debug(msg)))
    )(withErrorHandling)

    val withCors = corsPolicy.apply(withLogging)

    EmberServerBuilder
      .default[IO]
      .withHost(host)
      .withPort(port)
      .withIdleTimeout(config.http.idleTimeout)
      .withHttpApp(withCors.orNotFound)
      .build
      .evalTap { server =>
        IO(logger.info(s"REST server started on ${server.address}"))
      }
  }
}
