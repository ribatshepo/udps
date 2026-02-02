package io.gbmm.udps.api.rest

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.circe.syntax._
import org.http4s.{HttpRoutes, MediaType, StaticFile}
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.io._
import org.http4s.headers.`Content-Type`

/** Serves API documentation resources from the classpath.
  *
  * Exposes two endpoints:
  *   - GET /api/v1/docs/openapi.yaml  -- OpenAPI 3.0 specification
  *   - GET /api/v1/docs/graphql.sdl   -- GraphQL schema in SDL format
  */
class DocsRoutes extends LazyLogging {

  private val yamlContentType: `Content-Type` =
    `Content-Type`(new MediaType("application", "x-yaml"))

  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {

    case GET -> Root / "api" / "v1" / "docs" / "openapi.yaml" =>
      StaticFile
        .fromResource[IO]("/openapi.yaml")
        .getOrElseF(
          NotFound(ApiError("NOT_FOUND", "OpenAPI specification not found", None).asJson)
        )
        .map(_.withContentType(yamlContentType))

    case GET -> Root / "api" / "v1" / "docs" / "graphql.sdl" =>
      StaticFile
        .fromResource[IO]("/graphql-schema.sdl")
        .getOrElseF(
          NotFound(ApiError("NOT_FOUND", "GraphQL SDL schema not found", None).asJson)
        )
        .map(_.withContentType(`Content-Type`(MediaType.text.plain)))
  }
}
