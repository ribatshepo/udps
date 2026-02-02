package io.gbmm.udps.api.graphql

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.circe.{Json, JsonObject}
import org.http4s._
import org.http4s.circe._
import org.http4s.dsl.io._
import sangria.ast.Document
import sangria.execution.{ErrorWithResolver, Executor, QueryAnalysisError}
import sangria.marshalling.circe._
import sangria.parser.{QueryParser, SyntaxError}
import sangria.renderer.SchemaRenderer

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** http4s routes that serve the UDPS GraphQL API.
  *
  * @param context the resolver context holding repository and executor references
  */
final class GraphQLRoutes(context: GraphQLContext) extends LazyLogging {

  private val schema = Resolvers.schema

  /** Execution context derived from the Cats Effect IO runtime compute pool. */
  private implicit val executionContext: ExecutionContext =
    context.runtime.compute

  /** The http4s routes for GraphQL query execution and schema introspection. */
  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case req @ POST -> Root / "api" / "v1" / "graphql" =>
      handleGraphQL(req)

    case GET -> Root / "api" / "v1" / "graphql" / "schema" =>
      handleSchemaIntrospection
  }

  /** Parse a GraphQL request body and execute it via Sangria. */
  private def handleGraphQL(req: Request[IO]): IO[Response[IO]] =
    req.as[Json].flatMap { body =>
      val queryOpt = body.hcursor.get[String]("query").toOption
      val variablesOpt = body.hcursor
        .downField("variables")
        .as[Json]
        .toOption
        .filterNot(_.isNull)
      val operationNameOpt = body.hcursor.get[String]("operationName").toOption

      queryOpt match {
        case None =>
          BadRequest(errorJson("INVALID_REQUEST", "Missing required field: query"))

        case Some(queryStr) =>
          parseAndExecute(queryStr, variablesOpt, operationNameOpt)
      }
    }.handleErrorWith { err =>
      logger.error("Failed to process GraphQL request body", err)
      BadRequest(errorJson("INVALID_REQUEST", s"Malformed request body: ${err.getMessage}"))
    }

  /** Parse the query string and execute against the schema. */
  private def parseAndExecute(
      queryStr: String,
      variables: Option[Json],
      operationName: Option[String]
  ): IO[Response[IO]] =
    QueryParser.parse(queryStr) match {
      case Success(document) =>
        executeDocument(document, variables, operationName)

      case Failure(e: SyntaxError) =>
        BadRequest(
          errorJson(
            "SYNTAX_ERROR",
            e.getMessage,
            Some(
              Json.obj(
                "line" -> Json.fromInt(e.originalError.position.line),
                "column" -> Json.fromInt(e.originalError.position.column)
              )
            )
          )
        )

      case Failure(e) =>
        BadRequest(errorJson("PARSE_ERROR", e.getMessage))
    }

  /** Execute a parsed GraphQL document. */
  private def executeDocument(
      document: Document,
      variables: Option[Json],
      operationName: Option[String]
  ): IO[Response[IO]] = {
    val vars = variables
      .flatMap(_.asObject)
      .getOrElse(JsonObject.empty)

    IO.fromFuture(
      IO(
        Executor.execute(
          schema = schema,
          queryAst = document,
          userContext = context,
          variables = Json.fromJsonObject(vars),
          operationName = operationName
        )
      )
    ).flatMap { result =>
      Ok(result)
    }.handleErrorWith {
      case error: QueryAnalysisError =>
        BadRequest(error.resolveError)
      case error: ErrorWithResolver =>
        InternalServerError(error.resolveError)
      case err =>
        logger.error("Unexpected GraphQL execution error", err)
        InternalServerError(
          errorJson("INTERNAL_ERROR", "An internal error occurred during query execution")
        )
    }
  }

  /** Return the GraphQL schema as SDL text. */
  private val handleSchemaIntrospection: IO[Response[IO]] =
    Ok(SchemaRenderer.renderSchema(schema))
      .map(_.withContentType(headers.`Content-Type`(MediaType.text.plain)))

  /** Build a standard error JSON response body. */
  private def errorJson(
      code: String,
      message: String,
      details: Option[Json] = None
  ): Json =
    Json.obj(
      "errors" -> Json.arr(
        Json.obj(
          "message" -> Json.fromString(message),
          "extensions" -> Json.obj(
            "code" -> Json.fromString(code),
            "details" -> details.getOrElse(Json.Null)
          )
        )
      )
    )
}

object GraphQLRoutes {

  /** Create a new GraphQLRoutes instance from a context. */
  def apply(context: GraphQLContext): GraphQLRoutes =
    new GraphQLRoutes(context)
}
