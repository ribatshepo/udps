package io.gbmm.udps.api.rest

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.circe.syntax._
import io.gbmm.udps.catalog.repository._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.io._

/** REST routes for catalog CRUD operations on databases, schemas, tables, and columns.
  *
  * @param repo the metadata repository backing all persistence operations
  */
class CatalogRoutes(repo: MetadataRepository) extends LazyLogging {

  import JsonCodecs._

  private val notFoundError: ApiError =
    ApiError("NOT_FOUND", "The requested resource was not found", None)

  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {

    // -----------------------------------------------------------------------
    // Databases
    // -----------------------------------------------------------------------

    case GET -> Root / "api" / "v1" / "databases" =>
      repo.listDatabases.flatMap { dbs =>
        Ok(ApiResponse(dbs, Some(ResponseMeta(Some(dbs.length.toLong), None))).asJson)
      }

    case req @ POST -> Root / "api" / "v1" / "databases" =>
      req.as[CreateDatabaseRequest].flatMap { body =>
        validateNonBlank("name", body.name)
        val now = Instant.now()
        val db = CatalogDatabase(
          id = UUID.randomUUID(),
          name = body.name.trim,
          description = body.description.map(_.trim),
          createdAt = now,
          updatedAt = now
        )
        repo.createDatabase(db).flatMap(created => Created(ApiResponse(created, None).asJson))
      }

    case GET -> Root / "api" / "v1" / "databases" / UUIDVar(id) =>
      repo.getDatabase(id).flatMap {
        case Some(db) => Ok(ApiResponse(db, None).asJson)
        case None     => NotFound(notFoundError.asJson)
      }

    case req @ PUT -> Root / "api" / "v1" / "databases" / UUIDVar(id) =>
      req.as[UpdateDatabaseRequest].flatMap { body =>
        validateNonBlank("name", body.name)
        repo.getDatabase(id).flatMap {
          case Some(existing) =>
            val updated = existing.copy(
              name = body.name.trim,
              description = body.description.map(_.trim)
            )
            repo.updateDatabase(updated).flatMap(u => Ok(ApiResponse(u, None).asJson))
          case None =>
            NotFound(notFoundError.asJson)
        }
      }

    case DELETE -> Root / "api" / "v1" / "databases" / UUIDVar(id) =>
      repo.deleteDatabase(id).flatMap(_ => NoContent())

    // -----------------------------------------------------------------------
    // Schemas
    // -----------------------------------------------------------------------

    case GET -> Root / "api" / "v1" / "databases" / UUIDVar(dbId) / "schemas" =>
      repo.listSchemasByDatabase(dbId).flatMap { schemas =>
        Ok(ApiResponse(schemas, Some(ResponseMeta(Some(schemas.length.toLong), None))).asJson)
      }

    case req @ POST -> Root / "api" / "v1" / "databases" / UUIDVar(dbId) / "schemas" =>
      req.as[CreateSchemaRequest].flatMap { body =>
        validateNonBlank("name", body.name)
        val now = Instant.now()
        val schema = CatalogSchema(
          id = UUID.randomUUID(),
          databaseId = dbId,
          name = body.name.trim,
          createdAt = now,
          updatedAt = now
        )
        repo.createSchema(schema).flatMap(created => Created(ApiResponse(created, None).asJson))
      }

    case GET -> Root / "api" / "v1" / "schemas" / UUIDVar(id) =>
      repo.getSchema(id).flatMap {
        case Some(s) => Ok(ApiResponse(s, None).asJson)
        case None    => NotFound(notFoundError.asJson)
      }

    case req @ PUT -> Root / "api" / "v1" / "schemas" / UUIDVar(id) =>
      req.as[UpdateSchemaRequest].flatMap { body =>
        validateNonBlank("name", body.name)
        repo.getSchema(id).flatMap {
          case Some(existing) =>
            val updated = existing.copy(name = body.name.trim)
            repo.updateSchema(updated).flatMap(u => Ok(ApiResponse(u, None).asJson))
          case None =>
            NotFound(notFoundError.asJson)
        }
      }

    case DELETE -> Root / "api" / "v1" / "schemas" / UUIDVar(id) =>
      repo.deleteSchema(id).flatMap(_ => NoContent())

    // -----------------------------------------------------------------------
    // Tables
    // -----------------------------------------------------------------------

    case GET -> Root / "api" / "v1" / "schemas" / UUIDVar(schemaId) / "tables" =>
      repo.listTablesBySchema(schemaId).flatMap { tables =>
        Ok(ApiResponse(tables, Some(ResponseMeta(Some(tables.length.toLong), None))).asJson)
      }

    case req @ POST -> Root / "api" / "v1" / "schemas" / UUIDVar(schemaId) / "tables" =>
      req.as[CreateTableRequest].flatMap { body =>
        validateNonBlank("name", body.name)
        validateNonBlank("tier", body.tier)
        val now = Instant.now()
        val table = CatalogTable(
          id = UUID.randomUUID(),
          schemaId = schemaId,
          name = body.name.trim,
          rowCount = 0L,
          sizeBytes = 0L,
          tier = body.tier.trim,
          createdAt = now,
          updatedAt = now
        )
        repo.createTable(table).flatMap(created => Created(ApiResponse(created, None).asJson))
      }

    case GET -> Root / "api" / "v1" / "tables" / UUIDVar(id) =>
      repo.getTable(id).flatMap {
        case Some(t) => Ok(ApiResponse(t, None).asJson)
        case None    => NotFound(notFoundError.asJson)
      }

    case req @ PUT -> Root / "api" / "v1" / "tables" / UUIDVar(id) =>
      req.as[UpdateTableRequest].flatMap { body =>
        validateNonBlank("name", body.name)
        validateNonBlank("tier", body.tier)
        repo.getTable(id).flatMap {
          case Some(existing) =>
            val updated = existing.copy(
              name = body.name.trim,
              rowCount = body.rowCount,
              sizeBytes = body.sizeBytes,
              tier = body.tier.trim
            )
            repo.updateTable(updated).flatMap(u => Ok(ApiResponse(u, None).asJson))
          case None =>
            NotFound(notFoundError.asJson)
        }
      }

    case DELETE -> Root / "api" / "v1" / "tables" / UUIDVar(id) =>
      repo.deleteTable(id).flatMap(_ => NoContent())

    // -----------------------------------------------------------------------
    // Columns
    // -----------------------------------------------------------------------

    case GET -> Root / "api" / "v1" / "tables" / UUIDVar(tableId) / "columns" =>
      repo.listColumnsByTable(tableId).flatMap { columns =>
        Ok(ApiResponse(columns, Some(ResponseMeta(Some(columns.length.toLong), None))).asJson)
      }

    case req @ POST -> Root / "api" / "v1" / "tables" / UUIDVar(tableId) / "columns" =>
      req.as[CreateColumnRequest].flatMap { body =>
        validateNonBlank("name", body.name)
        validateNonBlank("dataType", body.dataType)
        val column = CatalogColumn(
          id = UUID.randomUUID(),
          tableId = tableId,
          name = body.name.trim,
          dataType = body.dataType.trim,
          nullable = body.nullable,
          indexed = body.indexed,
          ftsEnabled = body.ftsEnabled,
          piiClassified = body.piiClassified,
          ordinalPosition = body.ordinalPosition
        )
        repo.createColumn(column).flatMap(created => Created(ApiResponse(created, None).asJson))
      }

    case GET -> Root / "api" / "v1" / "columns" / UUIDVar(id) =>
      repo.getColumn(id).flatMap {
        case Some(c) => Ok(ApiResponse(c, None).asJson)
        case None    => NotFound(notFoundError.asJson)
      }

    case req @ PUT -> Root / "api" / "v1" / "columns" / UUIDVar(id) =>
      req.as[UpdateColumnRequest].flatMap { body =>
        validateNonBlank("name", body.name)
        validateNonBlank("dataType", body.dataType)
        repo.getColumn(id).flatMap {
          case Some(existing) =>
            val updated = existing.copy(
              name = body.name.trim,
              dataType = body.dataType.trim,
              nullable = body.nullable,
              indexed = body.indexed,
              ftsEnabled = body.ftsEnabled,
              piiClassified = body.piiClassified,
              ordinalPosition = body.ordinalPosition
            )
            repo.updateColumn(updated).flatMap(u => Ok(ApiResponse(u, None).asJson))
          case None =>
            NotFound(notFoundError.asJson)
        }
      }

    case DELETE -> Root / "api" / "v1" / "columns" / UUIDVar(id) =>
      repo.deleteColumn(id).flatMap(_ => NoContent())
  }

  /** Validates that a string field is not blank, raising IllegalArgumentException if it is.
    * The ErrorHandler middleware converts this to a 400 Bad Request response.
    */
  private def validateNonBlank(fieldName: String, value: String): Unit =
    if (value == null || value.trim.isEmpty)
      throw new IllegalArgumentException(s"'$fieldName' must not be blank")
}
