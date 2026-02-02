package io.gbmm.udps.api.grpc

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.catalog.repository._
import io.grpc.{Status, StatusRuntimeException}
import udps.api.catalog_service.CatalogServiceGrpc
import udps.api.catalog_service.{
  CreateColumnRequest,
  CreateDatabaseRequest,
  CreateSchemaRequest,
  CreateTableRequest,
  DeleteColumnRequest,
  DeleteDatabaseRequest,
  DeleteSchemaRequest,
  DeleteTableRequest,
  GetColumnRequest,
  GetDatabaseRequest,
  GetSchemaRequest,
  GetTableRequest,
  ListColumnsRequest,
  ListColumnsResponse,
  ListDatabasesRequest,
  ListDatabasesResponse,
  ListSchemasRequest,
  ListSchemasResponse,
  ListTablesRequest,
  ListTablesResponse,
  UpdateColumnRequest,
  UpdateDatabaseRequest,
  UpdateSchemaRequest,
  UpdateTableRequest
}
import udps.api.common.{ApiResponse, PaginationResponse, Timestamp}
import udps.api.catalog_service.{
  Column => ProtoColumn,
  Database => ProtoDatabase,
  Schema => ProtoSchema,
  Table => ProtoTable
}

import scala.concurrent.Future

final class CatalogServiceImpl(
    repository: MetadataRepository,
    ioRuntime: IORuntime
) extends CatalogServiceGrpc.CatalogService
    with LazyLogging {

  private[this] implicit val runtime: IORuntime = ioRuntime

  // ---------------------------------------------------------------------------
  // Databases
  // ---------------------------------------------------------------------------

  override def createDatabase(request: CreateDatabaseRequest): Future[ProtoDatabase] =
    runIO("CreateDatabase") {
      val now = Instant.now()
      val db = CatalogDatabase(
        id = UUID.randomUUID(),
        name = request.name,
        description = Option(request.description).filter(_.nonEmpty),
        createdAt = now,
        updatedAt = now
      )
      repository.createDatabase(db).map(toProtoDatabase)
    }

  override def getDatabase(request: GetDatabaseRequest): Future[ProtoDatabase] =
    runIO("GetDatabase") {
      for {
        id <- parseUUID(request.id, "database id")
        db <- repository.getDatabase(id)
        result <- db match {
          case Some(d) => IO.pure(toProtoDatabase(d))
          case None    => IO.raiseError(notFound(s"Database '${request.id}' not found"))
        }
      } yield result
    }

  override def listDatabases(request: ListDatabasesRequest): Future[ListDatabasesResponse] =
    runIO("ListDatabases") {
      repository.listDatabases.map { dbs =>
        ListDatabasesResponse(
          databases = dbs.map(toProtoDatabase),
          pagination = Some(PaginationResponse(totalCount = dbs.size))
        )
      }
    }

  override def updateDatabase(request: UpdateDatabaseRequest): Future[ProtoDatabase] =
    runIO("UpdateDatabase") {
      for {
        id <- parseUUID(request.id, "database id")
        existing <- repository.getDatabase(id)
        db <- existing match {
          case Some(d) =>
            val updated = d.copy(
              name = if (request.name.nonEmpty) request.name else d.name,
              description = if (request.description.nonEmpty) Some(request.description) else d.description
            )
            repository.updateDatabase(updated).map(toProtoDatabase)
          case None => IO.raiseError(notFound(s"Database '${request.id}' not found"))
        }
      } yield db
    }

  override def deleteDatabase(request: DeleteDatabaseRequest): Future[ApiResponse] =
    runIO("DeleteDatabase") {
      for {
        id <- parseUUID(request.id, "database id")
        _  <- repository.deleteDatabase(id)
      } yield ApiResponse(success = true, message = s"Database '${request.id}' deleted")
    }

  // ---------------------------------------------------------------------------
  // Schemas
  // ---------------------------------------------------------------------------

  override def createSchema(request: CreateSchemaRequest): Future[ProtoSchema] =
    runIO("CreateSchema") {
      for {
        dbId <- parseUUID(request.databaseId, "database_id")
        now   = Instant.now()
        schema = CatalogSchema(
          id = UUID.randomUUID(),
          databaseId = dbId,
          name = request.name,
          createdAt = now,
          updatedAt = now
        )
        created <- repository.createSchema(schema)
      } yield toProtoSchema(created)
    }

  override def getSchema(request: GetSchemaRequest): Future[ProtoSchema] =
    runIO("GetSchema") {
      for {
        id <- parseUUID(request.id, "schema id")
        schema <- repository.getSchema(id)
        result <- schema match {
          case Some(s) => IO.pure(toProtoSchema(s))
          case None    => IO.raiseError(notFound(s"Schema '${request.id}' not found"))
        }
      } yield result
    }

  override def listSchemas(request: ListSchemasRequest): Future[ListSchemasResponse] =
    runIO("ListSchemas") {
      for {
        dbId    <- parseUUID(request.databaseId, "database_id")
        schemas <- repository.listSchemasByDatabase(dbId)
      } yield ListSchemasResponse(
        schemas = schemas.map(toProtoSchema),
        pagination = Some(PaginationResponse(totalCount = schemas.size))
      )
    }

  override def updateSchema(request: UpdateSchemaRequest): Future[ProtoSchema] =
    runIO("UpdateSchema") {
      for {
        id <- parseUUID(request.id, "schema id")
        existing <- repository.getSchema(id)
        schema <- existing match {
          case Some(s) =>
            val updated = s.copy(
              name = if (request.name.nonEmpty) request.name else s.name
            )
            repository.updateSchema(updated).map(toProtoSchema)
          case None => IO.raiseError(notFound(s"Schema '${request.id}' not found"))
        }
      } yield schema
    }

  override def deleteSchema(request: DeleteSchemaRequest): Future[ApiResponse] =
    runIO("DeleteSchema") {
      for {
        id <- parseUUID(request.id, "schema id")
        _  <- repository.deleteSchema(id)
      } yield ApiResponse(success = true, message = s"Schema '${request.id}' deleted")
    }

  // ---------------------------------------------------------------------------
  // Tables
  // ---------------------------------------------------------------------------

  override def createTable(request: CreateTableRequest): Future[ProtoTable] =
    runIO("CreateTable") {
      for {
        schemaId <- parseUUID(request.schemaId, "schema_id")
        now       = Instant.now()
        table = CatalogTable(
          id = UUID.randomUUID(),
          schemaId = schemaId,
          name = request.name,
          rowCount = 0L,
          sizeBytes = 0L,
          tier = if (request.tier.nonEmpty) request.tier else "hot",
          createdAt = now,
          updatedAt = now
        )
        created <- repository.createTable(table)
      } yield toProtoTable(created)
    }

  override def getTable(request: GetTableRequest): Future[ProtoTable] =
    runIO("GetTable") {
      for {
        id    <- parseUUID(request.id, "table id")
        table <- repository.getTable(id)
        result <- table match {
          case Some(t) => IO.pure(toProtoTable(t))
          case None    => IO.raiseError(notFound(s"Table '${request.id}' not found"))
        }
      } yield result
    }

  override def listTables(request: ListTablesRequest): Future[ListTablesResponse] =
    runIO("ListTables") {
      for {
        schemaId <- parseUUID(request.schemaId, "schema_id")
        tables   <- repository.listTablesBySchema(schemaId)
      } yield ListTablesResponse(
        tables = tables.map(toProtoTable),
        pagination = Some(PaginationResponse(totalCount = tables.size))
      )
    }

  override def updateTable(request: UpdateTableRequest): Future[ProtoTable] =
    runIO("UpdateTable") {
      for {
        id <- parseUUID(request.id, "table id")
        existing <- repository.getTable(id)
        table <- existing match {
          case Some(t) =>
            val updated = t.copy(
              name = if (request.name.nonEmpty) request.name else t.name,
              tier = if (request.tier.nonEmpty) request.tier else t.tier
            )
            repository.updateTable(updated).map(toProtoTable)
          case None => IO.raiseError(notFound(s"Table '${request.id}' not found"))
        }
      } yield table
    }

  override def deleteTable(request: DeleteTableRequest): Future[ApiResponse] =
    runIO("DeleteTable") {
      for {
        id <- parseUUID(request.id, "table id")
        _  <- repository.deleteTable(id)
      } yield ApiResponse(success = true, message = s"Table '${request.id}' deleted")
    }

  // ---------------------------------------------------------------------------
  // Columns
  // ---------------------------------------------------------------------------

  override def createColumn(request: CreateColumnRequest): Future[ProtoColumn] =
    runIO("CreateColumn") {
      for {
        tableId <- parseUUID(request.tableId, "table_id")
        column = CatalogColumn(
          id = UUID.randomUUID(),
          tableId = tableId,
          name = request.name,
          dataType = request.dataType,
          nullable = request.nullable,
          indexed = false,
          ftsEnabled = false,
          piiClassified = false,
          ordinalPosition = request.ordinalPosition
        )
        created <- repository.createColumn(column)
      } yield toProtoColumn(created)
    }

  override def getColumn(request: GetColumnRequest): Future[ProtoColumn] =
    runIO("GetColumn") {
      for {
        id     <- parseUUID(request.id, "column id")
        column <- repository.getColumn(id)
        result <- column match {
          case Some(c) => IO.pure(toProtoColumn(c))
          case None    => IO.raiseError(notFound(s"Column '${request.id}' not found"))
        }
      } yield result
    }

  override def listColumns(request: ListColumnsRequest): Future[ListColumnsResponse] =
    runIO("ListColumns") {
      for {
        tableId <- parseUUID(request.tableId, "table_id")
        columns <- repository.listColumnsByTable(tableId)
      } yield ListColumnsResponse(
        columns = columns.map(toProtoColumn),
        pagination = Some(PaginationResponse(totalCount = columns.size))
      )
    }

  override def updateColumn(request: UpdateColumnRequest): Future[ProtoColumn] =
    runIO("UpdateColumn") {
      for {
        id <- parseUUID(request.id, "column id")
        existing <- repository.getColumn(id)
        column <- existing match {
          case Some(c) =>
            val updated = c.copy(
              name = if (request.name.nonEmpty) request.name else c.name,
              dataType = if (request.dataType.nonEmpty) request.dataType else c.dataType,
              nullable = request.nullable,
              indexed = request.indexed
            )
            repository.updateColumn(updated).map(toProtoColumn)
          case None => IO.raiseError(notFound(s"Column '${request.id}' not found"))
        }
      } yield column
    }

  override def deleteColumn(request: DeleteColumnRequest): Future[ApiResponse] =
    runIO("DeleteColumn") {
      for {
        id <- parseUUID(request.id, "column id")
        _  <- repository.deleteColumn(id)
      } yield ApiResponse(success = true, message = s"Column '${request.id}' deleted")
    }

  // ---------------------------------------------------------------------------
  // Conversions
  // ---------------------------------------------------------------------------

  private def toProtoDatabase(d: CatalogDatabase): ProtoDatabase =
    ProtoDatabase(
      id = d.id.toString,
      name = d.name,
      description = d.description.getOrElse(""),
      createdAt = Some(toTimestamp(d.createdAt)),
      updatedAt = Some(toTimestamp(d.updatedAt))
    )

  private def toProtoSchema(s: CatalogSchema): ProtoSchema =
    ProtoSchema(
      id = s.id.toString,
      databaseId = s.databaseId.toString,
      name = s.name,
      createdAt = Some(toTimestamp(s.createdAt)),
      updatedAt = Some(toTimestamp(s.updatedAt))
    )

  private def toProtoTable(t: CatalogTable): ProtoTable =
    ProtoTable(
      id = t.id.toString,
      schemaId = t.schemaId.toString,
      name = t.name,
      rowCount = t.rowCount,
      sizeBytes = t.sizeBytes,
      tier = t.tier,
      createdAt = Some(toTimestamp(t.createdAt)),
      updatedAt = Some(toTimestamp(t.updatedAt))
    )

  private def toProtoColumn(c: CatalogColumn): ProtoColumn =
    ProtoColumn(
      id = c.id.toString,
      tableId = c.tableId.toString,
      name = c.name,
      dataType = c.dataType,
      nullable = c.nullable,
      indexed = c.indexed,
      ftsEnabled = c.ftsEnabled,
      piiClassified = c.piiClassified,
      ordinalPosition = c.ordinalPosition
    )

  private def toTimestamp(instant: Instant): Timestamp =
    Timestamp(seconds = instant.getEpochSecond, nanos = instant.getNano)

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def parseUUID(value: String, fieldName: String): IO[UUID] =
    IO.delay(UUID.fromString(value)).handleErrorWith { _ =>
      IO.raiseError(
        Status.INVALID_ARGUMENT
          .withDescription(s"Invalid UUID for $fieldName: '$value'")
          .asRuntimeException()
      )
    }

  private def notFound(message: String): StatusRuntimeException =
    Status.NOT_FOUND.withDescription(message).asRuntimeException()

  private def runIO[A](rpcName: String)(effect: IO[A]): Future[A] =
    effect.handleErrorWith {
      case e: StatusRuntimeException => IO.raiseError(e)
      case e: IllegalArgumentException =>
        logger.warn("Invalid argument in {}: {}", rpcName, e.getMessage: Any)
        IO.raiseError(
          Status.ALREADY_EXISTS
            .withDescription(e.getMessage)
            .asRuntimeException()
        )
      case e: Throwable =>
        logger.error(s"Unexpected error in $rpcName", e)
        IO.raiseError(
          Status.INTERNAL
            .withDescription(s"Internal error in $rpcName: ${e.getMessage}")
            .asRuntimeException()
        )
    }.unsafeToFuture()
}
