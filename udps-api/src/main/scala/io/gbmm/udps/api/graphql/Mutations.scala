package io.gbmm.udps.api.graphql

import java.time.Instant
import java.util.UUID

import io.gbmm.udps.catalog.repository._
import sangria.schema._

/** Mutation resolvers for UDPS catalog CRUD operations. */
object Mutations {

  import GraphQLSchema._

  // ---------------------------------------------------------------------------
  // Input argument definitions
  // ---------------------------------------------------------------------------

  private val CreateDbArg = Argument("input", CreateDatabaseInput)
  private val UpdateDbArg = Argument("input", UpdateDatabaseInput)
  private val CreateSchemaArg = Argument("input", CreateSchemaInput)
  private val UpdateSchemaArg = Argument("input", UpdateSchemaInput)
  private val CreateTableArg = Argument("input", CreateTableInput)
  private val UpdateTableArg = Argument("input", UpdateTableInput)
  private val CreateColumnArg = Argument("input", CreateColumnInput)
  private val UpdateColumnArg = Argument("input", UpdateColumnInput)
  private val IdArg = Argument("id", UUIDType, description = "Entity identifier")

  // ---------------------------------------------------------------------------
  // Helper to extract fields from Sangria's default input map
  // ---------------------------------------------------------------------------

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def optField[A](m: Map[String, Any], key: String): Option[A] =
    m.get(key).flatMap(Option(_)).map(_.asInstanceOf[A])

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def reqField[A](m: Map[String, Any], key: String): A =
    m(key).asInstanceOf[A]

  // ---------------------------------------------------------------------------
  // Mutation type
  // ---------------------------------------------------------------------------

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  val MutationType: ObjectType[GraphQLContext, Unit] = ObjectType(
    "Mutation",
    "UDPS catalog mutation operations",
    fields[GraphQLContext, Unit](
      // --- Database CRUD ---
      Field(
        "createDatabase",
        DatabaseType,
        description = Some("Create a new database entry in the catalog"),
        arguments = CreateDbArg :: Nil,
        resolve = ctx => {
          val m = ctx.arg(CreateDbArg).asInstanceOf[Map[String, Any]]
          val now = Instant.now()
          val db = CatalogDatabase(
            id = UUID.randomUUID(),
            name = reqField[String](m, "name"),
            description = optField[String](m, "description"),
            createdAt = now,
            updatedAt = now
          )
          ctx.ctx.runIO(ctx.ctx.metadataRepository.createDatabase(db))
        }
      ),
      Field(
        "updateDatabase",
        DatabaseType,
        description = Some("Update an existing database entry"),
        arguments = UpdateDbArg :: Nil,
        resolve = ctx => {
          val m = ctx.arg(UpdateDbArg).asInstanceOf[Map[String, Any]]
          val id = reqField[UUID](m, "id")
          ctx.ctx.runIO(
            ctx.ctx.metadataRepository.getDatabase(id).flatMap {
              case Some(existing) =>
                val updated = existing.copy(
                  name = optField[String](m, "name").getOrElse(existing.name),
                  description = m.get("description") match {
                    case Some(null) => None // scalastyle:ignore null
                    case Some(v)    => Some(v.asInstanceOf[String])
                    case None       => existing.description
                  }
                )
                ctx.ctx.metadataRepository.updateDatabase(updated)
              case None =>
                cats.effect.IO.raiseError(new NoSuchElementException(s"Database $id not found"))
            }
          )
        }
      ),
      Field(
        "deleteDatabase",
        BooleanType,
        description = Some("Delete a database entry by identifier"),
        arguments = IdArg :: Nil,
        resolve = ctx => {
          val id = ctx.arg(IdArg)
          ctx.ctx.runIO(ctx.ctx.metadataRepository.deleteDatabase(id).map(_ => true))
        }
      ),
      // --- Schema CRUD ---
      Field(
        "createSchema",
        SchemaType,
        description = Some("Create a new schema entry in the catalog"),
        arguments = CreateSchemaArg :: Nil,
        resolve = ctx => {
          val m = ctx.arg(CreateSchemaArg).asInstanceOf[Map[String, Any]]
          val now = Instant.now()
          val s = CatalogSchema(
            id = UUID.randomUUID(),
            databaseId = reqField[UUID](m, "databaseId"),
            name = reqField[String](m, "name"),
            createdAt = now,
            updatedAt = now
          )
          ctx.ctx.runIO(ctx.ctx.metadataRepository.createSchema(s))
        }
      ),
      Field(
        "updateSchema",
        SchemaType,
        description = Some("Update an existing schema entry"),
        arguments = UpdateSchemaArg :: Nil,
        resolve = ctx => {
          val m = ctx.arg(UpdateSchemaArg).asInstanceOf[Map[String, Any]]
          val id = reqField[UUID](m, "id")
          ctx.ctx.runIO(
            ctx.ctx.metadataRepository.getSchema(id).flatMap {
              case Some(existing) =>
                val updated = existing.copy(
                  name = optField[String](m, "name").getOrElse(existing.name)
                )
                ctx.ctx.metadataRepository.updateSchema(updated)
              case None =>
                cats.effect.IO.raiseError(new NoSuchElementException(s"Schema $id not found"))
            }
          )
        }
      ),
      Field(
        "deleteSchema",
        BooleanType,
        description = Some("Delete a schema entry by identifier"),
        arguments = IdArg :: Nil,
        resolve = ctx => {
          val id = ctx.arg(IdArg)
          ctx.ctx.runIO(ctx.ctx.metadataRepository.deleteSchema(id).map(_ => true))
        }
      ),
      // --- Table CRUD ---
      Field(
        "createTable",
        TableType,
        description = Some("Create a new table entry in the catalog"),
        arguments = CreateTableArg :: Nil,
        resolve = ctx => {
          val m = ctx.arg(CreateTableArg).asInstanceOf[Map[String, Any]]
          val now = Instant.now()
          val table = CatalogTable(
            id = UUID.randomUUID(),
            schemaId = reqField[UUID](m, "schemaId"),
            name = reqField[String](m, "name"),
            rowCount = 0L,
            sizeBytes = 0L,
            tier = reqField[String](m, "tier"),
            createdAt = now,
            updatedAt = now
          )
          ctx.ctx.runIO(ctx.ctx.metadataRepository.createTable(table))
        }
      ),
      Field(
        "updateTable",
        TableType,
        description = Some("Update an existing table entry"),
        arguments = UpdateTableArg :: Nil,
        resolve = ctx => {
          val m = ctx.arg(UpdateTableArg).asInstanceOf[Map[String, Any]]
          val id = reqField[UUID](m, "id")
          ctx.ctx.runIO(
            ctx.ctx.metadataRepository.getTable(id).flatMap {
              case Some(existing) =>
                val updated = existing.copy(
                  name = optField[String](m, "name").getOrElse(existing.name),
                  rowCount = optField[Long](m, "rowCount").getOrElse(existing.rowCount),
                  sizeBytes = optField[Long](m, "sizeBytes").getOrElse(existing.sizeBytes),
                  tier = optField[String](m, "tier").getOrElse(existing.tier)
                )
                ctx.ctx.metadataRepository.updateTable(updated)
              case None =>
                cats.effect.IO.raiseError(new NoSuchElementException(s"Table $id not found"))
            }
          )
        }
      ),
      Field(
        "deleteTable",
        BooleanType,
        description = Some("Delete a table entry by identifier"),
        arguments = IdArg :: Nil,
        resolve = ctx => {
          val id = ctx.arg(IdArg)
          ctx.ctx.runIO(ctx.ctx.metadataRepository.deleteTable(id).map(_ => true))
        }
      ),
      // --- Column CRUD ---
      Field(
        "createColumn",
        ColumnType,
        description = Some("Create a new column entry in the catalog"),
        arguments = CreateColumnArg :: Nil,
        resolve = ctx => {
          val m = ctx.arg(CreateColumnArg).asInstanceOf[Map[String, Any]]
          val column = CatalogColumn(
            id = UUID.randomUUID(),
            tableId = reqField[UUID](m, "tableId"),
            name = reqField[String](m, "name"),
            dataType = reqField[String](m, "dataType"),
            nullable = reqField[Boolean](m, "nullable"),
            indexed = reqField[Boolean](m, "indexed"),
            ftsEnabled = reqField[Boolean](m, "ftsEnabled"),
            piiClassified = reqField[Boolean](m, "piiClassified"),
            ordinalPosition = reqField[Int](m, "ordinalPosition")
          )
          ctx.ctx.runIO(ctx.ctx.metadataRepository.createColumn(column))
        }
      ),
      Field(
        "updateColumn",
        ColumnType,
        description = Some("Update an existing column entry"),
        arguments = UpdateColumnArg :: Nil,
        resolve = ctx => {
          val m = ctx.arg(UpdateColumnArg).asInstanceOf[Map[String, Any]]
          val id = reqField[UUID](m, "id")
          ctx.ctx.runIO(
            ctx.ctx.metadataRepository.getColumn(id).flatMap {
              case Some(existing) =>
                val updated = existing.copy(
                  name = optField[String](m, "name").getOrElse(existing.name),
                  dataType = optField[String](m, "dataType").getOrElse(existing.dataType),
                  nullable = optField[Boolean](m, "nullable").getOrElse(existing.nullable),
                  indexed = optField[Boolean](m, "indexed").getOrElse(existing.indexed),
                  ftsEnabled = optField[Boolean](m, "ftsEnabled").getOrElse(existing.ftsEnabled),
                  piiClassified = optField[Boolean](m, "piiClassified").getOrElse(existing.piiClassified),
                  ordinalPosition = optField[Int](m, "ordinalPosition").getOrElse(existing.ordinalPosition)
                )
                ctx.ctx.metadataRepository.updateColumn(updated)
              case None =>
                cats.effect.IO.raiseError(new NoSuchElementException(s"Column $id not found"))
            }
          )
        }
      ),
      Field(
        "deleteColumn",
        BooleanType,
        description = Some("Delete a column entry by identifier"),
        arguments = IdArg :: Nil,
        resolve = ctx => {
          val id = ctx.arg(IdArg)
          ctx.ctx.runIO(ctx.ctx.metadataRepository.deleteColumn(id).map(_ => true))
        }
      )
    )
  )
}
