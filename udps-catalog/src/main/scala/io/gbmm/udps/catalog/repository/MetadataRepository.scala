package io.gbmm.udps.catalog.repository

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import cats.effect.kernel.Resource
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.hikari.HikariTransactor

/** Configuration for the catalog database connection pool. */
final case class CatalogDbConfig(
    jdbcUrl: String,
    username: String,
    password: String,
    maximumPoolSize: Int,
    minimumIdle: Int,
    connectionTimeoutMs: Long
)

object CatalogDbConfig {
  private val defaultMaxPoolSize = 10
  private val defaultMinIdle = 2
  private val defaultConnTimeoutMs = 30000L

  def apply(jdbcUrl: String, username: String, password: String): CatalogDbConfig =
    CatalogDbConfig(
      jdbcUrl = jdbcUrl,
      username = username,
      password = password,
      maximumPoolSize = defaultMaxPoolSize,
      minimumIdle = defaultMinIdle,
      connectionTimeoutMs = defaultConnTimeoutMs
    )
}

/** Trait defining CRUD operations for catalog metadata entities. */
trait MetadataRepository {

  // --- Databases ---
  def createDatabase(db: CatalogDatabase): IO[CatalogDatabase]
  def getDatabase(id: UUID): IO[Option[CatalogDatabase]]
  def getDatabaseByName(name: String): IO[Option[CatalogDatabase]]
  def listDatabases: IO[List[CatalogDatabase]]
  def updateDatabase(db: CatalogDatabase): IO[CatalogDatabase]
  def deleteDatabase(id: UUID): IO[Unit]

  // --- Schemas ---
  def createSchema(schema: CatalogSchema): IO[CatalogSchema]
  def getSchema(id: UUID): IO[Option[CatalogSchema]]
  def listSchemasByDatabase(databaseId: UUID): IO[List[CatalogSchema]]
  def updateSchema(schema: CatalogSchema): IO[CatalogSchema]
  def deleteSchema(id: UUID): IO[Unit]

  // --- Tables ---
  def createTable(table: CatalogTable): IO[CatalogTable]
  def getTable(id: UUID): IO[Option[CatalogTable]]
  def listTablesBySchema(schemaId: UUID): IO[List[CatalogTable]]
  def updateTable(table: CatalogTable): IO[CatalogTable]
  def deleteTable(id: UUID): IO[Unit]

  // --- Columns ---
  def createColumn(column: CatalogColumn): IO[CatalogColumn]
  def getColumn(id: UUID): IO[Option[CatalogColumn]]
  def listColumnsByTable(tableId: UUID): IO[List[CatalogColumn]]
  def updateColumn(column: CatalogColumn): IO[CatalogColumn]
  def deleteColumn(id: UUID): IO[Unit]
}

/** Doobie-backed implementation of MetadataRepository. */
final class DoobieMetadataRepository(xa: Transactor[IO])
    extends MetadataRepository
    with LazyLogging {

  // --- Databases ---

  override def createDatabase(db: CatalogDatabase): IO[CatalogDatabase] =
    sql"""INSERT INTO databases (id, name, description, created_at, updated_at)
          VALUES (${db.id}, ${db.name}, ${db.description}, ${db.createdAt}, ${db.updatedAt})"""
      .update.run.transact(xa)
      .flatMap { count =>
        if (count == 1) IO.pure(db)
        else IO.raiseError(new RuntimeException(s"Failed to insert database ${db.id}, affected rows: $count"))
      }
      .handleErrorWith { e =>
        logger.error(s"Error creating database ${db.name}", e)
        handleConstraintViolation(e, s"Database with name '${db.name}' already exists")
      }

  override def getDatabase(id: UUID): IO[Option[CatalogDatabase]] =
    sql"""SELECT id, name, description, created_at, updated_at
          FROM databases WHERE id = $id"""
      .query[CatalogDatabase].option.transact(xa)

  override def getDatabaseByName(name: String): IO[Option[CatalogDatabase]] =
    sql"""SELECT id, name, description, created_at, updated_at
          FROM databases WHERE name = $name"""
      .query[CatalogDatabase].option.transact(xa)

  override def listDatabases: IO[List[CatalogDatabase]] =
    sql"""SELECT id, name, description, created_at, updated_at
          FROM databases ORDER BY name"""
      .query[CatalogDatabase].to[List].transact(xa)

  override def updateDatabase(db: CatalogDatabase): IO[CatalogDatabase] = {
    val now = Instant.now()
    val updated = db.copy(updatedAt = now)
    sql"""UPDATE databases SET name = ${updated.name}, description = ${updated.description},
          updated_at = ${updated.updatedAt} WHERE id = ${updated.id}"""
      .update.run.transact(xa)
      .flatMap { count =>
        if (count == 1) IO.pure(updated)
        else IO.raiseError(new RuntimeException(s"Database ${db.id} not found for update"))
      }
      .handleErrorWith { e =>
        logger.error(s"Error updating database ${db.id}", e)
        handleConstraintViolation(e, s"Database name '${db.name}' conflicts with existing entry")
      }
  }

  override def deleteDatabase(id: UUID): IO[Unit] =
    sql"""DELETE FROM databases WHERE id = $id"""
      .update.run.transact(xa).void

  // --- Schemas ---

  override def createSchema(schema: CatalogSchema): IO[CatalogSchema] =
    sql"""INSERT INTO schemas (id, database_id, name, created_at, updated_at)
          VALUES (${schema.id}, ${schema.databaseId}, ${schema.name}, ${schema.createdAt}, ${schema.updatedAt})"""
      .update.run.transact(xa)
      .flatMap { count =>
        if (count == 1) IO.pure(schema)
        else IO.raiseError(new RuntimeException(s"Failed to insert schema ${schema.id}, affected rows: $count"))
      }
      .handleErrorWith { e =>
        logger.error(s"Error creating schema ${schema.name}", e)
        handleConstraintViolation(e, s"Schema '${schema.name}' already exists in this database")
      }

  override def getSchema(id: UUID): IO[Option[CatalogSchema]] =
    sql"""SELECT id, database_id, name, created_at, updated_at
          FROM schemas WHERE id = $id"""
      .query[CatalogSchema].option.transact(xa)

  override def listSchemasByDatabase(databaseId: UUID): IO[List[CatalogSchema]] =
    sql"""SELECT id, database_id, name, created_at, updated_at
          FROM schemas WHERE database_id = $databaseId ORDER BY name"""
      .query[CatalogSchema].to[List].transact(xa)

  override def updateSchema(schema: CatalogSchema): IO[CatalogSchema] = {
    val now = Instant.now()
    val updated = schema.copy(updatedAt = now)
    sql"""UPDATE schemas SET name = ${updated.name}, updated_at = ${updated.updatedAt}
          WHERE id = ${updated.id}"""
      .update.run.transact(xa)
      .flatMap { count =>
        if (count == 1) IO.pure(updated)
        else IO.raiseError(new RuntimeException(s"Schema ${schema.id} not found for update"))
      }
      .handleErrorWith { e =>
        logger.error(s"Error updating schema ${schema.id}", e)
        handleConstraintViolation(e, s"Schema name '${schema.name}' conflicts with existing entry")
      }
  }

  override def deleteSchema(id: UUID): IO[Unit] =
    sql"""DELETE FROM schemas WHERE id = $id"""
      .update.run.transact(xa).void

  // --- Tables ---

  override def createTable(table: CatalogTable): IO[CatalogTable] =
    sql"""INSERT INTO tables (id, schema_id, name, row_count, size_bytes, tier, created_at, updated_at)
          VALUES (${table.id}, ${table.schemaId}, ${table.name}, ${table.rowCount},
                  ${table.sizeBytes}, ${table.tier}, ${table.createdAt}, ${table.updatedAt})"""
      .update.run.transact(xa)
      .flatMap { count =>
        if (count == 1) IO.pure(table)
        else IO.raiseError(new RuntimeException(s"Failed to insert table ${table.id}, affected rows: $count"))
      }
      .handleErrorWith { e =>
        logger.error(s"Error creating table ${table.name}", e)
        handleConstraintViolation(e, s"Table '${table.name}' already exists in this schema")
      }

  override def getTable(id: UUID): IO[Option[CatalogTable]] =
    sql"""SELECT id, schema_id, name, row_count, size_bytes, tier, created_at, updated_at
          FROM tables WHERE id = $id"""
      .query[CatalogTable].option.transact(xa)

  override def listTablesBySchema(schemaId: UUID): IO[List[CatalogTable]] =
    sql"""SELECT id, schema_id, name, row_count, size_bytes, tier, created_at, updated_at
          FROM tables WHERE schema_id = $schemaId ORDER BY name"""
      .query[CatalogTable].to[List].transact(xa)

  override def updateTable(table: CatalogTable): IO[CatalogTable] = {
    val now = Instant.now()
    val updated = table.copy(updatedAt = now)
    sql"""UPDATE tables SET name = ${updated.name}, row_count = ${updated.rowCount},
          size_bytes = ${updated.sizeBytes}, tier = ${updated.tier},
          updated_at = ${updated.updatedAt} WHERE id = ${updated.id}"""
      .update.run.transact(xa)
      .flatMap { count =>
        if (count == 1) IO.pure(updated)
        else IO.raiseError(new RuntimeException(s"Table ${table.id} not found for update"))
      }
      .handleErrorWith { e =>
        logger.error(s"Error updating table ${table.id}", e)
        handleConstraintViolation(e, s"Table name '${table.name}' conflicts with existing entry")
      }
  }

  override def deleteTable(id: UUID): IO[Unit] =
    sql"""DELETE FROM tables WHERE id = $id"""
      .update.run.transact(xa).void

  // --- Columns ---

  override def createColumn(column: CatalogColumn): IO[CatalogColumn] =
    sql"""INSERT INTO columns (id, table_id, name, data_type, nullable, indexed, fts_enabled,
          pii_classified, ordinal_position)
          VALUES (${column.id}, ${column.tableId}, ${column.name}, ${column.dataType},
                  ${column.nullable}, ${column.indexed}, ${column.ftsEnabled},
                  ${column.piiClassified}, ${column.ordinalPosition})"""
      .update.run.transact(xa)
      .flatMap { count =>
        if (count == 1) IO.pure(column)
        else IO.raiseError(new RuntimeException(s"Failed to insert column ${column.id}, affected rows: $count"))
      }
      .handleErrorWith { e =>
        logger.error(s"Error creating column ${column.name}", e)
        handleConstraintViolation(e, s"Column '${column.name}' already exists in this table")
      }

  override def getColumn(id: UUID): IO[Option[CatalogColumn]] =
    sql"""SELECT id, table_id, name, data_type, nullable, indexed, fts_enabled,
          pii_classified, ordinal_position
          FROM columns WHERE id = $id"""
      .query[CatalogColumn].option.transact(xa)

  override def listColumnsByTable(tableId: UUID): IO[List[CatalogColumn]] =
    sql"""SELECT id, table_id, name, data_type, nullable, indexed, fts_enabled,
          pii_classified, ordinal_position
          FROM columns WHERE table_id = $tableId ORDER BY ordinal_position"""
      .query[CatalogColumn].to[List].transact(xa)

  override def updateColumn(column: CatalogColumn): IO[CatalogColumn] =
    sql"""UPDATE columns SET name = ${column.name}, data_type = ${column.dataType},
          nullable = ${column.nullable}, indexed = ${column.indexed},
          fts_enabled = ${column.ftsEnabled}, pii_classified = ${column.piiClassified},
          ordinal_position = ${column.ordinalPosition}
          WHERE id = ${column.id}"""
      .update.run.transact(xa)
      .flatMap { count =>
        if (count == 1) IO.pure(column)
        else IO.raiseError(new RuntimeException(s"Column ${column.id} not found for update"))
      }
      .handleErrorWith { e =>
        logger.error(s"Error updating column ${column.id}", e)
        handleConstraintViolation(e, s"Column name '${column.name}' conflicts with existing entry")
      }

  override def deleteColumn(id: UUID): IO[Unit] =
    sql"""DELETE FROM columns WHERE id = $id"""
      .update.run.transact(xa).void

  // --- Constraint violation handling ---

  private def handleConstraintViolation[A](e: Throwable, message: String): IO[A] =
    e match {
      case ex: org.postgresql.util.PSQLException
          if ex.getSQLState != null && ex.getSQLState.startsWith("23") =>
        IO.raiseError(new IllegalArgumentException(message, ex))
      case other =>
        IO.raiseError(other)
    }
}

object DoobieMetadataRepository {

  /** Creates a HikariCP-backed transactor as a cats-effect Resource. */
  def transactorResource(config: CatalogDbConfig): Resource[IO, HikariTransactor[IO]] =
    HikariTransactor.newHikariTransactor[IO](
      driverClassName = "org.postgresql.Driver",
      url = config.jdbcUrl,
      user = config.username,
      pass = config.password,
      connectEC = cats.effect.unsafe.IORuntime.global.compute
    ).evalTap { xa =>
      xa.configure { ds =>
        IO.delay {
          ds.setMaximumPoolSize(config.maximumPoolSize)
          ds.setMinimumIdle(config.minimumIdle)
          ds.setConnectionTimeout(config.connectionTimeoutMs)
        }
      }
    }

  /** Creates a MetadataRepository backed by HikariCP, as a Resource. */
  def resource(config: CatalogDbConfig): Resource[IO, MetadataRepository] =
    transactorResource(config).map(xa => new DoobieMetadataRepository(xa))
}
