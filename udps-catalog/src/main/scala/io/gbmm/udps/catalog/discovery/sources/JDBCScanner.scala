package io.gbmm.udps.catalog.discovery.sources

import cats.effect.{IO, Resource}
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.catalog.discovery._
import io.gbmm.udps.core.domain.DataType

import java.sql.{Connection, DatabaseMetaData, DriverManager, Types}
import scala.collection.mutable.ListBuffer

final class JDBCScanner extends DataSourceScanner with LazyLogging {

  override val sourceType: String = "jdbc"

  override def scan(config: ScanConfig): IO[DiscoveryResult] = {
    val schemaFilter = config.options.getOrElse("schema", "%")
    val tableFilter = config.options.getOrElse("tablePattern", "%")
    val username = config.options.getOrElse("username", "")
    val password = config.options.getOrElse("password", "")

    connectionResource(config.connectionString, username, password)
      .use { connection =>
        IO.blocking {
          val metadata = connection.getMetaData
          val tables = discoverTables(metadata, schemaFilter, tableFilter)
          DiscoveryResult(tables = tables, errors = Seq.empty)
        }
      }
      .handleErrorWith { ex =>
        IO.delay {
          logger.error("JDBC scan failed for {}", config.connectionString, ex)
          DiscoveryResult(tables = Seq.empty, errors = Seq(s"JDBC scan error: ${ex.getMessage}"))
        }
      }
  }

  private def connectionResource(url: String, username: String, password: String): Resource[IO, Connection] =
    Resource.make(
      IO.blocking(DriverManager.getConnection(url, username, password))
    )(conn => IO.blocking(conn.close()).handleErrorWith(_ => IO.unit))

  private def discoverTables(
    metadata: DatabaseMetaData,
    schemaFilter: String,
    tableFilter: String
  ): Seq[DiscoveredTable] = {
    val tableResults = metadata.getTables(null, schemaFilter, tableFilter, Array("TABLE", "VIEW"))
    val tables = ListBuffer.empty[DiscoveredTable]
    try {
      while (tableResults.next()) {
        val catalog = Option(tableResults.getString("TABLE_CAT")).getOrElse("")
        val schema = Option(tableResults.getString("TABLE_SCHEM")).getOrElse("")
        val tableName = tableResults.getString("TABLE_NAME")
        val qualifiedName = Seq(catalog, schema, tableName).filter(_.nonEmpty).mkString(".")

        val columns = discoverColumns(metadata, catalog, schema, tableName)
        val primaryKey = discoverPrimaryKey(metadata, catalog, schema, tableName)

        tables += DiscoveredTable(
          name = qualifiedName,
          columns = columns,
          rowCount = None,
          primaryKey = if (primaryKey.nonEmpty) Some(primaryKey) else None
        )
      }
    } finally {
      tableResults.close()
    }
    tables.toSeq
  }

  private def discoverColumns(
    metadata: DatabaseMetaData,
    catalog: String,
    schema: String,
    tableName: String
  ): Seq[DiscoveredColumn] = {
    val catParam = if (catalog.isEmpty) null else catalog
    val schemaParam = if (schema.isEmpty) null else schema
    val columnResults = metadata.getColumns(catParam, schemaParam, tableName, "%")
    val columns = ListBuffer.empty[DiscoveredColumn]
    try {
      while (columnResults.next()) {
        val colName = columnResults.getString("COLUMN_NAME")
        val jdbcType = columnResults.getInt("DATA_TYPE")
        val nullable = columnResults.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls
        val ordinal = columnResults.getInt("ORDINAL_POSITION")
        val precision = columnResults.getInt("COLUMN_SIZE")
        val scale = columnResults.getInt("DECIMAL_DIGITS")

        columns += DiscoveredColumn(
          name = colName,
          dataType = mapJdbcType(jdbcType, precision, scale),
          nullable = nullable,
          ordinalPosition = ordinal
        )
      }
    } finally {
      columnResults.close()
    }
    columns.toSeq
  }

  private def discoverPrimaryKey(
    metadata: DatabaseMetaData,
    catalog: String,
    schema: String,
    tableName: String
  ): Seq[String] = {
    val catParam = if (catalog.isEmpty) null else catalog
    val schemaParam = if (schema.isEmpty) null else schema
    val pkResults = metadata.getPrimaryKeys(catParam, schemaParam, tableName)
    val keys = ListBuffer.empty[(String, Short)]
    try {
      while (pkResults.next()) {
        val colName = pkResults.getString("COLUMN_NAME")
        val keySeq = pkResults.getShort("KEY_SEQ")
        keys += ((colName, keySeq))
      }
    } finally {
      pkResults.close()
    }
    keys.sortBy(_._2).map(_._1).toSeq
  }

  private def mapJdbcType(jdbcType: Int, precision: Int, scale: Int): DataType =
    jdbcType match {
      case Types.BIT | Types.BOOLEAN => DataType.Boolean
      case Types.TINYINT             => DataType.Int8
      case Types.SMALLINT            => DataType.Int16
      case Types.INTEGER             => DataType.Int32
      case Types.BIGINT              => DataType.Int64
      case Types.REAL                => DataType.Float32
      case Types.FLOAT | Types.DOUBLE => DataType.Float64
      case Types.NUMERIC | Types.DECIMAL =>
        DataType.Decimal(precision = precision, scale = scale)
      case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR | Types.NCHAR |
           Types.NVARCHAR | Types.LONGNVARCHAR | Types.CLOB | Types.NCLOB =>
        DataType.Utf8
      case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY | Types.BLOB =>
        DataType.Binary
      case Types.DATE                => DataType.Date32
      case Types.TIME | Types.TIME_WITH_TIMEZONE =>
        DataType.TimestampMillis
      case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE =>
        DataType.TimestampMicros
      case _                         => DataType.Utf8
    }
}
