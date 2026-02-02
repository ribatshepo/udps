package io.gbmm.udps.query.federation.adapters

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.core.domain.DataType
import io.gbmm.udps.query.federation._
import org.apache.calcite.adapter.jdbc.JdbcSchema
import org.apache.calcite.schema.Schema

import java.sql.{Connection, DatabaseMetaData, DriverManager, Types}
import javax.sql.DataSource

/** Base JDBC federation adapter shared by relational databases (PostgreSQL, MySQL).
  *
  * Subclasses must provide the JDBC driver class, URL format, and any source-specific
  * type mapping overrides.
  */
abstract class JdbcFederationAdapter extends FederationAdapter with LazyLogging {

  /** JDBC driver fully qualified class name (e.g., "org.postgresql.Driver"). */
  protected def driverClassName: String

  /** Build a JDBC URL from the given configuration. */
  protected def buildJdbcUrl(config: DataSourceConfig): String

  /** Default port for this database when the config specifies port 0. */
  protected def defaultPort: Int

  /** Map a JDBC type and type name to a UDPS DataType.
    *
    * The base implementation handles standard SQL types. Subclasses can override
    * to handle database-specific types before falling back to `super`.
    */
  protected def mapJdbcType(jdbcType: Int, typeName: String, precision: Int, scale: Int): DataType =
    jdbcType match {
      case Types.BIT | Types.BOOLEAN                       => DataType.Boolean
      case Types.TINYINT                                    => DataType.Int8
      case Types.SMALLINT                                   => DataType.Int16
      case Types.INTEGER                                    => DataType.Int32
      case Types.BIGINT                                     => DataType.Int64
      case Types.FLOAT | Types.REAL                         => DataType.Float32
      case Types.DOUBLE                                     => DataType.Float64
      case Types.NUMERIC | Types.DECIMAL                    =>
        if (precision > 0) DataType.Decimal(precision, scale)
        else DataType.Decimal(38, 18)
      case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR | Types.NCHAR | Types.NVARCHAR | Types.LONGNVARCHAR =>
        DataType.Utf8
      case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY | Types.BLOB =>
        DataType.Binary
      case Types.DATE                                       => DataType.Date32
      case Types.TIME | Types.TIME_WITH_TIMEZONE             => DataType.TimestampMillis
      case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE   => DataType.TimestampMicros
      case Types.CLOB | Types.NCLOB                         => DataType.Utf8
      case Types.ARRAY                                      => DataType.List(DataType.Utf8)
      case _                                                 => DataType.Utf8
    }

  override val pushdownCapabilities: PushdownCapabilities = PushdownCapabilities(
    predicates = true,
    projections = true,
    limits = true,
    aggregations = true,
    joins = false
  )

  override def connect(config: DataSourceConfig): IO[FederatedConnection] = IO {
    Class.forName(driverClassName)
    val effectivePort = if (config.port > 0) config.port else defaultPort
    val effectiveConfig = config.copy(port = effectivePort)
    val url = buildJdbcUrl(effectiveConfig)
    val props = new java.util.Properties()
    config.username.foreach(u => props.setProperty("user", u))
    config.password.foreach(p => props.setProperty("password", p))
    config.properties.foreach { case (k, v) => props.setProperty(k, v) }
    val conn = DriverManager.getConnection(url, props)
    logger.info(s"JDBC connection established to ${config.host}:$effectivePort/${config.database}")
    new JdbcFederatedConnection(conn, sourceType)
  }

  override def discoverTables(connection: FederatedConnection): IO[Seq[FederatedTableInfo]] = IO {
    val jdbcConn = connection.asInstanceOf[JdbcFederatedConnection].underlying
    val dbMeta: DatabaseMetaData = jdbcConn.getMetaData
    val catalog = jdbcConn.getCatalog
    val tablesRs = dbMeta.getTables(catalog, null, "%", Array("TABLE", "VIEW"))
    val tables = new scala.collection.mutable.ArrayBuffer[FederatedTableInfo]()

    while (tablesRs.next()) {
      val tableName = tablesRs.getString("TABLE_NAME")
      val schemaName = tablesRs.getString("TABLE_SCHEM")
      val qualifiedName = Option(schemaName).filter(_.nonEmpty) match {
        case Some(s) => s"$s.$tableName"
        case None    => tableName
      }

      val columnsRs = dbMeta.getColumns(catalog, schemaName, tableName, "%")
      val columns = new scala.collection.mutable.ArrayBuffer[FederatedColumnInfo]()
      while (columnsRs.next()) {
        val colName = columnsRs.getString("COLUMN_NAME")
        val jdbcType = columnsRs.getInt("DATA_TYPE")
        val typeName = columnsRs.getString("TYPE_NAME")
        val precision = columnsRs.getInt("COLUMN_SIZE")
        val scale = columnsRs.getInt("DECIMAL_DIGITS")
        columns += FederatedColumnInfo(colName, mapJdbcType(jdbcType, typeName, precision, scale))
      }
      columnsRs.close()

      val rowCount = estimateRowCount(jdbcConn, qualifiedName)
      tables += FederatedTableInfo(qualifiedName, columns.toSeq, rowCount, sourceType)
    }
    tablesRs.close()

    logger.info(s"Discovered ${tables.size} tables from ${sourceType}")
    tables.toSeq
  }

  override def createCalciteSchema(connection: FederatedConnection): IO[Schema] = IO {
    val jdbcConn = connection.asInstanceOf[JdbcFederatedConnection].underlying
    val dataSource: DataSource = new SingleConnectionDataSource(jdbcConn)
    JdbcSchema.create(
      null,
      sourceType.toString.toLowerCase,
      dataSource,
      jdbcConn.getCatalog,
      null
    )
  }

  /** Estimate row count for a table. Returns -1 if estimation fails. */
  private def estimateRowCount(conn: Connection, tableName: String): Long =
    try {
      val stmt = conn.createStatement()
      try {
        val escapedName = tableName.replace("'", "''")
        val rs = stmt.executeQuery(s"""SELECT COUNT(*) FROM "$escapedName"""")
        if (rs.next()) rs.getLong(1) else -1L
      } finally {
        stmt.close()
      }
    } catch {
      case _: Exception => -1L
    }
}

/** Wraps a java.sql.Connection as a FederatedConnection. */
final class JdbcFederatedConnection(
    val underlying: Connection,
    val sourceType: DataSourceType
) extends FederatedConnection {

  override def isConnected: IO[Boolean] = IO {
    !underlying.isClosed && underlying.isValid(5)
  }

  override def close: IO[Unit] = IO {
    if (!underlying.isClosed) {
      underlying.close()
    }
  }

  override def metadata: Map[String, String] = {
    try {
      val dbMeta = underlying.getMetaData
      Map(
        "databaseProductName"    -> dbMeta.getDatabaseProductName,
        "databaseProductVersion" -> dbMeta.getDatabaseProductVersion,
        "driverName"             -> dbMeta.getDriverName,
        "driverVersion"          -> dbMeta.getDriverVersion,
        "url"                    -> dbMeta.getURL
      )
    } catch {
      case e: Exception =>
        Map("error" -> e.getMessage)
    }
  }
}

/** Minimal DataSource wrapper around an existing JDBC Connection.
  *
  * Used to bridge the Calcite JdbcSchema API which requires a DataSource,
  * while we manage connection lifecycle separately through FederatedConnection.
  */
private[adapters] final class SingleConnectionDataSource(conn: Connection)
    extends DataSource {

  override def getConnection: Connection = conn
  override def getConnection(username: String, password: String): Connection = conn
  override def getLogWriter: java.io.PrintWriter = null
  override def setLogWriter(out: java.io.PrintWriter): Unit = ()
  override def setLoginTimeout(seconds: Int): Unit = ()
  override def getLoginTimeout: Int = 0
  override def getParentLogger: java.util.logging.Logger =
    java.util.logging.Logger.getLogger(getClass.getName)
  override def unwrap[T](iface: Class[T]): T =
    if (iface.isInstance(this)) this.asInstanceOf[T]
    else throw new java.sql.SQLException(s"Cannot unwrap to ${iface.getName}")
  override def isWrapperFor(iface: Class[_]): Boolean = iface.isInstance(this)
}
