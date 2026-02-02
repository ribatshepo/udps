package io.gbmm.udps.query.federation.adapters

import io.gbmm.udps.core.domain.DataType
import io.gbmm.udps.query.federation.{DataSourceConfig, DataSourceType}

/** MySQL-specific federation adapter.
  *
  * Extends the base JDBC adapter with MySQL driver configuration and
  * type mappings for MySQL-specific types such as TINYINT, ENUM, and SET.
  */
final class MySQLAdapter extends JdbcFederationAdapter {

  private val MYSQL_DEFAULT_PORT = 3306

  override val sourceType: DataSourceType = DataSourceType.MySQL

  override protected val driverClassName: String = "com.mysql.cj.jdbc.Driver"

  override protected val defaultPort: Int = MYSQL_DEFAULT_PORT

  override protected def buildJdbcUrl(config: DataSourceConfig): String = {
    val baseUrl = s"jdbc:mysql://${config.host}:${config.port}/${config.database}"
    val params = config.properties.get("connectionParams").getOrElse("useSSL=false&serverTimezone=UTC")
    s"$baseUrl?$params"
  }

  override protected def mapJdbcType(
      jdbcType: Int,
      typeName: String,
      precision: Int,
      scale: Int
  ): DataType = {
    val normalizedTypeName = typeName.toUpperCase
    normalizedTypeName match {
      case "TINYINT" =>
        if (precision == 1) DataType.Boolean
        else DataType.Int8
      case "TINYINT UNSIGNED"   => DataType.UInt8
      case "SMALLINT UNSIGNED"  => DataType.UInt16
      case "MEDIUMINT"          => DataType.Int32
      case "MEDIUMINT UNSIGNED" => DataType.UInt32
      case "INT UNSIGNED"       => DataType.UInt32
      case "BIGINT UNSIGNED"    => DataType.UInt64
      case "ENUM"               => DataType.Utf8
      case "SET"                => DataType.Utf8
      case "JSON"               => DataType.Utf8
      case "YEAR"               => DataType.Int16
      case "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" |
           "MULTIPOINT" | "MULTILINESTRING" | "MULTIPOLYGON" |
           "GEOMETRYCOLLECTION" =>
        DataType.Binary
      case "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" =>
        DataType.Utf8
      case "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" =>
        DataType.Binary
      case "DATETIME" =>
        DataType.TimestampMicros
      case _ =>
        super.mapJdbcType(jdbcType, typeName, precision, scale)
    }
  }
}

object MySQLAdapter {
  def apply(): MySQLAdapter = new MySQLAdapter()
}
