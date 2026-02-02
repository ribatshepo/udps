package io.gbmm.udps.query.federation.adapters

import io.gbmm.udps.core.domain.DataType
import io.gbmm.udps.query.federation.{DataSourceConfig, DataSourceType}

import java.sql.Types

/** PostgreSQL-specific federation adapter.
  *
  * Extends the base JDBC adapter with PostgreSQL driver configuration and
  * type mappings for PostgreSQL-specific types such as JSONB, ARRAY, and UUID.
  */
final class PostgreSQLAdapter extends JdbcFederationAdapter {

  private val PG_DEFAULT_PORT = 5432

  override val sourceType: DataSourceType = DataSourceType.PostgreSQL

  override protected val driverClassName: String = "org.postgresql.Driver"

  override protected val defaultPort: Int = PG_DEFAULT_PORT

  override protected def buildJdbcUrl(config: DataSourceConfig): String =
    s"jdbc:postgresql://${config.host}:${config.port}/${config.database}"

  override protected def mapJdbcType(
      jdbcType: Int,
      typeName: String,
      precision: Int,
      scale: Int
  ): DataType = {
    val normalizedTypeName = typeName.toLowerCase
    normalizedTypeName match {
      case "jsonb" | "json"       => DataType.Utf8
      case "uuid"                 => DataType.Utf8
      case "bytea"                => DataType.Binary
      case "text"                 => DataType.Utf8
      case "int2"                 => DataType.Int16
      case "int4"                 => DataType.Int32
      case "int8"                 => DataType.Int64
      case "float4"               => DataType.Float32
      case "float8"               => DataType.Float64
      case "bool"                 => DataType.Boolean
      case "timestamptz"          => DataType.TimestampMicros
      case "timestamp"            => DataType.TimestampMicros
      case "date"                 => DataType.Date32
      case "interval"             => DataType.Utf8
      case "inet" | "cidr"        => DataType.Utf8
      case "macaddr" | "macaddr8" => DataType.Utf8
      case "money"                => DataType.Decimal(19, 2)
      case "serial" | "serial4"   => DataType.Int32
      case "bigserial" | "serial8" => DataType.Int64
      case "smallserial" | "serial2" => DataType.Int16
      case n if n.startsWith("_") =>
        val elementTypeName = n.substring(1)
        val elementType = mapPostgresElementType(elementTypeName)
        DataType.List(elementType)
      case _ if jdbcType == Types.ARRAY =>
        DataType.List(DataType.Utf8)
      case _ =>
        super.mapJdbcType(jdbcType, typeName, precision, scale)
    }
  }

  private def mapPostgresElementType(elementTypeName: String): DataType =
    elementTypeName match {
      case "int2"              => DataType.Int16
      case "int4"              => DataType.Int32
      case "int8"              => DataType.Int64
      case "float4"            => DataType.Float32
      case "float8"            => DataType.Float64
      case "bool"              => DataType.Boolean
      case "text" | "varchar"  => DataType.Utf8
      case "uuid"              => DataType.Utf8
      case "timestamp" | "timestamptz" => DataType.TimestampMicros
      case _                   => DataType.Utf8
    }
}

object PostgreSQLAdapter {
  def apply(): PostgreSQLAdapter = new PostgreSQLAdapter()
}
