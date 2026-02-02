package io.gbmm.udps.query.calcite

import io.gbmm.udps.core.domain.{ColumnMetadata, DataType}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.sql.`type`.SqlTypeName

/** Bidirectional mapping between UDPS DataType and Calcite RelDataType / SqlTypeName. */
object TypeMapper {

  private val DEFAULT_TIMESTAMP_PRECISION = 0
  private val MILLIS_TIMESTAMP_PRECISION = 3
  private val MICROS_TIMESTAMP_PRECISION = 6
  private val NANOS_TIMESTAMP_PRECISION = 9

  /** Convert a UDPS DataType to a Calcite RelDataType using the provided factory. */
  def toRelDataType(dataType: DataType, nullable: Boolean, factory: RelDataTypeFactory): RelDataType = {
    val baseType = dataType match {
      case DataType.Boolean        => factory.createSqlType(SqlTypeName.BOOLEAN)
      case DataType.Int8           => factory.createSqlType(SqlTypeName.TINYINT)
      case DataType.Int16          => factory.createSqlType(SqlTypeName.SMALLINT)
      case DataType.Int32          => factory.createSqlType(SqlTypeName.INTEGER)
      case DataType.Int64          => factory.createSqlType(SqlTypeName.BIGINT)
      case DataType.UInt8          => factory.createSqlType(SqlTypeName.SMALLINT)
      case DataType.UInt16         => factory.createSqlType(SqlTypeName.INTEGER)
      case DataType.UInt32         => factory.createSqlType(SqlTypeName.BIGINT)
      case DataType.UInt64         => factory.createSqlType(SqlTypeName.DECIMAL, 20, 0)
      case DataType.Float16        => factory.createSqlType(SqlTypeName.FLOAT)
      case DataType.Float32        => factory.createSqlType(SqlTypeName.FLOAT)
      case DataType.Float64        => factory.createSqlType(SqlTypeName.DOUBLE)
      case DataType.Decimal(p, s)  => factory.createSqlType(SqlTypeName.DECIMAL, p, s)
      case DataType.Utf8           => factory.createSqlType(SqlTypeName.VARCHAR)
      case DataType.Binary         => factory.createSqlType(SqlTypeName.VARBINARY)
      case DataType.Date32         => factory.createSqlType(SqlTypeName.DATE)
      case DataType.Date64         => factory.createSqlType(SqlTypeName.DATE)
      case DataType.TimestampSec   => factory.createSqlType(SqlTypeName.TIMESTAMP, DEFAULT_TIMESTAMP_PRECISION)
      case DataType.TimestampMillis => factory.createSqlType(SqlTypeName.TIMESTAMP, MILLIS_TIMESTAMP_PRECISION)
      case DataType.TimestampMicros => factory.createSqlType(SqlTypeName.TIMESTAMP, MICROS_TIMESTAMP_PRECISION)
      case DataType.TimestampNanos  => factory.createSqlType(SqlTypeName.TIMESTAMP, NANOS_TIMESTAMP_PRECISION)
      case DataType.List(elem)     =>
        val elemType = toRelDataType(elem, nullable = true, factory)
        factory.createArrayType(elemType, -1)
      case DataType.Struct(fields) =>
        val fieldTypes = fields.map(f => toRelDataType(f.dataType, f.nullable, factory))
        val fieldNames = fields.map(_.name)
        factory.createStructType(
          java.util.Arrays.asList(fieldTypes: _*),
          java.util.Arrays.asList(fieldNames: _*)
        )
      case DataType.Map(k, v)     =>
        val keyType = toRelDataType(k, nullable = false, factory)
        val valType = toRelDataType(v, nullable = true, factory)
        factory.createMapType(keyType, valType)
      case DataType.Null           => factory.createSqlType(SqlTypeName.NULL)
    }
    factory.createTypeWithNullability(baseType, nullable)
  }

  /** Convert a Calcite SqlTypeName back to a UDPS DataType.
    *
    * For complex types (List, Struct, Map) and parameterized types (Decimal, Timestamp),
    * the full RelDataType is required to recover nested structure.
    */
  def fromRelDataType(relDataType: RelDataType): DataType = {
    relDataType.getSqlTypeName match {
      case SqlTypeName.BOOLEAN   => DataType.Boolean
      case SqlTypeName.TINYINT   => DataType.Int8
      case SqlTypeName.SMALLINT  => DataType.Int16
      case SqlTypeName.INTEGER   => DataType.Int32
      case SqlTypeName.BIGINT    => DataType.Int64
      case SqlTypeName.FLOAT     => DataType.Float32
      case SqlTypeName.REAL      => DataType.Float32
      case SqlTypeName.DOUBLE    => DataType.Float64
      case SqlTypeName.DECIMAL   =>
        DataType.Decimal(relDataType.getPrecision, relDataType.getScale)
      case SqlTypeName.VARCHAR | SqlTypeName.CHAR => DataType.Utf8
      case SqlTypeName.VARBINARY | SqlTypeName.BINARY => DataType.Binary
      case SqlTypeName.DATE      => DataType.Date32
      case SqlTypeName.TIMESTAMP | SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        relDataType.getPrecision match {
          case p if p <= DEFAULT_TIMESTAMP_PRECISION => DataType.TimestampSec
          case p if p <= MILLIS_TIMESTAMP_PRECISION  => DataType.TimestampMillis
          case p if p <= MICROS_TIMESTAMP_PRECISION  => DataType.TimestampMicros
          case _                                      => DataType.TimestampNanos
        }
      case SqlTypeName.ARRAY     =>
        val componentType = relDataType.getComponentType
        DataType.List(fromRelDataType(componentType))
      case SqlTypeName.MAP       =>
        val keyType = relDataType.getKeyType
        val valueType = relDataType.getValueType
        DataType.Map(fromRelDataType(keyType), fromRelDataType(valueType))
      case SqlTypeName.ROW       =>
        val fieldList = relDataType.getFieldList
        val columns = new scala.collection.mutable.ArrayBuffer[ColumnMetadata](fieldList.size())
        val iter = fieldList.iterator()
        while (iter.hasNext) {
          val field = iter.next()
          columns += ColumnMetadata(
            name = field.getName,
            dataType = fromRelDataType(field.getType),
            nullable = field.getType.isNullable,
            description = None,
            tags = Map.empty,
            statistics = None
          )
        }
        DataType.Struct(columns.toSeq)
      case SqlTypeName.NULL      => DataType.Null
      case other                 =>
        throw new UnsupportedOperationException(
          s"Cannot map Calcite type '${other.getName}' to a UDPS DataType"
        )
    }
  }

  /** Build a Calcite row type from a sequence of UDPS ColumnMetadata. */
  def buildRowType(columns: Seq[ColumnMetadata], factory: RelDataTypeFactory): RelDataType = {
    val builder = factory.builder()
    columns.foreach { col =>
      builder.add(col.name, toRelDataType(col.dataType, col.nullable, factory))
    }
    builder.build()
  }
}
