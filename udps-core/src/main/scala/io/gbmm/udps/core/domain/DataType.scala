package io.gbmm.udps.core.domain

sealed trait DataType extends Product with Serializable

object DataType {
  case object Boolean extends DataType
  case object Int8 extends DataType
  case object Int16 extends DataType
  case object Int32 extends DataType
  case object Int64 extends DataType
  case object UInt8 extends DataType
  case object UInt16 extends DataType
  case object UInt32 extends DataType
  case object UInt64 extends DataType
  case object Float16 extends DataType
  case object Float32 extends DataType
  case object Float64 extends DataType
  final case class Decimal(precision: Int, scale: Int) extends DataType
  case object Utf8 extends DataType
  case object Binary extends DataType
  case object Date32 extends DataType
  case object Date64 extends DataType
  case object TimestampSec extends DataType
  case object TimestampMillis extends DataType
  case object TimestampMicros extends DataType
  case object TimestampNanos extends DataType
  final case class List(elementType: DataType) extends DataType
  final case class Struct(fields: Seq[ColumnMetadata]) extends DataType
  final case class Map(keyType: DataType, valueType: DataType) extends DataType
  case object Null extends DataType
}
