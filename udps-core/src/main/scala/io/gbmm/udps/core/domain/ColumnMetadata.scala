package io.gbmm.udps.core.domain

final case class ColumnStatistics(
  nullCount: Long,
  distinctCount: Long,
  minValue: Option[String],
  maxValue: Option[String],
  avgSize: Option[Long]
)

final case class ColumnMetadata(
  name: String,
  dataType: DataType,
  nullable: Boolean,
  description: Option[String],
  tags: Map[String, String],
  statistics: Option[ColumnStatistics]
)
