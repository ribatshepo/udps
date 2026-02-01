package io.gbmm.udps.core.domain

import java.time.Instant

final case class SchemaInfo(
  columns: Seq[ColumnMetadata],
  primaryKey: Option[Seq[String]],
  partitionKeys: Seq[String],
  version: Int,
  createdAt: Instant,
  updatedAt: Instant
)
