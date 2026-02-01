package io.gbmm.udps.core.domain

import java.time.Instant
import java.util.UUID

final case class IndexInfo(
  name: String,
  indexType: IndexType,
  columns: Seq[String],
  sizeBytes: Long
)

final case class TableMetadata(
  id: UUID,
  name: String,
  namespace: String,
  schema: SchemaInfo,
  storageTier: StorageTier,
  compression: CompressionCodec,
  partitionInfo: Option[PartitionInfo],
  indexes: Seq[IndexInfo],
  rowCount: Long,
  sizeBytes: Long,
  createdAt: Instant,
  updatedAt: Instant,
  properties: Map[String, String]
)
