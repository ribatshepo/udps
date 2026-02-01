package io.gbmm

package object udps {
  // Re-export core domain types for convenience
  type DataType = core.domain.DataType
  type CompressionCodec = core.domain.CompressionCodec
  type StorageTier = core.domain.StorageTier
  type IndexType = core.domain.IndexType
  type ColumnMetadata = core.domain.ColumnMetadata
  type ColumnStatistics = core.domain.ColumnStatistics
  type SchemaInfo = core.domain.SchemaInfo
  type TableMetadata = core.domain.TableMetadata
  type PartitionInfo = core.domain.PartitionInfo
  type QueryPlan = core.domain.QueryPlan
}
