package io.gbmm.udps.core.domain

sealed trait PartitionStrategy extends Product with Serializable

object PartitionStrategy {
  case object Hash extends PartitionStrategy
  case object Range extends PartitionStrategy
  case object List extends PartitionStrategy
  case object RoundRobin extends PartitionStrategy
}

final case class PartitionInfo(
  strategy: PartitionStrategy,
  columns: Seq[String],
  partitionCount: Int
)
