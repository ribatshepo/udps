package io.gbmm.udps.core.domain

sealed abstract class StorageTier(val priority: Int) extends Product with Serializable

object StorageTier {
  case object Hot extends StorageTier(priority = 0)
  case object Warm extends StorageTier(priority = 1)
  case object Cold extends StorageTier(priority = 2)
  case object Archive extends StorageTier(priority = 3)
}
