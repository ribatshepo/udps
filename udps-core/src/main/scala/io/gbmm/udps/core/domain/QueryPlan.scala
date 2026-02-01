package io.gbmm.udps.core.domain

import java.time.Instant
import java.util.UUID

final case class QueryPlan(
  id: UUID,
  sql: Option[String],
  logicalPlan: String,
  physicalPlan: Option[String],
  estimatedCost: Option[Double],
  estimatedRows: Option[Long],
  createdAt: Instant
)
