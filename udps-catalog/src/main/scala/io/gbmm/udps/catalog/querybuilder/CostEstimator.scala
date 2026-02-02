package io.gbmm.udps.catalog.querybuilder

final case class TableStats(
    rowCount: Long,
    avgRowSizeBytes: Long
)

final case class CostEstimate(
    estimatedRows: Long,
    estimatedBytes: Long,
    complexity: String
)

final class CostEstimator {

  private val FilterSelectivity = 0.3
  private val JoinMultiplier = 2.0
  private val CrossJoinWarningThreshold = 1000000L

  private val ComplexityLowThreshold = 10000L
  private val ComplexityMediumThreshold = 1000000L

  def estimate(
      spec: QuerySpec,
      tableStats: Map[String, TableStats]
  ): CostEstimate = {
    val baseRows = estimateBaseRows(spec.tables, tableStats)
    val afterFilters = applyFilterSelectivity(baseRows, spec.filters.size)
    val afterJoins = applyJoinMultiplier(afterFilters, spec.joins, tableStats)
    val finalRows = applyLimit(afterJoins, spec.limit)

    val avgRowSize = estimateAvgRowSize(spec.tables, tableStats)
    val estimatedBytes = finalRows * avgRowSize

    val complexity = classifyComplexity(finalRows, spec)

    CostEstimate(
      estimatedRows = finalRows,
      estimatedBytes = estimatedBytes,
      complexity = complexity
    )
  }

  private def estimateBaseRows(
      tables: Seq[TableRef],
      stats: Map[String, TableStats]
  ): Long = {
    if (tables.isEmpty) return 0L
    tables.map(t => stats.get(t.name).map(_.rowCount).getOrElse(0L)).max
  }

  private def applyFilterSelectivity(rows: Long, filterCount: Int): Long = {
    val selectivity = math.pow(FilterSelectivity, filterCount.toDouble)
    math.max(1L, (rows * selectivity).toLong)
  }

  private def applyJoinMultiplier(
      rows: Long,
      joins: Seq[JoinSpec],
      stats: Map[String, TableStats]
  ): Long = {
    if (joins.isEmpty) return rows
    joins.foldLeft(rows) { (currentRows, join) =>
      val rightRows = stats.get(join.rightTable.name).map(_.rowCount).getOrElse(0L)
      join.joinType match {
        case JoinType.Cross =>
          val product = currentRows * math.max(1L, rightRows)
          if (product > CrossJoinWarningThreshold) product
          else product
        case _ =>
          (currentRows * JoinMultiplier).toLong
      }
    }
  }

  private def applyLimit(rows: Long, limit: Option[Int]): Long =
    limit.map(l => math.min(rows, l.toLong)).getOrElse(rows)

  private def estimateAvgRowSize(
      tables: Seq[TableRef],
      stats: Map[String, TableStats]
  ): Long = {
    val sizes = tables.flatMap(t => stats.get(t.name).map(_.avgRowSizeBytes))
    if (sizes.isEmpty) DefaultRowSizeBytes
    else sizes.sum / sizes.size
  }

  private def classifyComplexity(rows: Long, spec: QuerySpec): String = {
    val hasCrossJoin = spec.joins.exists(_.joinType == JoinType.Cross)
    val hasMultipleJoins = spec.joins.size > 1
    val hasAggregation = spec.columns.exists(_.aggregation.nonEmpty)

    if (hasCrossJoin || rows > ComplexityMediumThreshold) "HIGH"
    else if (hasMultipleJoins || hasAggregation || rows > ComplexityLowThreshold) "MEDIUM"
    else "LOW"
  }

  private val DefaultRowSizeBytes = 256L
}
