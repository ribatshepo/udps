package io.gbmm.udps.query.adaptive

import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.query.physical._

/** Suggestion to create an index based on observed runtime statistics. */
final case class IndexSuggestion(
  tableName: String,
  columns: Seq[String],
  reason: String
)

/** Sealed hierarchy of decisions the re-optimizer can make between stages. */
sealed trait ReOptimizationDecision extends Product with Serializable

object ReOptimizationDecision {
  case object NoChange extends ReOptimizationDecision

  final case class SwitchJoinStrategy(
    from: String,
    to: String
  ) extends ReOptimizationDecision

  final case class AdjustParallelism(
    from: Int,
    to: Int
  ) extends ReOptimizationDecision

  final case class SkipPartitions(
    partitionIds: Set[Int]
  ) extends ReOptimizationDecision
}

/** Runtime re-optimizer that uses actual execution statistics to adjust
  * remaining stages of a query plan.
  *
  * All methods are pure (no side effects). The caller is responsible for
  * applying decisions to the execution DAG.
  */
final class ReOptimizer extends LazyLogging {

  import ReOptimizer._

  /** Determine whether re-optimization is warranted based on the ratio
    * between estimated and actual row counts.
    *
    * @param estimated operator stats from the original cost model
    * @param actual    operator stats observed at runtime
    * @param threshold factor by which actual must differ from estimated (default 2.0)
    * @return true if |actual / estimated| exceeds the threshold in either direction
    */
  def shouldReOptimize(
    estimated: OperatorStats,
    actual: OperatorStats,
    threshold: Double = DefaultReOptimizationThreshold
  ): Boolean = {
    val estimatedRows = math.max(estimated.rowsOutput, 1L)
    val actualRows    = math.max(actual.rowsOutput, 1L)
    val ratio         = actualRows.toDouble / estimatedRows.toDouble

    val shouldReOpt = ratio > threshold || ratio < (1.0 / threshold)
    if (shouldReOpt) {
      logger.info(
        "Re-optimization triggered: estimated={} actual={} ratio={} threshold={}",
        estimatedRows.toString, actualRows.toString, f"$ratio%.2f", f"$threshold%.2f"
      )
    }
    shouldReOpt
  }

  /** Re-optimize a hash join based on actual statistics from its children.
    *
    * Strategy:
    *  - Both sides < 100 rows: switch to NestedLoopJoinOp
    *  - One side < 10,000 rows (broadcast threshold): keep HashJoin but ensure
    *    the smaller side is the build side (left)
    *  - Otherwise: swap build/probe sides if the right side is actually smaller
    */
  def reOptimizeJoin(
    join: HashJoinOp,
    leftStats: OperatorStats,
    rightStats: OperatorStats
  ): PhysicalOperator = {
    val leftRows  = leftStats.rowsOutput
    val rightRows = rightStats.rowsOutput

    if (leftRows < NestedLoopRowThreshold && rightRows < NestedLoopRowThreshold) {
      logger.info(
        "Switching join to NestedLoopJoin: left={} right={} (both below {} threshold)",
        leftRows.toString, rightRows.toString, NestedLoopRowThreshold.toString
      )
      NestedLoopJoinOp(
        left = join.left,
        right = join.right,
        joinType = join.joinType,
        condition = None,
        estimatedRows = join.estimatedRows
      )
    } else if (rightRows < BroadcastRowThreshold && leftRows >= BroadcastRowThreshold) {
      // Right side is small enough to broadcast -- it should be the build side.
      // In HashJoinOp, left is the build side, so swap if right is smaller.
      logger.info(
        "Swapping join sides for broadcast: moving right ({} rows) to build side",
        rightRows.toString
      )
      HashJoinOp(
        left = join.right,
        right = join.left,
        joinType = swapJoinType(join.joinType),
        leftKeys = join.rightKeys,
        rightKeys = join.leftKeys,
        estimatedRows = join.estimatedRows
      )
    } else if (leftRows < BroadcastRowThreshold && rightRows >= BroadcastRowThreshold) {
      // Left (build) side is already the smaller one -- no change needed
      logger.info("Join build side already optimal: left={} rows (build), right={} rows (probe)",
        leftRows.toString, rightRows.toString)
      join
    } else if (rightRows < leftRows) {
      // Neither side is below broadcast threshold, but right is smaller --
      // swap so the smaller side is the build side for memory efficiency.
      logger.info(
        "Swapping join sides: right ({} rows) smaller than left ({} rows)",
        rightRows.toString, leftRows.toString
      )
      HashJoinOp(
        left = join.right,
        right = join.left,
        joinType = swapJoinType(join.joinType),
        leftKeys = join.rightKeys,
        rightKeys = join.leftKeys,
        estimatedRows = join.estimatedRows
      )
    } else {
      join
    }
  }

  /** Adjust the partition count for a stage based on the actual number of rows
    * observed in upstream stages.
    *
    * @param currentPartitions current number of partitions
    * @param actualRows        actual row count observed
    * @param targetRowsPerPartition desired rows per partition (default 100,000)
    * @return adjusted partition count, clamped to [1, MaxPartitions]
    */
  def reOptimizeParallelism(
    currentPartitions: Int,
    actualRows: Long,
    targetRowsPerPartition: Long = DefaultTargetRowsPerPartition
  ): Int = {
    val idealPartitions = math.max(
      MinPartitions,
      math.ceil(actualRows.toDouble / targetRowsPerPartition.toDouble).toInt
    )
    val clamped = math.min(idealPartitions, MaxPartitions)

    if (clamped != currentPartitions) {
      logger.info(
        "Adjusting parallelism: current={} ideal={} actualRows={} targetPerPartition={}",
        currentPartitions.toString, clamped.toString, actualRows.toString, targetRowsPerPartition.toString
      )
    }
    clamped
  }

  /** Analyze operator statistics to suggest indexes for table scans that
    * apply low-selectivity filters (i.e., filters that discard many rows).
    *
    * A suggestion is produced when a filter operator has selectivity below
    * the configured threshold, indicating that an index could significantly
    * reduce I/O.
    */
  def suggestIndexes(stats: Map[String, OperatorStats]): Seq[IndexSuggestion] =
    stats.values.toSeq.flatMap { opStats =>
      if (opStats.operatorType == "FilterOp" && opStats.selectivity < IndexSuggestionSelectivityThreshold) {
        extractTableAndColumns(opStats).map { case (table, cols) =>
          IndexSuggestion(
            tableName = table,
            columns = cols,
            reason = f"Filter selectivity ${opStats.selectivity}%.4f below ${IndexSuggestionSelectivityThreshold}%.2f " +
              f"on ${opStats.rowsProcessed}%d rows -- index could reduce scan by " +
              f"${((1.0 - opStats.selectivity) * 100.0)}%.1f%%"
          )
        }
      } else {
        Seq.empty
      }
    }

  /** Build a [[ReOptimizationDecision]] by comparing estimated vs actual stats
    * for an operator, returning the appropriate decision.
    */
  def decideForOperator(
    operator: PhysicalOperator,
    estimated: OperatorStats,
    actual: OperatorStats,
    currentPartitions: Int
  ): ReOptimizationDecision = {
    if (!shouldReOptimize(estimated, actual)) {
      ReOptimizationDecision.NoChange
    } else {
      operator match {
        case _: HashJoinOp =>
          ReOptimizationDecision.SwitchJoinStrategy(
            from = "HashJoin",
            to = determineJoinTarget(actual.rowsOutput)
          )
        case _ =>
          val newPartitions = reOptimizeParallelism(currentPartitions, actual.rowsOutput)
          if (newPartitions != currentPartitions) {
            ReOptimizationDecision.AdjustParallelism(from = currentPartitions, to = newPartitions)
          } else {
            ReOptimizationDecision.NoChange
          }
      }
    }
  }

  private def determineJoinTarget(actualRows: Long): String =
    if (actualRows < NestedLoopRowThreshold) "NestedLoopJoin"
    else if (actualRows < BroadcastRowThreshold) "BroadcastHashJoin"
    else "HashJoin"

  /** Swap left/right outer join types when swapping join sides.
    * Inner, Full, and Cross joins are symmetric.
    */
  private def swapJoinType(joinType: JoinType): JoinType = joinType match {
    case JoinType.LeftOuter  => JoinType.RightOuter
    case JoinType.RightOuter => JoinType.LeftOuter
    case other               => other
  }

  /** Extract table name and filter columns from an operator's ID encoding.
    *
    * The operator ID convention is "FilterOp_<table>_<col1>,<col2>".
    * If the ID does not follow this convention, no suggestion is produced.
    */
  private def extractTableAndColumns(opStats: OperatorStats): Seq[(String, Seq[String])] = {
    val parts = opStats.operatorId.split("_", 3) // limit to 3 segments
    if (parts.length >= 3) {
      val table   = parts(1)
      val columns = parts(2).split(",").toSeq.filter(_.nonEmpty)
      if (columns.nonEmpty) Seq((table, columns)) else Seq.empty
    } else {
      Seq.empty
    }
  }
}

object ReOptimizer {
  private[adaptive] val DefaultReOptimizationThreshold: Double = 2.0
  private[adaptive] val BroadcastRowThreshold: Long            = 10000L
  private[adaptive] val NestedLoopRowThreshold: Long           = 100L
  private[adaptive] val DefaultTargetRowsPerPartition: Long    = 100000L
  private[adaptive] val MinPartitions: Int                     = 1
  private[adaptive] val MaxPartitions: Int                     = 256
  private[adaptive] val IndexSuggestionSelectivityThreshold: Double = 0.1
}
