package io.gbmm.udps.query.adaptive

import cats.effect.{IO, Ref}
import com.typesafe.scalalogging.LazyLogging

import java.time.Instant

/** Immutable statistics collected for a single physical operator during execution. */
final case class OperatorStats(
  operatorId: String,
  operatorType: String,
  rowsProcessed: Long,
  rowsOutput: Long,
  bytesProcessed: Long,
  executionTimeMs: Long,
  memoryUsedBytes: Long
) {

  /** Selectivity is the ratio of output rows to processed rows.
    * Returns 1.0 when no rows have been processed to avoid division by zero.
    */
  def selectivity: Double =
    if (rowsProcessed > 0L) rowsOutput.toDouble / rowsProcessed.toDouble
    else 1.0
}

/** Immutable statistics collected for a single execution stage. */
final case class StageStats(
  stageId: String,
  partitionCount: Int,
  totalRows: Long,
  totalBytes: Long,
  executionTimeMs: Long,
  peakMemoryBytes: Long
)

/** Point-in-time immutable snapshot of all collected statistics. */
final case class StatisticsSnapshot(
  operatorStats: Map[String, OperatorStats],
  stageStats: Map[String, StageStats],
  snapshotTimestamp: Instant
)

/** Thread-safe, mutable runtime statistics collector backed by Cats Effect Ref.
  *
  * All mutation is performed through atomic Ref updates, making this safe
  * for concurrent access from multiple fibers.
  */
final class RuntimeStatistics private (
  operatorStatsRef: Ref[IO, Map[String, OperatorStats]],
  stageStatsRef: Ref[IO, Map[String, StageStats]]
) extends LazyLogging {

  /** Record (or overwrite) statistics for an operator identified by its ID. */
  def recordOperatorStats(operatorId: String, stats: OperatorStats): IO[Unit] =
    operatorStatsRef.update(_.updated(operatorId, stats)) *>
      IO(logger.debug("Recorded operator stats for {}: rows_in={}, rows_out={}, selectivity={}",
        operatorId, stats.rowsProcessed.toString, stats.rowsOutput.toString, f"${stats.selectivity}%.4f"))

  /** Retrieve statistics for a single operator, if recorded. */
  def getOperatorStats(operatorId: String): IO[Option[OperatorStats]] =
    operatorStatsRef.get.map(_.get(operatorId))

  /** Retrieve all recorded operator statistics. */
  def getAllStats: IO[Map[String, OperatorStats]] =
    operatorStatsRef.get

  /** Record (or overwrite) statistics for an execution stage. */
  def recordStageStats(stageId: String, stats: StageStats): IO[Unit] =
    stageStatsRef.update(_.updated(stageId, stats)) *>
      IO(logger.debug("Recorded stage stats for {}: rows={}, partitions={}, time_ms={}",
        stageId, stats.totalRows.toString, stats.partitionCount.toString, stats.executionTimeMs.toString))

  /** Retrieve statistics for a single stage, if recorded. */
  def getStageStats(stageId: String): IO[Option[StageStats]] =
    stageStatsRef.get.map(_.get(stageId))

  /** Retrieve all recorded stage statistics. */
  def getAllStageStats: IO[Map[String, StageStats]] =
    stageStatsRef.get

  /** Create an immutable snapshot of all statistics at this instant. */
  def snapshot: IO[StatisticsSnapshot] =
    for {
      ops    <- operatorStatsRef.get
      stages <- stageStatsRef.get
    } yield StatisticsSnapshot(ops, stages, Instant.now())

  /** Clear all collected statistics. */
  def reset: IO[Unit] =
    operatorStatsRef.set(Map.empty) *> stageStatsRef.set(Map.empty)
}

object RuntimeStatistics {

  /** Create a new empty RuntimeStatistics instance. */
  def create: IO[RuntimeStatistics] =
    for {
      opRef    <- Ref.of[IO, Map[String, OperatorStats]](Map.empty)
      stageRef <- Ref.of[IO, Map[String, StageStats]](Map.empty)
    } yield new RuntimeStatistics(opRef, stageRef)
}
