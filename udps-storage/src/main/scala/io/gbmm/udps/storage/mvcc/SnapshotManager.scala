package io.gbmm.udps.storage.mvcc

import cats.effect.{IO, Ref}

import com.typesafe.scalalogging.LazyLogging

import java.time.Instant
import java.util.UUID

/** An immutable point-in-time view of a table's data files. */
final case class Snapshot(
  snapshotId: Long,
  transactionId: UUID,
  tableName: String,
  timestamp: Instant,
  dataFiles: Seq[String],
  parentSnapshotId: Option[Long],
  isActive: Boolean
)

/** Aggregated metadata about a table's snapshot history. */
final case class SnapshotMetadata(
  tableName: String,
  currentSnapshotId: Long,
  snapshotCount: Int,
  oldestSnapshotTimestamp: Instant,
  newestSnapshotTimestamp: Instant
)

/**
 * Manages multi-version concurrency control snapshots for time-travel queries.
 *
 * Each snapshot represents an immutable view of a table at a specific point in time,
 * identified by a monotonically increasing snapshot ID and linked to the transaction
 * that produced it. Snapshot history is maintained per table and supports efficient
 * timestamp-based lookups via binary search.
 *
 * @param retentionDays number of days to retain snapshots before they become eligible for GC
 * @param storagePath   root directory under which snapshot data files reside
 */
final class SnapshotManager private (
  val retentionDays: Int,
  val storagePath: String,
  nextIdRef: Ref[IO, Long],
  stateRef: Ref[IO, Map[String, Seq[Snapshot]]]
) extends LazyLogging {

  /** Create a new snapshot for a table, linked to the committing transaction. */
  def createSnapshot(
    tableName: String,
    transactionId: UUID,
    dataFiles: Seq[String]
  ): IO[Snapshot] =
    for {
      snapshotId <- nextIdRef.getAndUpdate(_ + 1L)
      now        <- IO.realTimeInstant
      snapshot   <- stateRef.modify { state =>
        val history  = state.getOrElse(tableName, Seq.empty)
        val parentId = history.lastOption.map(_.snapshotId)
        val snap = Snapshot(
          snapshotId       = snapshotId,
          transactionId    = transactionId,
          tableName        = tableName,
          timestamp        = now,
          dataFiles        = dataFiles,
          parentSnapshotId = parentId,
          isActive         = true
        )
        val updated = state.updated(tableName, history :+ snap)
        (updated, snap)
      }
      _ <- IO.delay(logger.info(
        "Created snapshot {} for table '{}' with {} files (txn={})",
        snapshot.snapshotId.toString,
        tableName,
        dataFiles.size.toString,
        transactionId.toString
      ))
    } yield snapshot

  /** Retrieve a specific snapshot by ID. */
  def getSnapshot(tableName: String, snapshotId: Long): IO[Option[Snapshot]] =
    stateRef.get.map { state =>
      state.getOrElse(tableName, Seq.empty).find(_.snapshotId == snapshotId)
    }

  /** Retrieve the latest active snapshot for a table. */
  def getCurrentSnapshot(tableName: String): IO[Option[Snapshot]] =
    stateRef.get.map { state =>
      state.getOrElse(tableName, Seq.empty).filter(_.isActive).lastOption
    }

  /**
   * Time-travel: find the snapshot that was current at the given timestamp.
   *
   * Uses binary search on the chronologically-ordered snapshot history to locate the
   * latest snapshot whose timestamp is at or before the requested point in time.
   */
  def getSnapshotAtTimestamp(tableName: String, timestamp: Instant): IO[Option[Snapshot]] =
    stateRef.get.map { state =>
      val history = state.getOrElse(tableName, Seq.empty).filter(_.isActive)
      binarySearchByTimestamp(history, timestamp)
    }

  /** Return the data file paths associated with a snapshot. */
  def getSnapshotFiles(tableName: String, snapshotId: Long): IO[Seq[String]] =
    getSnapshot(tableName, snapshotId).map(_.map(_.dataFiles).getOrElse(Seq.empty))

  /** List all active snapshots for a table in chronological order. */
  def listSnapshots(tableName: String): IO[Seq[Snapshot]] =
    stateRef.get.map { state =>
      state.getOrElse(tableName, Seq.empty).filter(_.isActive)
    }

  /** Get summary metadata for a table's snapshot history. */
  def getMetadata(tableName: String): IO[Option[SnapshotMetadata]] =
    stateRef.get.map { state =>
      val active = state.getOrElse(tableName, Seq.empty).filter(_.isActive)
      if (active.isEmpty) None
      else Some(SnapshotMetadata(
        tableName               = tableName,
        currentSnapshotId       = active.last.snapshotId,
        snapshotCount           = active.size,
        oldestSnapshotTimestamp  = active.head.timestamp,
        newestSnapshotTimestamp  = active.last.timestamp
      ))
    }

  /**
   * Read the data files for a specific snapshot version.
   *
   * This is a non-blocking operation that does not interfere with concurrent writes;
   * it returns an immutable view of the files that were part of the snapshot at commit time.
   */
  def readAtSnapshot(tableName: String, snapshotId: Long): IO[Seq[String]] =
    getSnapshot(tableName, snapshotId).flatMap {
      case Some(snap) if snap.isActive => IO.pure(snap.dataFiles)
      case Some(_) =>
        IO.raiseError(new IllegalStateException(
          s"Snapshot $snapshotId for table '$tableName' is no longer active"
        ))
      case None =>
        IO.raiseError(new NoSuchElementException(
          s"Snapshot $snapshotId not found for table '$tableName'"
        ))
    }

  /** List all table names that have at least one snapshot. */
  def listTables: IO[Seq[String]] =
    stateRef.get.map(_.keys.toSeq.sorted)

  /** Provide internal access for GC to mark snapshots inactive. */
  private[mvcc] def markInactive(tableName: String, snapshotIds: Set[Long]): IO[Int] =
    stateRef.modify { state =>
      val history = state.getOrElse(tableName, Seq.empty)
      val updated = history.map { snap =>
        if (snapshotIds.contains(snap.snapshotId)) snap.copy(isActive = false)
        else snap
      }
      val count = snapshotIds.count(id => history.exists(s => s.snapshotId == id && s.isActive))
      (state.updated(tableName, updated), count)
    }

  /** Provide internal access for GC to read full history including inactive snapshots. */
  private[mvcc] def allSnapshots(tableName: String): IO[Seq[Snapshot]] =
    stateRef.get.map(_.getOrElse(tableName, Seq.empty))

  /**
   * Binary search for the latest snapshot at or before the target timestamp.
   *
   * Snapshots are stored in chronological order (ascending by timestamp). The search
   * finds the rightmost snapshot whose timestamp <= target.
   */
  private def binarySearchByTimestamp(
    snapshots: Seq[Snapshot],
    target: Instant
  ): Option[Snapshot] = {
    if (snapshots.isEmpty) return None
    val indexed = snapshots.toIndexedSeq
    var lo = 0
    var hi = indexed.size - 1
    var result: Option[Snapshot] = None

    while (lo <= hi) {
      val mid = lo + (hi - lo) / 2
      val ts  = indexed(mid).timestamp
      if (!ts.isAfter(target)) {
        result = Some(indexed(mid))
        lo = mid + 1
      } else {
        hi = mid - 1
      }
    }
    result
  }
}

object SnapshotManager {

  private val DefaultRetentionDays: Int = 30

  /** Create a new SnapshotManager with the given configuration. */
  def create(
    storagePath: String,
    retentionDays: Int = DefaultRetentionDays
  ): IO[SnapshotManager] =
    for {
      nextIdRef <- Ref.of[IO, Long](1L)
      stateRef  <- Ref.of[IO, Map[String, Seq[Snapshot]]](Map.empty)
    } yield new SnapshotManager(retentionDays, storagePath, nextIdRef, stateRef)
}
