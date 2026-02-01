package io.gbmm.udps.storage.mvcc

import cats.effect.IO
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import fs2.Stream

import java.nio.file.{Files, Paths}
import java.time.Duration
import scala.concurrent.duration.FiniteDuration

/** Result of a garbage collection run. */
final case class GCResult(
  snapshotsRemoved: Int,
  filesDeleted: Int,
  bytesReclaimed: Long,
  duration: Duration
)

/**
 * Garbage collector for expired MVCC snapshots.
 *
 * Identifies snapshots older than the retention period, determines which of their
 * data files are no longer referenced by any active snapshot, deletes those orphaned
 * files from disk, and marks the expired snapshots as inactive.
 *
 * @param snapshotManager the snapshot manager whose state this GC operates on
 * @param retentionDays   number of days a snapshot is retained before becoming eligible
 */
final class SnapshotGC(
  snapshotManager: SnapshotManager,
  retentionDays: Int
) extends LazyLogging {

  /**
   * Run garbage collection for a single table.
   *
   * Steps:
   *  1. Identify snapshots older than the retention cutoff.
   *  2. Build a set of files still referenced by active (non-expired) snapshots.
   *  3. Compute orphaned files (referenced only by expired snapshots).
   *  4. Delete orphaned files from disk and tally reclaimed bytes.
   *  5. Mark expired snapshots as inactive.
   */
  def collectGarbage(tableName: String): IO[GCResult] =
    for {
      start   <- IO.monotonic
      now     <- IO.realTimeInstant
      cutoff   = now.minus(Duration.ofDays(retentionDays.toLong))
      all     <- snapshotManager.allSnapshots(tableName)
      active   = all.filter(_.isActive)
      expired  = active.filter(_.timestamp.isBefore(cutoff))
      retained = active.filterNot(_.timestamp.isBefore(cutoff))
      retainedFiles = retained.flatMap(_.dataFiles).toSet
      expiredFiles  = expired.flatMap(_.dataFiles).toSet
      orphaned      = expiredFiles.diff(retainedFiles)
      deleteResults <- orphaned.toList.traverse(deleteFile)
      bytesReclaimed = deleteResults.sum
      removed <- snapshotManager.markInactive(tableName, expired.map(_.snapshotId).toSet)
      end     <- IO.monotonic
      elapsed  = Duration.ofNanos((end - start).toNanos)
      result   = GCResult(
        snapshotsRemoved = removed,
        filesDeleted     = deleteResults.size,
        bytesReclaimed   = bytesReclaimed,
        duration         = elapsed
      )
      _ <- IO.delay(logger.info(
        "GC for '{}': removed {} snapshots, deleted {} files, reclaimed {} bytes in {}ms",
        tableName,
        result.snapshotsRemoved.toString,
        result.filesDeleted.toString,
        result.bytesReclaimed.toString,
        elapsed.toMillis.toString
      ))
    } yield result

  /** Run garbage collection across all tables. */
  def collectAllGarbage(): IO[Seq[GCResult]] =
    snapshotManager.listTables.flatMap { tables =>
      tables.traverse(collectGarbage)
    }

  /**
   * Identify files on disk that are not referenced by any active snapshot.
   *
   * Compares all files across every snapshot (active and inactive) against the set
   * of files referenced by active snapshots. Files present only in inactive snapshots
   * are considered orphaned.
   */
  def identifyOrphanedFiles(tableName: String): IO[Seq[String]] =
    for {
      all <- snapshotManager.allSnapshots(tableName)
      activeFiles   = all.filter(_.isActive).flatMap(_.dataFiles).toSet
      inactiveFiles = all.filterNot(_.isActive).flatMap(_.dataFiles).toSet
    } yield inactiveFiles.diff(activeFiles).toSeq.sorted

  /**
   * Simulate garbage collection without performing any deletions.
   *
   * Returns a GCResult showing what would be removed, with zero bytes reclaimed
   * and zero actual deletions.
   */
  def dryRun(tableName: String): IO[GCResult] =
    for {
      start   <- IO.monotonic
      now     <- IO.realTimeInstant
      cutoff   = now.minus(Duration.ofDays(retentionDays.toLong))
      all     <- snapshotManager.allSnapshots(tableName)
      active   = all.filter(_.isActive)
      expired  = active.filter(_.timestamp.isBefore(cutoff))
      retained = active.filterNot(_.timestamp.isBefore(cutoff))
      retainedFiles = retained.flatMap(_.dataFiles).toSet
      expiredFiles  = expired.flatMap(_.dataFiles).toSet
      orphaned      = expiredFiles.diff(retainedFiles)
      estimatedBytes <- orphaned.toList.traverse(fileSizeOrZero)
      end     <- IO.monotonic
      elapsed  = Duration.ofNanos((end - start).toNanos)
      result   = GCResult(
        snapshotsRemoved = expired.size,
        filesDeleted     = orphaned.size,
        bytesReclaimed   = estimatedBytes.sum,
        duration         = elapsed
      )
      _ <- IO.delay(logger.info(
        "Dry-run GC for '{}': would remove {} snapshots, {} files, ~{} bytes",
        tableName,
        result.snapshotsRemoved.toString,
        result.filesDeleted.toString,
        result.bytesReclaimed.toString
      ))
    } yield result

  /**
   * Schedule periodic garbage collection as an fs2 stream.
   *
   * Emits one [[GCResult]] per table per interval tick. The stream runs indefinitely
   * and can be interrupted by the caller.
   */
  def scheduleGC(interval: FiniteDuration): Stream[IO, GCResult] =
    Stream
      .awakeEvery[IO](interval)
      .evalMap(_ => collectAllGarbage())
      .flatMap(results => Stream.emits(results))

  /**
   * Delete a file from disk, returning the number of bytes reclaimed.
   *
   * Resolves file paths relative to the snapshot manager's storage path.
   * Returns 0 if the file does not exist (idempotent).
   */
  private def deleteFile(filePath: String): IO[Long] =
    IO.blocking {
      val path = Paths.get(snapshotManager.storagePath).resolve(filePath)
      if (Files.exists(path)) {
        val size = Files.size(path)
        Files.delete(path)
        logger.debug("Deleted orphaned file: {} ({} bytes)", path.toString, size.toString)
        size
      } else {
        logger.debug("File already absent, skipping: {}", path.toString)
        0L
      }
    }.handleErrorWith { err =>
      IO.delay(logger.warn("Failed to delete file {}: {}", filePath, err.getMessage)).as(0L)
    }

  /**
   * Read the size of a file without deleting it. Returns 0 if the file is absent.
   */
  private def fileSizeOrZero(filePath: String): IO[Long] =
    IO.blocking {
      val path = Paths.get(snapshotManager.storagePath).resolve(filePath)
      if (Files.exists(path)) Files.size(path) else 0L
    }.handleErrorWith(_ => IO.pure(0L))
}

object SnapshotGC {

  /** Create a SnapshotGC using the retention period from the snapshot manager. */
  def apply(snapshotManager: SnapshotManager): SnapshotGC =
    new SnapshotGC(snapshotManager, snapshotManager.retentionDays)

  /** Create a SnapshotGC with a custom retention period. */
  def apply(snapshotManager: SnapshotManager, retentionDays: Int): SnapshotGC =
    new SnapshotGC(snapshotManager, retentionDays)
}
