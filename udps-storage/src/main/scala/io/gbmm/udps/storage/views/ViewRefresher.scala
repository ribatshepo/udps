package io.gbmm.udps.storage.views

import cats.effect.{Clock, IO, Ref}
import com.typesafe.scalalogging.LazyLogging
import fs2.Stream
import io.gbmm.udps.storage.parquet.{CompressionConfig, ParquetWriter}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.hadoop.fs.Path

import java.time.Instant
import java.util.UUID

import scala.concurrent.duration._

/**
 * Executes the actual data refresh for materialized views.
 *
 * The caller provides an `executeQuery` function that translates a SQL
 * string into an Arrow [[VectorSchemaRoot]]. This abstraction decouples
 * the storage layer from the query module.
 *
 * @param executeQuery executes a SQL query and returns the result as an
 *                     Arrow columnar batch. The caller is responsible for
 *                     closing the returned [[VectorSchemaRoot]].
 */
final class ViewRefresher private (
    executeQuery: String => IO[VectorSchemaRoot],
    watermarks: Ref[IO, Map[UUID, Instant]]
) extends LazyLogging {

  // ---- Named constants ---------------------------------------------------

  private val DefaultScheduleInterval: FiniteDuration = 60.seconds
  private val ContinuousPollingInterval: FiniteDuration = 5.seconds
  private val MinCronDigitCount: Int = 1

  // ---- Public API --------------------------------------------------------

  /**
   * Full refresh: re-execute the view SQL and overwrite its Parquet data.
   */
  def refreshOnDemand(view: ViewDefinition): IO[ViewRefreshResult] =
    timedRefresh(view, isIncremental = false) {
      executeAndWriteParquet(view.sqlQuery, view.dataPath, view)
    }

  /**
   * Incremental refresh: only process data that arrived after the given
   * watermark. The view's SQL is augmented with a temporal predicate.
   */
  def refreshIncremental(view: ViewDefinition, changesSince: Instant): IO[ViewRefreshResult] =
    timedRefresh(view, isIncremental = true) {
      val incrementalQuery = augmentQueryWithWatermark(view.sqlQuery, changesSince)
      for {
        rows <- executeAndWriteParquet(incrementalQuery, view.dataPath, view)
        _    <- watermarks.update(_.updated(view.id, Instant.now()))
      } yield rows
    }

  /**
   * Scheduled refresh: returns an fs2 [[Stream]] that emits a
   * [[ViewRefreshResult]] on each tick. The interval is derived from the
   * view's cron expression (simplified to a fixed period).
   */
  def refreshScheduled(view: ViewDefinition): IO[Stream[IO, ViewRefreshResult]] =
    IO.pure {
      val interval = view.refreshMode match {
        case RefreshMode.Scheduled(cron) => parseCronToInterval(cron)
        case _                           => DefaultScheduleInterval
      }

      Stream
        .awakeEvery[IO](interval)
        .evalMap(_ => refreshOnDemand(view))
    }

  /**
   * Continuous refresh: returns an fs2 [[Stream]] that polls for changes
   * and emits a [[ViewRefreshResult]] whenever new data is detected.
   */
  def refreshContinuous(view: ViewDefinition): IO[Stream[IO, ViewRefreshResult]] =
    IO.pure {
      Stream
        .awakeEvery[IO](ContinuousPollingInterval)
        .evalMap { _ =>
          for {
            wm     <- watermarks.get.map(_.getOrElse(view.id, Instant.EPOCH))
            result <- refreshIncremental(view, wm)
          } yield result
        }
    }

  // ---- Internal ----------------------------------------------------------

  /**
   * Execute the SQL query and write results to Parquet at the view's data
   * path. Returns the number of rows written.
   */
  private def executeAndWriteParquet(
      sql: String,
      dataPath: String,
      view: ViewDefinition
  ): IO[Long] =
    for {
      root <- executeQuery(sql)
      metadata = ParquetWriter.FileMetadata.now(
        tableName = view.name,
        namespace = view.namespace,
        partitionInfo = if (view.partitionColumns.nonEmpty) Some(view.partitionColumns.mkString(",")) else None
      )
      config   = CompressionConfig.Default
      parquetPath = new Path(dataPath)
      rowCount <- ParquetWriter.writeBatch(parquetPath, root, config, metadata)
      _        <- IO.blocking(root.close())
    } yield rowCount

  /**
   * Wrap a refresh operation with timing and result construction.
   */
  private def timedRefresh(view: ViewDefinition, isIncremental: Boolean)(
      body: IO[Long]
  ): IO[ViewRefreshResult] =
    for {
      startNanos <- Clock[IO].monotonic.map(_.toNanos)
      rows       <- body
      endNanos   <- Clock[IO].monotonic.map(_.toNanos)
      now        <- Clock[IO].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
      elapsedMs   = (endNanos - startNanos) / 1000000L
    } yield ViewRefreshResult(
      viewId = view.id,
      rowsWritten = rows,
      refreshedAt = now,
      durationMs = elapsedMs,
      isIncremental = isIncremental
    )

  /**
   * Augment a SQL query with a watermark filter. Wraps the original
   * query as a subquery and applies a temporal predicate.
   */
  private def augmentQueryWithWatermark(sql: String, since: Instant): String =
    s"SELECT * FROM ($sql) AS __view_src WHERE __view_src._updated_at > '${since.toString}'"

  /**
   * Parse a simplified cron expression into a [[FiniteDuration]].
   *
   * Supports common shorthand patterns:
   *   - Expressions containing `/N` in the minutes field are interpreted
   *     as "every N minutes".
   *   - Expressions with an hourly digit are interpreted as "every N hours".
   *   - All other expressions fall back to the default schedule interval.
   */
  private def parseCronToInterval(cron: String): FiniteDuration = {
    val parts = cron.trim.split("\\s+")
    if (parts.length >= 2) {
      val minuteField = parts(0)
      val hourField   = parts(1)

      if (minuteField.contains("/")) {
        val everyMinutes = minuteField.split("/")(1)
        parseFiniteDuration(everyMinutes, _.minutes, DefaultScheduleInterval)
      } else if (hourField.contains("/")) {
        val everyHours = hourField.split("/")(1)
        parseFiniteDuration(everyHours, _.hours, DefaultScheduleInterval)
      } else if (minuteField.forall(_.isDigit) && minuteField.length >= MinCronDigitCount &&
                 hourField.contains("*")) {
        DefaultScheduleInterval
      } else {
        DefaultScheduleInterval
      }
    } else {
      DefaultScheduleInterval
    }
  }

  /**
   * Safely parse a numeric string into a [[FiniteDuration]] via the
   * supplied unit constructor, falling back to `default` on failure.
   */
  private def parseFiniteDuration(
      raw: String,
      unit: Long => FiniteDuration,
      default: FiniteDuration
  ): FiniteDuration =
    try {
      val value = raw.toLong
      if (value > 0) unit(value) else default
    } catch {
      case _: NumberFormatException =>
        logger.warn("Failed to parse cron interval value: {}", raw)
        default
    }
}

object ViewRefresher {

  /**
   * Build a [[ViewRefresher]].
   *
   * @param executeQuery function that runs a SQL query and returns an
   *                     Arrow [[VectorSchemaRoot]]
   */
  def create(executeQuery: String => IO[VectorSchemaRoot]): IO[ViewRefresher] =
    Ref.of[IO, Map[UUID, Instant]](Map.empty).map { wmRef =>
      new ViewRefresher(executeQuery, wmRef)
    }
}
