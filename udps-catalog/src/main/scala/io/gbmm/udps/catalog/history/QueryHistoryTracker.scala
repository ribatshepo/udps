package io.gbmm.udps.catalog.history

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import doobie.implicits.javasql._
import doobie.postgres.implicits._

final case class QueryRecord(
    id: UUID,
    sqlText: String,
    userId: Option[String],
    startTime: Instant,
    endTime: Option[Instant],
    durationMs: Option[Long],
    rowsReturned: Option[Long],
    bytesScanned: Option[Long],
    status: String,
    errorMessage: Option[String]
)

final class QueryHistoryTracker(xa: Transactor[IO]) extends LazyLogging {

  private val DefaultHistoryLimit = 100
  private val DefaultRetentionDays = 90

  def recordQuery(record: QueryRecord): IO[Unit] =
    sql"""INSERT INTO catalog_query_history (
            id, sql_text, user_id, start_time, end_time,
            duration_ms, rows_returned, bytes_scanned, status, error_message
          ) VALUES (
            ${record.id}, ${record.sqlText}, ${record.userId},
            ${Timestamp.from(record.startTime)},
            ${record.endTime.map(Timestamp.from)},
            ${record.durationMs}, ${record.rowsReturned},
            ${record.bytesScanned}, ${record.status}, ${record.errorMessage}
          )""".update.run
      .transact(xa)
      .flatMap { inserted =>
        IO.delay(
          logger.info("Recorded query history: id={} status={} rows={}",
            record.id.toString, record.status, inserted.toString)
        )
      }

  def getHistory(userId: Option[String], limit: Int = DefaultHistoryLimit): IO[Seq[QueryRecord]] = {
    val baseQuery = fr"SELECT id, sql_text, user_id, start_time, end_time, duration_ms, rows_returned, bytes_scanned, status, error_message FROM catalog_query_history"
    val withFilter = userId match {
      case Some(uid) => baseQuery ++ fr"WHERE user_id = $uid"
      case None      => baseQuery
    }
    val withOrder = withFilter ++ fr"ORDER BY start_time DESC LIMIT $limit"
    withOrder
      .query[QueryRecord]
      .to[Seq]
      .transact(xa)
  }

  def applyRetentionPolicy(maxAgeDays: Int = DefaultRetentionDays): IO[Long] = {
    val cutoff = Instant.now().minusSeconds(maxAgeDays.toLong * 86400L)
    sql"""DELETE FROM catalog_query_history WHERE start_time < ${Timestamp.from(cutoff)}"""
      .update
      .run
      .transact(xa)
      .map(_.toLong)
      .flatTap { deleted =>
        IO.delay(
          logger.info("Applied retention policy: maxAgeDays={} deleted={}",
            maxAgeDays.toString, deleted.toString)
        )
      }
  }

  private object Timestamp {
    def from(instant: Instant): java.sql.Timestamp =
      java.sql.Timestamp.from(instant)
  }
}
