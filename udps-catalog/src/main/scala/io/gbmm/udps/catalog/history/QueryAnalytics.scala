package io.gbmm.udps.catalog.history

import java.util.UUID

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

final class QueryAnalytics(xa: Transactor[IO]) extends LazyLogging {

  private val DefaultQueryLimit = 10
  private val DefaultUserQueryLimit = 100

  def topSlowestQueries(limit: Int = DefaultQueryLimit): IO[Seq[QueryRecord]] =
    sql"""SELECT id, sql_text, user_id, start_time, end_time,
            duration_ms, rows_returned, bytes_scanned, status, error_message
          FROM catalog_query_history
          WHERE duration_ms IS NOT NULL AND status = 'COMPLETED'
          ORDER BY duration_ms DESC
          LIMIT $limit"""
      .query[QueryRecord]
      .to[Seq]
      .transact(xa)

  def mostFrequentQueries(limit: Int = DefaultQueryLimit): IO[Seq[(String, Long)]] =
    sql"""SELECT md5(sql_text) AS query_hash, COUNT(*) AS exec_count
          FROM catalog_query_history
          GROUP BY md5(sql_text)
          ORDER BY exec_count DESC
          LIMIT $limit"""
      .query[(String, Long)]
      .to[Seq]
      .transact(xa)

  def queriesByUser(userId: String, limit: Int = DefaultUserQueryLimit): IO[Seq[QueryRecord]] =
    sql"""SELECT id, sql_text, user_id, start_time, end_time,
            duration_ms, rows_returned, bytes_scanned, status, error_message
          FROM catalog_query_history
          WHERE user_id = $userId
          ORDER BY start_time DESC
          LIMIT $limit"""
      .query[QueryRecord]
      .to[Seq]
      .transact(xa)

  def queriesByTable(tableName: String, limit: Int = DefaultUserQueryLimit): IO[Seq[QueryRecord]] = {
    val pattern = s"%$tableName%"
    sql"""SELECT id, sql_text, user_id, start_time, end_time,
            duration_ms, rows_returned, bytes_scanned, status, error_message
          FROM catalog_query_history
          WHERE sql_text LIKE $pattern
          ORDER BY start_time DESC
          LIMIT $limit"""
      .query[QueryRecord]
      .to[Seq]
      .transact(xa)
  }

  def optimizationRecommendations(queryId: UUID): IO[Seq[String]] =
    sql"""SELECT id, sql_text, user_id, start_time, end_time,
            duration_ms, rows_returned, bytes_scanned, status, error_message
          FROM catalog_query_history
          WHERE id = $queryId"""
      .query[QueryRecord]
      .option
      .transact(xa)
      .map {
        case Some(record) => analyzeQuery(record)
        case None         => Seq(s"Query with id $queryId not found")
      }

  private def analyzeQuery(record: QueryRecord): Seq[String] = {
    val recommendations = Seq.newBuilder[String]
    val sqlUpper = record.sqlText.toUpperCase

    val whereColumns = extractWhereColumns(record.sqlText)
    if (whereColumns.nonEmpty) {
      recommendations += s"Consider adding indexes on columns used in WHERE clause: ${whereColumns.mkString(", ")}"
    }

    if (!sqlUpper.contains("LIMIT")) {
      recommendations += "Consider adding a LIMIT clause to restrict result set size"
    }

    if (sqlUpper.contains("SELECT *")) {
      recommendations += "Avoid SELECT *; specify only needed columns to reduce data transfer"
    }

    record.durationMs.foreach { duration =>
      val SlowQueryThresholdMs = 5000L
      if (duration > SlowQueryThresholdMs) {
        recommendations += s"Query took ${duration}ms which exceeds the slow query threshold; review execution plan"
      }
    }

    record.rowsReturned.foreach { rows =>
      val LargeResultThreshold = 100000L
      if (rows > LargeResultThreshold) {
        recommendations += s"Query returned $rows rows; consider pagination or filtering to reduce result size"
      }
    }

    record.bytesScanned.foreach { bytes =>
      val LargeScanThreshold = 1073741824L
      if (bytes > LargeScanThreshold) {
        recommendations += s"Query scanned ${bytes / 1048576L}MB; consider partitioning or indexing to reduce scan size"
      }
    }

    if (sqlUpper.contains("ORDER BY") && !sqlUpper.contains("LIMIT")) {
      recommendations += "ORDER BY without LIMIT may cause full result materialization; add LIMIT if possible"
    }

    val result = recommendations.result()
    if (result.isEmpty) Seq("No optimization recommendations; query appears efficient")
    else result
  }

  private def extractWhereColumns(sql: String): Seq[String] = {
    val wherePattern = "(?i)WHERE\\s+(.+?)(?:ORDER|GROUP|LIMIT|HAVING|$)".r
    val columnPattern = "(?i)([a-zA-Z_][a-zA-Z0-9_.]*?)\\s*(?:=|<>|!=|<|>|<=|>=|LIKE|IN|IS)".r

    wherePattern.findFirstMatchIn(sql) match {
      case Some(m) =>
        val whereClause = m.group(1)
        columnPattern.findAllMatchIn(whereClause).map(_.group(1).trim).toSeq.distinct
      case None => Seq.empty
    }
  }
}
