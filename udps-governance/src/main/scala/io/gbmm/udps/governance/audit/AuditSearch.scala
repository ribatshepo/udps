package io.gbmm.udps.governance.audit

import java.time.Instant
import java.time.format.DateTimeFormatter

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

final case class AuditSearchCriteria(
    userId: Option[String],
    action: Option[AuditAction],
    resource: Option[String],
    startTime: Option[Instant],
    endTime: Option[Instant]
)

class AuditSearch(xa: Transactor[IO]) extends LazyLogging {

  import AuditEntry._

  def search(criteria: AuditSearchCriteria, limit: Int = 100): IO[Seq[AuditEntry]] = {
    val baseFragment = fr"""SELECT id, user_id, action, resource, timestamp, ip_address, user_agent, status, details, hmac_signature
                            FROM audit_log"""
    val conditions = buildConditions(criteria)
    val whereClause = conditions match {
      case Nil => Fragment.empty
      case _   => fr"WHERE" ++ conditions.reduceLeft(_ ++ fr"AND" ++ _)
    }
    val orderAndLimit = fr"ORDER BY timestamp DESC LIMIT $limit"
    val query = baseFragment ++ whereClause ++ orderAndLimit

    query
      .query[AuditEntry]
      .to[List]
      .transact(xa)
      .map(_.toSeq)
  }

  def generateComplianceReport(startTime: Instant, endTime: Instant): IO[String] =
    sql"""SELECT id, user_id, action, resource, timestamp, ip_address, user_agent, status, details, hmac_signature
          FROM audit_log WHERE timestamp >= $startTime AND timestamp <= $endTime ORDER BY timestamp ASC"""
      .query[AuditEntry]
      .to[List]
      .transact(xa)
      .map(entries => formatCsvReport(entries.toSeq))

  def countByAction(startTime: Instant, endTime: Instant): IO[Map[String, Long]] =
    sql"""SELECT action, COUNT(*) FROM audit_log WHERE timestamp >= $startTime AND timestamp <= $endTime GROUP BY action"""
      .query[(String, Long)]
      .to[List]
      .transact(xa)
      .map(_.toMap)

  private def buildConditions(criteria: AuditSearchCriteria): List[Fragment] = {
    val userFilter = criteria.userId.map(u => fr"user_id = $u").toList
    val actionFilter = criteria.action.map(a => fr"action = $a").toList
    val resourceFilter = criteria.resource.map(r => fr"resource = $r").toList
    val startFilter = criteria.startTime.map(t => fr"timestamp >= $t").toList
    val endFilter = criteria.endTime.map(t => fr"timestamp <= $t").toList
    userFilter ::: actionFilter ::: resourceFilter ::: startFilter ::: endFilter
  }

  private val csvFormatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

  private def formatCsvReport(entries: Seq[AuditEntry]): String = {
    val header = "id,user_id,action,resource,timestamp,ip_address,user_agent,status,details,hmac_signature"
    val rows = entries.map { e =>
      Seq(
        e.id.toString,
        escapeCsv(e.userId),
        AuditAction.asString(e.action),
        escapeCsv(e.resource),
        csvFormatter.format(e.timestamp),
        e.ipAddress.getOrElse(""),
        escapeCsv(e.userAgent.getOrElse("")),
        escapeCsv(e.status),
        escapeCsv(e.details.noSpaces),
        e.hmacSignature
      ).mkString(",")
    }
    (header +: rows).mkString("\n")
  }

  private def escapeCsv(value: String): String =
    if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
      "\"" + value.replace("\"", "\"\"") + "\""
    } else {
      value
    }
}
