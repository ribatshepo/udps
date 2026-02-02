package io.gbmm.udps.governance.gdpr

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import io.circe.syntax._
import io.circe.generic.auto._

/** Enumeration of rights under GDPR. */
sealed trait GDPRRight extends Product with Serializable
object GDPRRight {
  case object RightToAccess extends GDPRRight
  case object RightToErasure extends GDPRRight
  case object RightToRectification extends GDPRRight
  case object RightToPortability extends GDPRRight
}

/** Tracks a GDPR request lifecycle. */
final case class GDPRRequest(
    id: UUID,
    right: GDPRRight,
    subjectId: String,
    requestedAt: Instant,
    processedAt: Option[Instant],
    status: String,
    details: Option[String]
)

/** Aggregated data export for a data subject. */
final case class GDPRDataExport(
    subjectId: String,
    tables: Map[String, Seq[Map[String, String]]],
    exportedAt: Instant
)

/** Result of an erasure operation. */
final case class ErasureResult(
    subjectId: String,
    tablesAffected: Int,
    rowsDeleted: Long
)

/** Supported export formats. */
sealed trait ExportFormat extends Product with Serializable
object ExportFormat {
  case object JsonFormat extends ExportFormat
  case object CsvFormat extends ExportFormat
}

/** Service handling GDPR compliance operations with full audit logging. */
final class GDPRComplianceService(xa: Transactor[IO]) extends LazyLogging {

  private val subjectDataTables: List[String] = List(
    "user_profiles",
    "user_activity",
    "consent_records",
    "user_preferences"
  )

  def accessRequest(subjectId: String): IO[GDPRDataExport] =
    for {
      _         <- validateSubjectId(subjectId)
      requestId <- IO.delay(UUID.randomUUID())
      _         <- logAudit(requestId, GDPRRight.RightToAccess, subjectId, "processing")
      tables    <- subjectDataTables.traverse(t => exportTable(t, subjectId).map(t -> _))
      now       <- IO.delay(Instant.now())
      export     = GDPRDataExport(subjectId, tables.toMap, now)
      _         <- logAudit(requestId, GDPRRight.RightToAccess, subjectId, "completed")
    } yield export

  def erasureRequest(subjectId: String): IO[ErasureResult] =
    for {
      _         <- validateSubjectId(subjectId)
      requestId <- IO.delay(UUID.randomUUID())
      _         <- logAudit(requestId, GDPRRight.RightToErasure, subjectId, "processing")
      counts    <- subjectDataTables.traverse(t => deleteFromTable(t, subjectId))
      affected   = counts.count(_ > 0)
      total      = counts.sum
      _         <- logAudit(requestId, GDPRRight.RightToErasure, subjectId, "completed")
    } yield ErasureResult(subjectId, affected, total)

  def rectificationRequest(subjectId: String, corrections: Map[String, String]): IO[Unit] =
    for {
      _         <- validateSubjectId(subjectId)
      _         <- IO.raiseWhen(corrections.isEmpty)(
                     new IllegalArgumentException("Corrections map must not be empty")
                   )
      requestId <- IO.delay(UUID.randomUUID())
      _         <- logAudit(requestId, GDPRRight.RightToRectification, subjectId, "processing")
      _         <- applyCorrections(subjectId, corrections)
      _         <- logAudit(requestId, GDPRRight.RightToRectification, subjectId, "completed")
    } yield ()

  def portabilityExport(subjectId: String, format: ExportFormat): IO[String] =
    for {
      _         <- validateSubjectId(subjectId)
      requestId <- IO.delay(UUID.randomUUID())
      _         <- logAudit(requestId, GDPRRight.RightToPortability, subjectId, "processing")
      export    <- accessRequest(subjectId)
      output     = formatExport(export, format)
      _         <- logAudit(requestId, GDPRRight.RightToPortability, subjectId, "completed")
    } yield output

  private def validateSubjectId(subjectId: String): IO[Unit] =
    IO.raiseWhen(subjectId == null || subjectId.trim.isEmpty)(
      new IllegalArgumentException("Subject ID must not be null or empty")
    )

  private def exportTable(tableName: String, subjectId: String): IO[Seq[Map[String, String]]] = {
    val safeTable = sanitizeTableName(tableName)
    val query = s"SELECT * FROM $safeTable WHERE subject_id = ?"
    HC.stream[Map[String, String]](
      query,
      HPS.set(subjectId),
      chunkSize = 512
    )(readRowAsMap).compile.toList.transact(xa).map(_.toSeq)
  }

  private def readRowAsMap: Read[Map[String, String]] =
    Read[String].map(s => Map("data" -> s))

  private def deleteFromTable(tableName: String, subjectId: String): IO[Long] = {
    val safeTable = sanitizeTableName(tableName)
    Fragment.const(s"DELETE FROM $safeTable WHERE subject_id = ")
      .++(fr"$subjectId")
      .update.run.transact(xa).map(_.toLong)
  }

  private def applyCorrections(subjectId: String, corrections: Map[String, String]): IO[Unit] = {
    val setClauses = corrections.keys.toList.map(k => sanitizeColumnName(k))
    val setFragment = setClauses.map(c => Fragment.const(s"$c = ")).zip(
      corrections.values.toList.map(v => fr"$v")
    ).map { case (col, value) => col ++ value }
      .reduceLeft((a, b) => a ++ fr"," ++ b)

    val update = Fragment.const("UPDATE user_profiles SET ") ++ setFragment ++
      fr" WHERE subject_id = $subjectId"
    update.update.run.transact(xa).void
  }

  private def formatExport(dataExport: GDPRDataExport, format: ExportFormat): String =
    format match {
      case ExportFormat.JsonFormat =>
        dataExport.asJson.noSpaces
      case ExportFormat.CsvFormat =>
        val builder = new StringBuilder
        dataExport.tables.foreach { case (table, rows) =>
          val _ = builder.append(s"# $table\n")
          rows.headOption.foreach { firstRow =>
            val _ = builder.append(firstRow.keys.mkString(",")).append("\n")
          }
          rows.foreach { row =>
            val _ = builder.append(row.values.map(escapeCsv).mkString(",")).append("\n")
          }
        }
        builder.toString()
    }

  private def escapeCsv(value: String): String =
    if (value.contains(",") || value.contains("\"") || value.contains("\n"))
      "\"" + value.replace("\"", "\"\"") + "\""
    else value

  private def logAudit(
      requestId: UUID,
      right: GDPRRight,
      subjectId: String,
      status: String
  ): IO[Unit] = {
    val rightStr = right.getClass.getSimpleName.stripSuffix("$")
    val now = Instant.now()
    sql"""INSERT INTO gdpr_audit_log (id, gdpr_right, subject_id, status, logged_at)
          VALUES ($requestId, $rightStr, $subjectId, $status, $now)"""
      .update.run.transact(xa).void
      .handleErrorWith { e =>
        IO.delay(logger.error(s"Failed to write GDPR audit log for request $requestId", e))
      }
  }

  private val tableNamePattern = "^[a-zA-Z_][a-zA-Z0-9_]*$".r

  private def sanitizeTableName(name: String): String =
    tableNamePattern.findFirstIn(name).getOrElse(
      throw new IllegalArgumentException(s"Invalid table name: $name")
    )

  private def sanitizeColumnName(name: String): String =
    tableNamePattern.findFirstIn(name).getOrElse(
      throw new IllegalArgumentException(s"Invalid column name: $name")
    )
}
