package io.gbmm.udps.governance.gdpr

import java.util.UUID

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

/** Defines a data retention rule for a specific table. */
final case class RetentionRule(
    id: UUID,
    tableName: String,
    retentionDays: Int,
    dateColumn: String,
    enabled: Boolean
)

/** Aggregated result from enforcing all retention rules. */
final case class RetentionResult(
    rulesProcessed: Int,
    totalRowsDeleted: Long,
    errors: Seq[String]
)

/** Engine that manages and enforces data retention policies. */
final class RetentionPolicyEngine(xa: Transactor[IO]) extends LazyLogging {

  private val identifierPattern = "^[a-zA-Z_][a-zA-Z0-9_]*$".r

  def addRule(rule: RetentionRule): IO[Unit] =
    for {
      _ <- validateRule(rule)
      _ <- sql"""INSERT INTO retention_rules (id, table_name, retention_days, date_column, enabled)
                 VALUES (${rule.id}, ${rule.tableName}, ${rule.retentionDays},
                         ${rule.dateColumn}, ${rule.enabled})"""
            .update.run.transact(xa).void
      _ <- IO.delay(logger.info(
             s"Retention rule added: table=${rule.tableName} retention=${rule.retentionDays}d"
           ))
    } yield ()

  def listRules: IO[Seq[RetentionRule]] =
    sql"""SELECT id, table_name, retention_days, date_column, enabled
          FROM retention_rules ORDER BY table_name"""
      .query[RetentionRule].to[List].transact(xa).map(_.toSeq)

  def enforceAll: IO[RetentionResult] =
    for {
      rules   <- listRules
      enabled  = rules.filter(_.enabled)
      results <- enabled.toList.traverse { rule =>
                   enforce(rule)
                     .map(count => Right(count): Either[String, Long])
                     .handleErrorWith { e =>
                       val msg = s"Error enforcing rule for ${rule.tableName}: ${e.getMessage}"
                       IO.delay(logger.error(msg, e)).as(Left(msg): Either[String, Long])
                     }
                 }
      deleted  = results.collect { case Right(c) => c }
      errors   = results.collect { case Left(e) => e }
      total    = deleted.sum
      _       <- IO.delay(logger.info(
                   s"Retention enforcement complete: rules=${enabled.size} deleted=$total errors=${errors.size}"
                 ))
    } yield RetentionResult(enabled.size, total, errors.toSeq)

  def enforce(rule: RetentionRule): IO[Long] =
    for {
      _     <- validateRule(rule)
      table  = sanitizeIdentifier(rule.tableName, "table name")
      col    = sanitizeIdentifier(rule.dateColumn, "date column")
      count <- Fragment.const(
                 s"DELETE FROM $table WHERE $col < NOW() - INTERVAL '"
               ).++(Fragment.const(s"${rule.retentionDays} days'"))
                .update.run.transact(xa).map(_.toLong)
      _     <- IO.delay(logger.info(
                 s"Retention enforced: table=$table deleted=$count"
               ))
    } yield count

  private def validateRule(rule: RetentionRule): IO[Unit] =
    for {
      _ <- IO.raiseWhen(rule.tableName == null || rule.tableName.trim.isEmpty)(
             new IllegalArgumentException("Table name must not be null or empty")
           )
      _ <- IO.raiseWhen(rule.dateColumn == null || rule.dateColumn.trim.isEmpty)(
             new IllegalArgumentException("Date column must not be null or empty")
           )
      _ <- IO.raiseWhen(rule.retentionDays <= 0)(
             new IllegalArgumentException("Retention days must be positive")
           )
      _ <- IO.delay(sanitizeIdentifier(rule.tableName, "table name"))
      _ <- IO.delay(sanitizeIdentifier(rule.dateColumn, "date column"))
    } yield ()

  private def sanitizeIdentifier(name: String, label: String): String =
    identifierPattern.findFirstIn(name).getOrElse(
      throw new IllegalArgumentException(s"Invalid $label: $name")
    )
}
