package io.gbmm.udps.governance.classification

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

/**
 * A user-defined classification rule that maps a regex pattern to a PII category.
 *
 * @param id        unique identifier for this rule
 * @param name      human-readable name
 * @param pattern   regular expression pattern string
 * @param category  PII category name this rule detects
 * @param priority  execution priority (higher values = checked first)
 * @param createdAt timestamp when this rule was created
 */
final case class CustomClassificationRule(
    id: UUID,
    name: String,
    pattern: String,
    category: String,
    priority: Int,
    createdAt: Instant
)

/** Persistence interface for custom classification rules. */
trait CustomRuleStore {
  def create(rule: CustomClassificationRule): IO[Unit]
  def list: IO[Seq[CustomClassificationRule]]
  def findById(id: UUID): IO[Option[CustomClassificationRule]]
  def update(rule: CustomClassificationRule): IO[Unit]
  def delete(id: UUID): IO[Unit]
}

/** Doobie-backed implementation of CustomRuleStore. */
final class DoobieCustomRuleStore(xa: Transactor[IO])
    extends CustomRuleStore
    with LazyLogging {

  override def create(rule: CustomClassificationRule): IO[Unit] =
    sql"""INSERT INTO custom_classification_rules
          (id, name, pattern, category, priority, created_at)
          VALUES (${rule.id}, ${rule.name}, ${rule.pattern},
                  ${rule.category}, ${rule.priority}, ${rule.createdAt})"""
      .update.run.transact(xa)
      .flatMap { count =>
        if (count == 1) IO.unit
        else IO.raiseError(
          new RuntimeException(
            s"Failed to insert custom rule ${rule.id}, affected rows: $count"
          )
        )
      }
      .handleErrorWith { e =>
        logger.error(s"Error creating custom rule '${rule.name}'", e)
        IO.raiseError(e)
      }

  override def list: IO[Seq[CustomClassificationRule]] =
    sql"""SELECT id, name, pattern, category, priority, created_at
          FROM custom_classification_rules
          ORDER BY priority DESC, name ASC"""
      .query[CustomClassificationRule].to[List].transact(xa)
      .map(_.toSeq)

  override def findById(id: UUID): IO[Option[CustomClassificationRule]] =
    sql"""SELECT id, name, pattern, category, priority, created_at
          FROM custom_classification_rules WHERE id = $id"""
      .query[CustomClassificationRule].option.transact(xa)

  override def update(rule: CustomClassificationRule): IO[Unit] =
    sql"""UPDATE custom_classification_rules
          SET name = ${rule.name}, pattern = ${rule.pattern},
              category = ${rule.category}, priority = ${rule.priority}
          WHERE id = ${rule.id}"""
      .update.run.transact(xa)
      .flatMap { count =>
        if (count == 1) IO.unit
        else IO.raiseError(
          new RuntimeException(
            s"Custom rule ${rule.id} not found for update"
          )
        )
      }
      .handleErrorWith { e =>
        logger.error(s"Error updating custom rule ${rule.id}", e)
        IO.raiseError(e)
      }

  override def delete(id: UUID): IO[Unit] =
    sql"""DELETE FROM custom_classification_rules WHERE id = $id"""
      .update.run.transact(xa).void
}

/** Utility for converting custom rules to PIIPatterns. */
object CustomRules {

  /**
   * Convert a sequence of custom rules into PIIPattern instances.
   * Rules with invalid regex or unknown categories are logged and skipped.
   */
  def loadAsPatterns(rules: Seq[CustomClassificationRule]): Seq[PIIPattern] =
    rules
      .sortBy(-_.priority)
      .flatMap { rule =>
        val categoryOpt = PIICategory.fromName(rule.category)
        val regexOpt = compileRegex(rule.pattern)

        (categoryOpt, regexOpt) match {
          case (Some(cat), Some(regex)) =>
            Some(PIIPattern(
              category = cat,
              regex = regex,
              description = s"Custom rule: ${rule.name}",
              validator = None
            ))
          case _ =>
            None
        }
      }

  private def compileRegex(pattern: String): Option[scala.util.matching.Regex] =
    scala.util.Try(pattern.r).toOption
}
