package io.gbmm.udps.catalog.quality

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.catalog.profiling.ProfileResult

final case class QualityReport(
    results: Seq[EvaluationResult],
    alertsTriggered: Seq[String]
)

class QualityRulesEngine(evaluator: RuleEvaluator) extends LazyLogging {

  private val DefaultAlertThreshold = 0.1

  def evaluateRules(
      rules: Seq[QualityRule],
      profiles: Map[String, ProfileResult],
      rowCount: Long,
      alertThreshold: Double = DefaultAlertThreshold
  ): IO[QualityReport] =
    IO {
      val results = rules.flatMap { rule =>
        profiles.get(rule.columnName) match {
          case Some(profile) =>
            val result = evaluator.evaluate(rule, profile, rowCount)
            Some(result)
          case None =>
            logger.warn("No profile found for column={} referenced in rule={}", rule.columnName, rule.ruleName)
            None
        }
      }

      val allViolations = results.flatMap(_.violations)
      val alerts = allViolations.collect {
        case v if v.violationRate > alertThreshold =>
          f"ALERT: ${v.ruleName} on ${v.tableName}.${v.columnName} — " +
            f"violation rate ${v.violationRate * 100.0}%.2f%% exceeds threshold ${alertThreshold * 100.0}%.2f%%. " +
            s"Details: ${v.details}"
      }

      alerts.foreach(a => logger.warn(a))

      QualityReport(results = results, alertsTriggered = alerts)
    }
}

object QualityRulesEngine {
  def apply(evaluator: RuleEvaluator): QualityRulesEngine =
    new QualityRulesEngine(evaluator)
}
