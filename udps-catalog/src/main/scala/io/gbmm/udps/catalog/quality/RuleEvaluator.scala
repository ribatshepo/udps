package io.gbmm.udps.catalog.quality

import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.catalog.profiling._

final case class QualityViolation(
    ruleName: String,
    tableName: String,
    columnName: String,
    violationCount: Long,
    violationRate: Double,
    details: String
)

final case class EvaluationResult(
    violations: Seq[QualityViolation],
    suggestions: Seq[String]
)

class RuleEvaluator(tableName: String) extends LazyLogging {

  def evaluate(rule: QualityRule, profile: ProfileResult, rowCount: Long): EvaluationResult =
    rule match {
      case r: CompletenessRule => evaluateCompleteness(r, profile, rowCount)
      case r: ValidityRule     => evaluateValidity(r, profile, rowCount)
      case r: UniquenessRule   => evaluateUniqueness(r, profile, rowCount)
      case r: ConsistencyRule  => evaluateConsistency(r)
    }

  private def evaluateCompleteness(
      rule: CompletenessRule,
      profile: ProfileResult,
      rowCount: Long
  ): EvaluationResult = {
    val nullCount = extractNullCount(profile)
    val nonNullPct = if (rowCount > 0L) (rowCount - nullCount).toDouble / rowCount * 100.0 else 0.0

    if (nonNullPct < rule.minNonNullPercent) {
      val violationCount = nullCount
      val violationRate = if (rowCount > 0L) nullCount.toDouble / rowCount else 0.0
      val details = f"Non-null percentage $nonNullPct%.2f%% is below required ${rule.minNonNullPercent}%.2f%%"
      logger.info("Completeness violation on column={}: {}", rule.columnName, details)
      EvaluationResult(
        violations = Seq(QualityViolation(
          ruleName = rule.ruleName,
          tableName = tableName,
          columnName = rule.columnName,
          violationCount = violationCount,
          violationRate = violationRate,
          details = details
        )),
        suggestions = Seq(
          s"Consider adding a NOT NULL constraint on column '${rule.columnName}'",
          s"Investigate data source for missing values in '${rule.columnName}'"
        )
      )
    } else {
      EvaluationResult(violations = Seq.empty, suggestions = Seq.empty)
    }
  }

  private def evaluateValidity(
      rule: ValidityRule,
      profile: ProfileResult,
      rowCount: Long
  ): EvaluationResult =
    rule.validationType match {
      case ValidationType.RangeCheck(min, max) =>
        evaluateRangeValidity(rule, profile, rowCount, min, max)
      case ValidationType.RegexCheck(pattern) =>
        evaluateRegexValidity(rule, profile, rowCount, pattern)
      case ValidationType.InSetCheck(validValues) =>
        evaluateInSetValidity(rule, profile, rowCount, validValues)
    }

  private def evaluateRangeValidity(
      rule: ValidityRule,
      profile: ProfileResult,
      rowCount: Long,
      min: Double,
      max: Double
  ): EvaluationResult = profile match {
    case np: NumericProfile =>
      val outOfRange = (np.min < min, np.max > max) match {
        case (true, true) =>
          Some(s"Values found outside range [$min, $max]: min=${np.min}, max=${np.max}")
        case (true, false) =>
          Some(f"Values found below minimum $min: column min=${np.min}")
        case (false, true) =>
          Some(f"Values found above maximum $max: column max=${np.max}")
        case (false, false) =>
          None
      }
      outOfRange match {
        case Some(details) =>
          val estimatedViolations = np.outlierCount
          val rate = if (rowCount > 0L) estimatedViolations.toDouble / rowCount else 0.0
          EvaluationResult(
            violations = Seq(QualityViolation(
              ruleName = rule.ruleName,
              tableName = tableName,
              columnName = rule.columnName,
              violationCount = estimatedViolations,
              violationRate = rate,
              details = details
            )),
            suggestions = Seq(
              s"Add a CHECK constraint on '${rule.columnName}' to enforce range [$min, $max]",
              s"Review data pipeline for out-of-range values in '${rule.columnName}'"
            )
          )
        case None =>
          EvaluationResult(violations = Seq.empty, suggestions = Seq.empty)
      }
    case _ =>
      EvaluationResult(
        violations = Seq.empty,
        suggestions = Seq(s"Range check on '${rule.columnName}' skipped: column is not numeric")
      )
  }

  private def evaluateRegexValidity(
      rule: ValidityRule,
      profile: ProfileResult,
      rowCount: Long,
      pattern: String
  ): EvaluationResult = profile match {
    case sp: StringProfile =>
      val matchingPattern = sp.patterns.find(_.pattern == pattern)
      matchingPattern match {
        case Some(pi) if pi.matchPercentage < 100.0 =>
          val nonMatchCount = rowCount - sp.nullCount - pi.matchCount
          val rate = if (rowCount > 0L) nonMatchCount.toDouble / rowCount else 0.0
          EvaluationResult(
            violations = Seq(QualityViolation(
              ruleName = rule.ruleName,
              tableName = tableName,
              columnName = rule.columnName,
              violationCount = nonMatchCount,
              violationRate = rate,
              details = f"Only ${pi.matchPercentage}%.2f%% of values match pattern '$pattern'"
            )),
            suggestions = Seq(
              s"Add validation for pattern '$pattern' in data ingestion for '${rule.columnName}'"
            )
          )
        case Some(_) =>
          EvaluationResult(violations = Seq.empty, suggestions = Seq.empty)
        case None =>
          val nonNullCount = rowCount - sp.nullCount
          val rate = if (rowCount > 0L) nonNullCount.toDouble / rowCount else 0.0
          EvaluationResult(
            violations = Seq(QualityViolation(
              ruleName = rule.ruleName,
              tableName = tableName,
              columnName = rule.columnName,
              violationCount = nonNullCount,
              violationRate = rate,
              details = s"No values match pattern '$pattern'"
            )),
            suggestions = Seq(
              s"Verify pattern '$pattern' is correct for column '${rule.columnName}'"
            )
          )
      }
    case _ =>
      EvaluationResult(
        violations = Seq.empty,
        suggestions = Seq(s"Regex check on '${rule.columnName}' skipped: column is not a string type")
      )
  }

  private def evaluateInSetValidity(
      rule: ValidityRule,
      profile: ProfileResult,
      rowCount: Long,
      validValues: Set[String]
  ): EvaluationResult = profile match {
    case sp: StringProfile =>
      val invalidTopValues = sp.topValues.filterNot { case (v, _) => validValues.contains(v) }
      if (invalidTopValues.nonEmpty) {
        val invalidCount = invalidTopValues.map(_._2).sum
        val rate = if (rowCount > 0L) invalidCount.toDouble / rowCount else 0.0
        val invalidSample = invalidTopValues.take(5).map(_._1).mkString(", ")
        EvaluationResult(
          violations = Seq(QualityViolation(
            ruleName = rule.ruleName,
            tableName = tableName,
            columnName = rule.columnName,
            violationCount = invalidCount,
            violationRate = rate,
            details = s"Values not in allowed set found: [$invalidSample]"
          )),
          suggestions = Seq(
            s"Add an ENUM constraint or lookup table for '${rule.columnName}'",
            s"Update allowed values set if [$invalidSample] are valid"
          )
        )
      } else {
        EvaluationResult(violations = Seq.empty, suggestions = Seq.empty)
      }
    case _ =>
      EvaluationResult(
        violations = Seq.empty,
        suggestions = Seq(s"InSet check on '${rule.columnName}' skipped: column is not a string type")
      )
  }

  private def evaluateUniqueness(
      rule: UniquenessRule,
      profile: ProfileResult,
      rowCount: Long
  ): EvaluationResult = {
    val distinctCount = extractDistinctCount(profile)
    val nonNullCount = rowCount - extractNullCount(profile)
    if (distinctCount < nonNullCount && nonNullCount > 0L) {
      val duplicates = nonNullCount - distinctCount
      val rate = duplicates.toDouble / rowCount
      EvaluationResult(
        violations = Seq(QualityViolation(
          ruleName = rule.ruleName,
          tableName = tableName,
          columnName = rule.columnName,
          violationCount = duplicates,
          violationRate = rate,
          details = s"Found $duplicates duplicate values out of $nonNullCount non-null rows (distinct=$distinctCount)"
        )),
        suggestions = Seq(
          s"Add a UNIQUE constraint on column '${rule.columnName}'",
          s"Investigate duplicate values in '${rule.columnName}' for data integrity"
        )
      )
    } else {
      EvaluationResult(violations = Seq.empty, suggestions = Seq.empty)
    }
  }

  private def evaluateConsistency(rule: ConsistencyRule): EvaluationResult =
    EvaluationResult(
      violations = Seq.empty,
      suggestions = Seq(
        s"Consistency check for '${rule.sourceColumn}' -> " +
          s"'${rule.referenceTable}.${rule.referenceColumn}' requires cross-table profiling data"
      )
    )

  private def extractNullCount(profile: ProfileResult): Long = profile match {
    case p: NumericProfile => p.nullCount
    case p: StringProfile  => p.nullCount
    case p: DateProfile    => p.nullCount
    case p: BooleanProfile => p.nullCount
  }

  private def extractDistinctCount(profile: ProfileResult): Long = profile match {
    case p: NumericProfile => p.distinctCount
    case p: StringProfile  => p.distinctCount
    case _: DateProfile    => 0L
    case p: BooleanProfile =>
      var count = 0L
      if (p.trueCount > 0L) count += 1L
      if (p.falseCount > 0L) count += 1L
      count
  }
}

object RuleEvaluator {
  def apply(tableName: String): RuleEvaluator = new RuleEvaluator(tableName)
}
