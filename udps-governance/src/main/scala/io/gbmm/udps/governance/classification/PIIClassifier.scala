package io.gbmm.udps.governance.classification

import java.time.Instant

import cats.effect.IO
import cats.effect.implicits._
import com.typesafe.scalalogging.LazyLogging

/**
 * Result of classifying a single column for PII.
 *
 * @param columnName   the name of the classified column
 * @param category     detected PII category
 * @param confidence   confidence score between 0.0 and 1.0
 * @param source       detection method (e.g. "column_name_heuristic", "regex_scan", "validator")
 * @param sampleMatches number of sample values that matched
 */
final case class ClassificationResult(
    columnName: String,
    category: PIICategory,
    confidence: Double,
    source: String,
    sampleMatches: Int
)

/**
 * An administrative override for a column's PII classification.
 *
 * @param columnName   the column being overridden
 * @param tableName    the table containing the column
 * @param category     the overridden category (None to clear classification)
 * @param overriddenBy identity of the person who set the override
 * @param overriddenAt timestamp of the override
 */
final case class ClassificationOverride(
    columnName: String,
    tableName: String,
    category: Option[PIICategory],
    overriddenBy: String,
    overriddenAt: Instant
)

/**
 * PII classifier that combines column name heuristics and regex pattern scanning.
 *
 * @param patterns    compiled PII patterns to scan sample data against
 * @param customRules additional custom classification rules (converted to patterns)
 */
final class PIIClassifier(
    patterns: Seq[PIIPattern],
    customRules: Seq[CustomClassificationRule]
) extends LazyLogging {

  private val defaultMaxSamples = 1000
  private val nameHeuristicConfidence = 0.7
  private val validatorConfidenceBoost = 0.15
  private val minimumConfidenceThreshold = 0.1

  private val allPatterns: Seq[PIIPattern] = {
    val custom = CustomRules.loadAsPatterns(customRules)
    custom ++ patterns
  }

  /**
   * Classify a single column by name heuristics and sample data scanning.
   *
   * @param columnName   the column name to check against heuristics
   * @param sampleValues sample data values from the column
   * @param maxSamples   maximum number of samples to scan
   * @return sequence of classification results, sorted by confidence descending
   */
  def classifyColumn(
      columnName: String,
      sampleValues: Seq[String],
      maxSamples: Int = defaultMaxSamples
  ): Seq[ClassificationResult] = {
    val nameResults = classifyByColumnName(columnName)
    val contentResults = classifyByContent(columnName, sampleValues.take(maxSamples))
    val combined = mergeResults(nameResults ++ contentResults)
    combined
      .filter(_.confidence >= minimumConfidenceThreshold)
      .sortBy(r => (-r.confidence, r.category.name))
  }

  /**
   * Classify all columns of a table in parallel.
   *
   * @param columns sequence of (columnName, sampleValues) pairs
   * @return map of column name to classification results
   */
  def classifyTable(
      columns: Seq[(String, Seq[String])]
  ): IO[Map[String, Seq[ClassificationResult]]] = {
    val parallelism = Runtime.getRuntime.availableProcessors().max(1)
    columns.toList
      .parTraverseN(parallelism) { case (name, samples) =>
        IO.delay {
          val results = classifyColumn(name, samples)
          (name, results)
        }
      }
      .map(_.toMap)
  }

  private def classifyByColumnName(columnName: String): Seq[ClassificationResult] = {
    val normalized = columnName.toLowerCase.trim
    BuiltInPatterns.columnNameHeuristics.collect {
      case (pattern, category) if normalized.contains(pattern) =>
        ClassificationResult(
          columnName = columnName,
          category = category,
          confidence = nameHeuristicConfidence,
          source = "column_name_heuristic",
          sampleMatches = 0
        )
    }.toSeq
  }

  private def classifyByContent(
      columnName: String,
      samples: Seq[String]
  ): Seq[ClassificationResult] = {
    if (samples.isEmpty) return Seq.empty

    val nonEmpty = samples.filter(s => s != null && s.nonEmpty)
    if (nonEmpty.isEmpty) return Seq.empty

    val sampleCount = nonEmpty.length.toDouble

    allPatterns.flatMap { pattern =>
      val matches = nonEmpty.filter { value =>
        pattern.regex.findFirstIn(value).isDefined
      }

      if (matches.isEmpty) {
        None
      } else {
        val validatedMatches = pattern.validator match {
          case Some(validate) => matches.filter(validate)
          case None => matches
        }

        if (validatedMatches.isEmpty) {
          None
        } else {
          val baseConfidence = validatedMatches.length / sampleCount
          val confidence = pattern.validator match {
            case Some(_) =>
              math.min(baseConfidence + validatorConfidenceBoost, 1.0)
            case None =>
              baseConfidence
          }

          val source = pattern.validator match {
            case Some(_) => "regex_scan+validator"
            case None => "regex_scan"
          }

          Some(ClassificationResult(
            columnName = columnName,
            category = pattern.category,
            confidence = confidence,
            source = source,
            sampleMatches = validatedMatches.length
          ))
        }
      }
    }
  }

  /** Merge multiple results for the same category, keeping the highest confidence. */
  private def mergeResults(
      results: Seq[ClassificationResult]
  ): Seq[ClassificationResult] =
    results
      .groupBy(_.category)
      .values
      .map { group =>
        group.maxBy(_.confidence)
      }
      .toSeq
}

object PIIClassifier {

  /** Create a classifier with built-in patterns and no custom rules. */
  def default: PIIClassifier =
    new PIIClassifier(BuiltInPatterns.all, Seq.empty)

  /** Create a classifier with built-in patterns and custom rules. */
  def withCustomRules(customRules: Seq[CustomClassificationRule]): PIIClassifier =
    new PIIClassifier(BuiltInPatterns.all, customRules)
}
