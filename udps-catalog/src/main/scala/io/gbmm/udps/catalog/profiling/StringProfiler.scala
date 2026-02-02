package io.gbmm.udps.catalog.profiling

import scala.util.matching.Regex

object StringProfiler {

  private val TopValuesLimit = 10

  private val EmailPattern: Regex = """^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$""".r
  private val PhonePattern: Regex = """^\+?[0-9\s\-().]{7,20}$""".r
  private val UrlPattern: Regex = """^https?://[^\s]+$""".r
  private val UuidPattern: Regex = """^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$""".r
  private val NumericOnlyPattern: Regex = """^[0-9]+(\.[0-9]+)?$""".r
  private val AlphaOnlyPattern: Regex = """^[a-zA-Z]+$""".r

  private val PatternDefinitions: Seq[(String, Regex)] = Seq(
    ("email", EmailPattern),
    ("phone", PhonePattern),
    ("url", UrlPattern),
    ("uuid", UuidPattern),
    ("numeric_only", NumericOnlyPattern),
    ("alpha_only", AlphaOnlyPattern)
  )

  def profile(values: Seq[String]): StringProfile = {
    if (values.isEmpty) {
      return StringProfile(
        minLength = 0,
        maxLength = 0,
        avgLength = 0.0,
        patterns = Seq.empty,
        nullCount = 0L,
        distinctCount = 0L,
        topValues = Seq.empty
      )
    }

    val lengths = values.map(_.length)
    val minLen = lengths.min
    val maxLen = lengths.max
    val avgLen = lengths.sum.toDouble / lengths.size

    val totalCount = values.size.toLong
    val patterns = detectPatterns(values, totalCount)

    val frequencyMap = values.groupBy(identity).view.mapValues(_.size.toLong).toMap
    val topValues = frequencyMap.toSeq.sortBy(-_._2).take(TopValuesLimit)
    val distinctCount = frequencyMap.size.toLong

    StringProfile(
      minLength = minLen,
      maxLength = maxLen,
      avgLength = avgLen,
      patterns = patterns,
      nullCount = 0L,
      distinctCount = distinctCount,
      topValues = topValues
    )
  }

  private def detectPatterns(values: Seq[String], totalCount: Long): Seq[PatternInfo] =
    PatternDefinitions.flatMap { case (name, regex) =>
      val matchCount = values.count(v => regex.findFirstIn(v).isDefined).toLong
      if (matchCount > 0L) {
        val pct = if (totalCount > 0L) matchCount.toDouble / totalCount * 100.0 else 0.0
        Some(PatternInfo(name, matchCount, pct))
      } else {
        None
      }
    }
}
