package io.gbmm.udps.catalog.profiling

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import scala.util.Try

object DateProfiler {

  private val DateFormatters: Seq[DateTimeFormatter] = Seq(
    DateTimeFormatter.ISO_LOCAL_DATE,
    DateTimeFormatter.ISO_OFFSET_DATE_TIME,
    DateTimeFormatter.ISO_DATE_TIME,
    DateTimeFormatter.ofPattern("yyyy/MM/dd"),
    DateTimeFormatter.ofPattern("dd-MM-yyyy"),
    DateTimeFormatter.ofPattern("dd/MM/yyyy"),
    DateTimeFormatter.ofPattern("MM-dd-yyyy"),
    DateTimeFormatter.ofPattern("MM/dd/yyyy"),
    DateTimeFormatter.ofPattern("yyyyMMdd")
  )

  def profile(values: Seq[String]): DateProfile = {
    if (values.isEmpty) {
      return DateProfile(
        minDate = "",
        maxDate = "",
        rangeDays = 0L,
        nullCount = 0L
      )
    }

    val parsed = values.flatMap(parseDate)
    if (parsed.isEmpty) {
      return DateProfile(
        minDate = "",
        maxDate = "",
        rangeDays = 0L,
        nullCount = values.size.toLong
      )
    }

    val sorted = parsed.sorted
    val minDate = sorted.head
    val maxDate = sorted.last
    val rangeDays = ChronoUnit.DAYS.between(minDate, maxDate)
    val unparsedCount = (values.size - parsed.size).toLong

    DateProfile(
      minDate = minDate.toString,
      maxDate = maxDate.toString,
      rangeDays = rangeDays,
      nullCount = unparsedCount
    )
  }

  private def parseDate(value: String): Option[LocalDate] = {
    val trimmed = value.trim
    if (trimmed.isEmpty) return None
    DateFormatters.view.flatMap { fmt =>
      Try(LocalDate.parse(trimmed, fmt)).toOption
        .orElse(Try(LocalDate.parse(trimmed.take(10), fmt)).toOption)
    }.headOption
  }
}
