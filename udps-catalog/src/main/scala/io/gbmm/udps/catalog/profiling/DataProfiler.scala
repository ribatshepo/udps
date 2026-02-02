package io.gbmm.udps.catalog.profiling

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging

sealed trait ProfileResult

final case class NumericProfile(
    min: Double,
    max: Double,
    mean: Double,
    median: Double,
    stddev: Double,
    quartiles: (Double, Double, Double),
    histogram: Seq[HistogramBucket],
    nullCount: Long,
    distinctCount: Long,
    outlierCount: Long
) extends ProfileResult

final case class StringProfile(
    minLength: Int,
    maxLength: Int,
    avgLength: Double,
    patterns: Seq[PatternInfo],
    nullCount: Long,
    distinctCount: Long,
    topValues: Seq[(String, Long)]
) extends ProfileResult

final case class DateProfile(
    minDate: String,
    maxDate: String,
    rangeDays: Long,
    nullCount: Long
) extends ProfileResult

final case class BooleanProfile(
    trueCount: Long,
    falseCount: Long,
    nullCount: Long
) extends ProfileResult

final case class HistogramBucket(
    lowerBound: Double,
    upperBound: Double,
    count: Long
)

final case class PatternInfo(
    pattern: String,
    matchCount: Long,
    matchPercentage: Double
)

class DataProfiler extends LazyLogging {

  private val DefaultSampleSize = 100000

  def profileColumn(data: Seq[Any], columnType: String): ProfileResult = {
    val normalizedType = columnType.toLowerCase.trim
    normalizedType match {
      case t if isNumericType(t)  => profileNumericColumn(data)
      case t if isStringType(t)   => profileStringColumn(data)
      case t if isDateType(t)     => profileDateColumn(data)
      case t if isBooleanType(t)  => profileBooleanColumn(data)
      case _                      => profileStringColumn(data)
    }
  }

  def profileTable(
      rows: Seq[Seq[Any]],
      columns: Seq[(String, String)]
  ): IO[Map[String, ProfileResult]] = {
    val tasks: List[IO[(String, ProfileResult)]] = columns.toList.zipWithIndex.map {
      case ((colName, colType), idx) =>
        IO {
          val columnData: Seq[Any] = rows.map { row =>
            if (idx < row.size) row(idx) else null
          }
          logger.debug("Profiling column={} type={} rows={}", colName, colType, columnData.size.toString)
          (colName, profileColumn(columnData, colType))
        }
    }
    tasks.parSequence.map(_.toMap)
  }

  def sampleAndProfile(
      rows: Seq[Seq[Any]],
      columns: Seq[(String, String)],
      sampleSize: Int = DefaultSampleSize
  ): IO[Map[String, ProfileResult]] = {
    val sampled = if (rows.size <= sampleSize) {
      rows
    } else {
      val rng = new scala.util.Random(42L)
      rng.shuffle(rows).take(sampleSize)
    }
    IO(logger.info("Sampling {} rows from {} total for profiling", sampled.size.toString, rows.size.toString)) *>
      profileTable(sampled, columns)
  }

  private def profileNumericColumn(data: Seq[Any]): NumericProfile = {
    val (nullCount, nonNull) = partitionNulls(data)
    val doubles = nonNull.flatMap(toDouble)
    val result = NumericProfiler.profile(doubles)
    result.copy(nullCount = nullCount)
  }

  private def profileStringColumn(data: Seq[Any]): StringProfile = {
    val (nullCount, nonNull) = partitionNulls(data)
    val strings = nonNull.map(_.toString)
    val result = StringProfiler.profile(strings)
    result.copy(nullCount = nullCount)
  }

  private def profileDateColumn(data: Seq[Any]): DateProfile = {
    val (nullCount, nonNull) = partitionNulls(data)
    val strings = nonNull.map(_.toString)
    val result = DateProfiler.profile(strings)
    result.copy(nullCount = result.nullCount + nullCount)
  }

  private def profileBooleanColumn(data: Seq[Any]): BooleanProfile = {
    val (nullCount, nonNull) = partitionNulls(data)
    var trueCount = 0L
    var falseCount = 0L
    nonNull.foreach { v =>
      val boolVal = v match {
        case b: Boolean => b
        case s: String  => s.toLowerCase match {
          case "true" | "yes" | "1" | "t" | "y" => true
          case _                                 => false
        }
        case n: Number => n.intValue() != 0
        case _         => false
      }
      if (boolVal) trueCount += 1L else falseCount += 1L
    }
    BooleanProfile(trueCount = trueCount, falseCount = falseCount, nullCount = nullCount)
  }

  private def partitionNulls(data: Seq[Any]): (Long, Seq[Any]) = {
    var nullCount = 0L
    val nonNull = data.filter {
      case null =>
        nullCount += 1L
        false
      case _ => true
    }
    (nullCount, nonNull)
  }

  private def toDouble(v: Any): Option[Double] = v match {
    case d: Double      => Some(d)
    case f: Float       => Some(f.toDouble)
    case l: Long        => Some(l.toDouble)
    case i: Int         => Some(i.toDouble)
    case s: Short       => Some(s.toDouble)
    case b: Byte        => Some(b.toDouble)
    case bd: BigDecimal => Some(bd.toDouble)
    case n: Number      => Some(n.doubleValue())
    case s: String      => scala.util.Try(s.toDouble).toOption
    case _              => None
  }

  private def isNumericType(t: String): Boolean =
    t.contains("int") || t.contains("long") || t.contains("float") ||
      t.contains("double") || t.contains("decimal") || t.contains("numeric") ||
      t.contains("number") || t.contains("bigint") || t.contains("smallint") ||
      t.contains("tinyint") || t.contains("real")

  private def isStringType(t: String): Boolean =
    t.contains("string") || t.contains("varchar") || t.contains("char") ||
      t.contains("text") || t.contains("clob") || t.contains("nvarchar")

  private def isDateType(t: String): Boolean =
    t.contains("date") || t.contains("time") || t.contains("timestamp")

  private def isBooleanType(t: String): Boolean =
    t.contains("bool") || t.contains("boolean") || t.contains("bit")
}

object DataProfiler {
  def apply(): DataProfiler = new DataProfiler()
}
