package io.gbmm.udps.catalog.sampling

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import io.circe.syntax._

final case class SamplingResult(
    rows: IndexedSeq[IndexedSeq[Any]],
    metadata: SampleMetadata
)

final class SamplingEngine extends LazyLogging {

  import SamplingStrategy._

  private val SystematicStartOffset = 0

  def sample(
      data: IndexedSeq[IndexedSeq[Any]],
      strategy: SamplingStrategy
  ): IO[SamplingResult] =
    IO.delay {
      val originalCount = data.size.toLong
      val sampled = strategy match {
        case r: RandomSampling     => applyRandom(data, r)
        case s: StratifiedSampling => applyStratified(data, s)
        case s: SystematicSampling => applySystematic(data, s)
        case c: ClusterSampling    => applyCluster(data, c)
        case t: TimeBasedSampling  => applyTimeBased(data, t)
      }
      val seedOpt = extractSeed(strategy)
      val meta = SampleMetadata(
        id = UUID.randomUUID(),
        strategy = strategy,
        originalRowCount = originalCount,
        sampleRowCount = sampled.size.toLong,
        seed = seedOpt,
        createdAt = Instant.now()
      )
      logger.info(
        "Sampling complete: strategy={} original={} sampled={}",
        strategy.getClass.getSimpleName,
        originalCount.toString,
        sampled.size.toString
      )
      SamplingResult(sampled, meta)
    }

  def exportToCsv(result: SamplingResult, columns: Seq[String]): String = {
    val header = columns.mkString(",")
    val rows = result.rows.map { row =>
      row.map(escapeCsvField).mkString(",")
    }
    (header +: rows).mkString("\n")
  }

  def exportToJson(result: SamplingResult, columns: Seq[String]): Json = {
    val rowsJson = result.rows.map { row =>
      val fields = columns.zip(row).map { case (col, value) =>
        col -> valueToJson(value)
      }
      Json.obj(fields: _*)
    }
    Json.obj(
      "metadata" -> result.metadata.asJson(SampleMetadata.encoder),
      "rows" -> Json.arr(rowsJson: _*)
    )
  }

  private def applyRandom(
      data: IndexedSeq[IndexedSeq[Any]],
      config: RandomSampling
  ): IndexedSeq[IndexedSeq[Any]] = {
    if (data.isEmpty) return IndexedSeq.empty
    val rng = new java.util.Random(config.seed)
    val size = math.min(config.sampleSize, data.size)
    val indices = scala.collection.mutable.LinkedHashSet.empty[Int]
    while (indices.size < size) {
      val _ = indices.add(rng.nextInt(data.size))
    }
    indices.toIndexedSeq.sorted.map(data)
  }

  private def applyStratified(
      data: IndexedSeq[IndexedSeq[Any]],
      config: StratifiedSampling
  ): IndexedSeq[IndexedSeq[Any]] = {
    if (data.isEmpty) return IndexedSeq.empty
    val stratumIndex = findColumnIndex(config.stratumColumn, data.head.size)
    val groups = data.groupBy(row => Option(row(stratumIndex)).map(_.toString).getOrElse("__null__"))
    val numStrata = groups.size
    val perStratum = math.max(1, config.sampleSize / numStrata)
    val rng = new java.util.Random(config.seed)
    groups.values.flatMap { groupRows =>
      val shuffled = rng.ints(0, groupRows.size)
        .distinct()
        .limit(math.min(perStratum, groupRows.size).toLong)
        .toArray
      shuffled.map(groupRows)
    }.toIndexedSeq
  }

  private def applySystematic(
      data: IndexedSeq[IndexedSeq[Any]],
      config: SystematicSampling
  ): IndexedSeq[IndexedSeq[Any]] = {
    if (data.isEmpty || config.every <= 0) return IndexedSeq.empty
    val indices = SystematicStartOffset until data.size by config.every
    indices.map(data)
  }

  private def applyCluster(
      data: IndexedSeq[IndexedSeq[Any]],
      config: ClusterSampling
  ): IndexedSeq[IndexedSeq[Any]] = {
    if (data.isEmpty) return IndexedSeq.empty
    val clusterIndex = findColumnIndex(config.clusterColumn, data.head.size)
    val groups = data.groupBy(row => Option(row(clusterIndex)).map(_.toString).getOrElse("__null__"))
    val clusterKeys = groups.keys.toIndexedSeq
    val rng = new java.util.Random(config.seed)
    val numToSelect = math.min(config.numClusters, clusterKeys.size)
    val shuffled = scala.util.Random.javaRandomToRandom(rng).shuffle(clusterKeys)
    val selectedKeys = shuffled.take(numToSelect).toSet
    data.filter { row =>
      val key = Option(row(clusterIndex)).map(_.toString).getOrElse("__null__")
      selectedKeys.contains(key)
    }
  }

  private def applyTimeBased(
      data: IndexedSeq[IndexedSeq[Any]],
      config: TimeBasedSampling
  ): IndexedSeq[IndexedSeq[Any]] = {
    if (data.isEmpty) return IndexedSeq.empty
    val timeIndex = findColumnIndex(config.timeColumn, data.head.size)
    data.filter { row =>
      val value = row(timeIndex)
      parseInstant(value) match {
        case Some(ts) =>
          !ts.isBefore(config.startTime) && !ts.isAfter(config.endTime)
        case None => false
      }
    }
  }

  private def findColumnIndex(columnRef: String, numColumns: Int): Int = {
    val parsed = scala.util.Try(columnRef.toInt)
    parsed match {
      case scala.util.Success(idx) if idx >= 0 && idx < numColumns => idx
      case _ => 0
    }
  }

  private def parseInstant(value: Any): Option[Instant] =
    value match {
      case i: Instant               => Some(i)
      case l: java.lang.Long        => Some(Instant.ofEpochMilli(l.longValue()))
      case l: Long                  => Some(Instant.ofEpochMilli(l))
      case s: String =>
        scala.util.Try(Instant.parse(s)).toOption
      case _ => None
    }

  private def extractSeed(strategy: SamplingStrategy): Option[Long] =
    strategy match {
      case r: RandomSampling     => Some(r.seed)
      case s: StratifiedSampling => Some(s.seed)
      case c: ClusterSampling    => Some(c.seed)
      case _: SystematicSampling => None
      case _: TimeBasedSampling  => None
    }

  private def escapeCsvField(value: Any): String = {
    val str = Option(value).map(_.toString).getOrElse("")
    if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
      "\"" + str.replace("\"", "\"\"") + "\""
    } else {
      str
    }
  }

  private def valueToJson(value: Any): Json =
    value match {
      case null          => Json.Null
      case b: Boolean    => Json.fromBoolean(b)
      case i: Int        => Json.fromInt(i)
      case l: Long       => Json.fromLong(l)
      case d: Double     => Json.fromDoubleOrNull(d)
      case f: Float      => Json.fromFloatOrNull(f)
      case bd: BigDecimal => Json.fromBigDecimal(bd)
      case other         => Json.fromString(other.toString)
    }
}
