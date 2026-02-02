package io.gbmm.udps.catalog.discovery

import cats.effect.IO
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import io.circe.syntax._
import io.circe.parser.{decode => circeDecode}
import io.circe.{Decoder, Encoder}

import java.time.Instant

final case class ScanCheckpoint(
  id: Long,
  scanId: String,
  sourceType: String,
  connectionString: String,
  tablesJson: String,
  scannedAt: Instant
)

final case class IncrementalScanResult(
  currentResult: DiscoveryResult,
  changes: Seq[SchemaChangeEvent],
  evolution: EvolutionValidation
)

final class IncrementalScanner(
  engine: SchemaDiscoveryEngine,
  transactor: Transactor[IO]
) extends LazyLogging {

  def scanIncremental(
    scanId: String,
    configs: Seq[ScanConfig]
  ): IO[IncrementalScanResult] =
    for {
      currentResult <- engine.discover(configs)
      previousResults <- configs.toList.traverse(loadPreviousResult(scanId, _))
      previousMerged = mergePreviousResults(previousResults)
      changes = ChangeDetector.detectChanges(previousMerged, currentResult)
      evolution = SchemaEvolution.validate(changes)
      _ <- saveCheckpoints(scanId, configs, currentResult)
      _ <- IO(logger.info(
        "Incremental scan complete: scanId={} tables={} changes={} compatibility={}",
        scanId,
        currentResult.tables.size.toString,
        changes.size.toString,
        evolution.overallLevel.toString
      ))
    } yield IncrementalScanResult(
      currentResult = currentResult,
      changes = changes,
      evolution = evolution
    )

  private def loadPreviousResult(scanId: String, config: ScanConfig): IO[DiscoveryResult] = {
    val query = sql"""
      SELECT tables_json FROM scan_checkpoints
      WHERE scan_id = $scanId
        AND source_type = ${config.sourceType}
        AND connection_string = ${config.connectionString}
      ORDER BY scanned_at DESC
      LIMIT 1
    """.query[String].option

    query.transact(transactor).flatMap {
      case Some(json) =>
        IO.fromEither(
          circeDecode[Seq[DiscoveredTable]](json)(
            Decoder.decodeSeq(IncrementalScanner.discoveredTableDecoder)
          )
        ).map(tables => DiscoveryResult(tables = tables, errors = Seq.empty))
          .handleErrorWith { err =>
            IO(logger.warn("Failed to decode previous checkpoint: {}", err.getMessage)) *>
              IO.pure(DiscoveryResult(tables = Seq.empty, errors = Seq.empty))
          }
      case None =>
        IO.pure(DiscoveryResult(tables = Seq.empty, errors = Seq.empty))
    }
  }

  private def mergePreviousResults(results: List[DiscoveryResult]): DiscoveryResult =
    DiscoveryResult(
      tables = results.flatMap(_.tables),
      errors = Seq.empty
    )

  private def saveCheckpoints(
    scanId: String,
    configs: Seq[ScanConfig],
    result: DiscoveryResult
  ): IO[Unit] = {
    val tablesBySource = configs.map { config =>
      val relevantTables = result.tables
      config -> relevantTables
    }

    val inserts = tablesBySource.map { case (config, tables) =>
      val tablesJson = tables.asJson(
        Encoder.encodeSeq(IncrementalScanner.discoveredTableEncoder)
      ).noSpaces

      sql"""
        INSERT INTO scan_checkpoints (scan_id, source_type, connection_string, tables_json, scanned_at)
        VALUES ($scanId, ${config.sourceType}, ${config.connectionString}, $tablesJson, ${Instant.now()})
      """.update.run
    }

    inserts.toList.traverse_(_.transact(transactor)).void
  }
}

object IncrementalScanner {

  import io.circe.generic.semiauto._
  import io.gbmm.udps.core.domain.DataType

  implicit val dataTypeEncoder: Encoder[DataType] = Encoder.encodeString.contramap[DataType](dataTypeToString)
  implicit val dataTypeDecoder: Decoder[DataType] = Decoder.decodeString.emap(stringToDataType)

  implicit val discoveredColumnEncoder: Encoder[DiscoveredColumn] = deriveEncoder[DiscoveredColumn]
  implicit val discoveredColumnDecoder: Decoder[DiscoveredColumn] = deriveDecoder[DiscoveredColumn]

  implicit val discoveredTableEncoder: Encoder[DiscoveredTable] = deriveEncoder[DiscoveredTable]
  implicit val discoveredTableDecoder: Decoder[DiscoveredTable] = deriveDecoder[DiscoveredTable]

  private def dataTypeToString(dt: DataType): String = dt match {
    case DataType.Boolean        => "boolean"
    case DataType.Int8           => "int8"
    case DataType.Int16          => "int16"
    case DataType.Int32          => "int32"
    case DataType.Int64          => "int64"
    case DataType.UInt8          => "uint8"
    case DataType.UInt16         => "uint16"
    case DataType.UInt32         => "uint32"
    case DataType.UInt64         => "uint64"
    case DataType.Float16        => "float16"
    case DataType.Float32        => "float32"
    case DataType.Float64        => "float64"
    case DataType.Decimal(p, s)  => s"decimal($p,$s)"
    case DataType.Utf8           => "utf8"
    case DataType.Binary         => "binary"
    case DataType.Date32         => "date32"
    case DataType.Date64         => "date64"
    case DataType.TimestampSec   => "timestamp_sec"
    case DataType.TimestampMillis => "timestamp_millis"
    case DataType.TimestampMicros => "timestamp_micros"
    case DataType.TimestampNanos => "timestamp_nanos"
    case DataType.Null           => "null"
    case DataType.List(el)       => s"list(${dataTypeToString(el)})"
    case DataType.Struct(_)      => "struct"
    case DataType.Map(k, v)      => s"map(${dataTypeToString(k)},${dataTypeToString(v)})"
  }

  private val DecimalPattern = """decimal\((\d+),(\d+)\)""".r
  private val ListPattern = """list\((.+)\)""".r
  private val MapPattern = """map\((.+),(.+)\)""".r

  private def stringToDataType(s: String): Either[String, DataType] = s match {
    case "boolean"          => Right(DataType.Boolean)
    case "int8"             => Right(DataType.Int8)
    case "int16"            => Right(DataType.Int16)
    case "int32"            => Right(DataType.Int32)
    case "int64"            => Right(DataType.Int64)
    case "uint8"            => Right(DataType.UInt8)
    case "uint16"           => Right(DataType.UInt16)
    case "uint32"           => Right(DataType.UInt32)
    case "uint64"           => Right(DataType.UInt64)
    case "float16"          => Right(DataType.Float16)
    case "float32"          => Right(DataType.Float32)
    case "float64"          => Right(DataType.Float64)
    case "utf8"             => Right(DataType.Utf8)
    case "binary"           => Right(DataType.Binary)
    case "date32"           => Right(DataType.Date32)
    case "date64"           => Right(DataType.Date64)
    case "timestamp_sec"    => Right(DataType.TimestampSec)
    case "timestamp_millis" => Right(DataType.TimestampMillis)
    case "timestamp_micros" => Right(DataType.TimestampMicros)
    case "timestamp_nanos"  => Right(DataType.TimestampNanos)
    case "null"             => Right(DataType.Null)
    case "struct"           => Right(DataType.Struct(Seq.empty))
    case DecimalPattern(p, sc) => Right(DataType.Decimal(p.toInt, sc.toInt))
    case ListPattern(el)    => stringToDataType(el).map(DataType.List)
    case MapPattern(k, v)   =>
      for {
        kt <- stringToDataType(k)
        vt <- stringToDataType(v)
      } yield DataType.Map(kt, vt)
    case other              => Left(s"Unknown data type: $other")
  }

  def create(
    engine: SchemaDiscoveryEngine,
    transactor: Transactor[IO]
  ): IncrementalScanner =
    new IncrementalScanner(engine, transactor)
}
