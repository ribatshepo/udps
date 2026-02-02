package io.gbmm.udps.catalog.discovery

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.core.domain.DataType

final case class ScanConfig(
  sourceType: String,
  connectionString: String,
  options: Map[String, String]
)

final case class DiscoveredColumn(
  name: String,
  dataType: DataType,
  nullable: Boolean,
  ordinalPosition: Int
)

final case class DiscoveredTable(
  name: String,
  columns: Seq[DiscoveredColumn],
  rowCount: Option[Long],
  primaryKey: Option[Seq[String]]
)

final case class DiscoveryResult(
  tables: Seq[DiscoveredTable],
  errors: Seq[String]
)

final case class ScanProgress(
  total: Int,
  completed: Int,
  currentSource: String
)

trait DataSourceScanner {
  def sourceType: String
  def scan(config: ScanConfig): IO[DiscoveryResult]
}

final class SchemaDiscoveryEngine(
  scanners: Seq[DataSourceScanner],
  progressRef: Ref[IO, ScanProgress]
) extends LazyLogging {

  private val MaxParallelScans = 4

  def discover(configs: Seq[ScanConfig]): IO[DiscoveryResult] =
    for {
      _ <- progressRef.set(ScanProgress(total = configs.size, completed = 0, currentSource = ""))
      results <- configs.toList.parTraverseN(MaxParallelScans)(scanSingle)
    } yield mergeResults(results)

  def progress: IO[ScanProgress] = progressRef.get

  private def scanSingle(config: ScanConfig): IO[DiscoveryResult] = {
    val scannerOpt = scanners.find(_.sourceType == config.sourceType)
    scannerOpt match {
      case Some(scanner) =>
        for {
          _ <- progressRef.update(p => p.copy(currentSource = config.connectionString))
          result <- scanner.scan(config).handleErrorWith { err =>
            IO.pure(DiscoveryResult(
              tables = Seq.empty,
              errors = Seq(s"Scanner ${config.sourceType} failed for ${config.connectionString}: ${err.getMessage}")
            ))
          }
          _ <- progressRef.update(p => p.copy(completed = p.completed + 1))
          _ <- IO(logger.info("Completed scan for source={} type={} tables={} errors={}",
            config.connectionString, config.sourceType, result.tables.size.toString, result.errors.size.toString))
        } yield result

      case None =>
        IO.pure(DiscoveryResult(
          tables = Seq.empty,
          errors = Seq(s"No scanner registered for source type: ${config.sourceType}")
        ))
    }
  }

  private def mergeResults(results: List[DiscoveryResult]): DiscoveryResult =
    DiscoveryResult(
      tables = results.flatMap(_.tables),
      errors = results.flatMap(_.errors)
    )
}

object SchemaDiscoveryEngine {
  def create(scanners: Seq[DataSourceScanner]): IO[SchemaDiscoveryEngine] =
    Ref.of[IO, ScanProgress](ScanProgress(total = 0, completed = 0, currentSource = ""))
      .map(ref => new SchemaDiscoveryEngine(scanners, ref))
}
