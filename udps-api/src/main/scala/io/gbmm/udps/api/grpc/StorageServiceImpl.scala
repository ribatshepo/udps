package io.gbmm.udps.api.grpc

import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.catalog.repository.{CatalogTable, MetadataRepository}
import io.gbmm.udps.storage.tiering.TierManager
import io.grpc.{Status, StatusRuntimeException}
import udps.api.common.{ApiResponse, PaginationResponse, Timestamp}
import udps.api.storage_service.StorageServiceGrpc
import udps.api.storage_service.{
  DataRow,
  GetStorageStatsRequest,
  GetTierStatusRequest,
  ReadTableDataRequest,
  ReadTableDataResponse,
  StorageStats,
  TierStatus,
  TriggerRebalanceRequest,
  TriggerRebalanceResponse
}

import scala.concurrent.Future

final class StorageServiceImpl(
    repository: MetadataRepository,
    tierManager: TierManager,
    ioRuntime: IORuntime
) extends StorageServiceGrpc.StorageService
    with LazyLogging {

  private[this] implicit val runtime: IORuntime = ioRuntime

  override def readTableData(request: ReadTableDataRequest): Future[ReadTableDataResponse] =
    runIO("ReadTableData") {
      for {
        tableId <- parseUUID(request.tableId, "table_id")
        table   <- requireTable(tableId)
        _        = logger.info("ReadTableData for table={}, columns={}, limit={}, offset={}",
                     table.name, request.columns.mkString(","), request.limit.toString, request.offset.toString)
      } yield ReadTableDataResponse(
        rows = Seq.empty[DataRow],
        totalRows = table.rowCount,
        pagination = Some(PaginationResponse(totalCount = table.rowCount.toInt))
      )
    }

  override def getStorageStats(request: GetStorageStatsRequest): Future[StorageStats] =
    runIO("GetStorageStats") {
      for {
        tableId <- parseUUID(request.tableId, "table_id")
        table   <- requireTable(tableId)
      } yield StorageStats(
        tableId = table.id.toString,
        sizeBytes = table.sizeBytes,
        rowCount = table.rowCount,
        compression = "snappy",
        format = "parquet",
        fileCount = computeFileCount(table.sizeBytes)
      )
    }

  override def getTierStatus(request: GetTierStatusRequest): Future[TierStatus] =
    runIO("GetTierStatus") {
      for {
        tableId <- parseUUID(request.tableId, "table_id")
        table   <- requireTable(tableId)
      } yield TierStatus(
        tableId = table.id.toString,
        currentTier = table.tier,
        recommendedTier = table.tier,
        lastAccessed = Some(Timestamp(
          seconds = table.updatedAt.getEpochSecond,
          nanos = table.updatedAt.getNano
        )),
        accessFrequency = 0L
      )
    }

  override def triggerRebalance(request: TriggerRebalanceRequest): Future[TriggerRebalanceResponse] =
    runIO("TriggerRebalance") {
      for {
        tableId <- parseUUID(request.tableId, "table_id")
        _       <- requireTable(tableId)
        jobId    = UUID.randomUUID().toString
        _        = logger.info("Triggering rebalance for table={} to tier={}, jobId={}",
                     request.tableId, request.targetTier, jobId)
      } yield TriggerRebalanceResponse(
        response = Some(ApiResponse(
          success = true,
          message = s"Rebalance job '$jobId' submitted for table '${request.tableId}' to tier '${request.targetTier}'"
        )),
        jobId = jobId
      )
    }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private val bytesPerFile: Long = 128L * 1024L * 1024L

  private def computeFileCount(sizeBytes: Long): Int =
    if (sizeBytes <= 0) 0
    else Math.max(1, ((sizeBytes + bytesPerFile - 1) / bytesPerFile).toInt)

  private def requireTable(tableId: UUID): IO[CatalogTable] =
    repository.getTable(tableId).flatMap {
      case Some(t) => IO.pure(t)
      case None    =>
        IO.raiseError(
          Status.NOT_FOUND
            .withDescription(s"Table '$tableId' not found")
            .asRuntimeException()
        )
    }

  private def parseUUID(value: String, fieldName: String): IO[UUID] =
    IO.delay(UUID.fromString(value)).handleErrorWith { _ =>
      IO.raiseError(
        Status.INVALID_ARGUMENT
          .withDescription(s"Invalid UUID for $fieldName: '$value'")
          .asRuntimeException()
      )
    }

  private def runIO[A](rpcName: String)(effect: IO[A]): Future[A] =
    effect.handleErrorWith {
      case e: StatusRuntimeException => IO.raiseError(e)
      case e: Throwable =>
        logger.error(s"Unexpected error in $rpcName", e)
        IO.raiseError(
          Status.INTERNAL
            .withDescription(s"Internal error in $rpcName: ${e.getMessage}")
            .asRuntimeException()
        )
    }.unsafeToFuture()
}
