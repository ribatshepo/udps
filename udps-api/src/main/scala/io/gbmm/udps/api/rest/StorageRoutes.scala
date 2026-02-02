package io.gbmm.udps.api.rest

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.circe.{Decoder, Encoder, Json}
import io.circe.syntax._
import io.gbmm.udps.api.rest.JsonCodecs.apiResponseEncoder
import io.gbmm.udps.catalog.repository.MetadataRepository
import io.gbmm.udps.core.domain.StorageTier
import io.gbmm.udps.storage.tiering.{DataFileInfo, TierManager}
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.io._

import java.time.Instant
import java.util.UUID

/** Response model for storage statistics of a table. */
final case class StorageStatsResponse(
  tableId: UUID,
  tableName: String,
  rowCount: Long,
  sizeBytes: Long,
  currentTier: String,
  createdAt: String,
  updatedAt: String
)

object StorageStatsResponse {
  implicit val encoder: Encoder[StorageStatsResponse] = Encoder.instance { s =>
    Json.obj(
      "tableId"     -> s.tableId.toString.asJson,
      "tableName"   -> s.tableName.asJson,
      "rowCount"    -> s.rowCount.asJson,
      "sizeBytes"   -> s.sizeBytes.asJson,
      "currentTier" -> s.currentTier.asJson,
      "createdAt"   -> s.createdAt.asJson,
      "updatedAt"   -> s.updatedAt.asJson
    )
  }
}

/** Response model for the tier status of a table. */
final case class TierStatusResponse(
  tableId: UUID,
  currentTier: String,
  recommendedTier: String,
  tierChangePending: Boolean
)

object TierStatusResponse {
  implicit val encoder: Encoder[TierStatusResponse] = Encoder.instance { t =>
    Json.obj(
      "tableId"           -> t.tableId.toString.asJson,
      "currentTier"       -> t.currentTier.asJson,
      "recommendedTier"   -> t.recommendedTier.asJson,
      "tierChangePending" -> t.tierChangePending.asJson
    )
  }
}

/** Request body for triggering a tier rebalance. */
final case class RebalanceRequest(targetTier: String)

object RebalanceRequest {
  implicit val decoder: Decoder[RebalanceRequest] = Decoder.instance { c =>
    c.downField("targetTier").as[String].map(RebalanceRequest(_))
  }
}

/** Response model for a rebalance operation. */
final case class RebalanceResponse(
  tableId: UUID,
  previousTier: String,
  newTier: String,
  transitionedAt: String
)

object RebalanceResponse {
  implicit val encoder: Encoder[RebalanceResponse] = Encoder.instance { r =>
    Json.obj(
      "tableId"         -> r.tableId.toString.asJson,
      "previousTier"    -> r.previousTier.asJson,
      "newTier"         -> r.newTier.asJson,
      "transitionedAt"  -> r.transitionedAt.asJson
    )
  }
}

/** Path extractor for UUID segments. */
private[rest] object UUIDVar {
  def unapply(str: String): Option[UUID] =
    scala.util.Try(UUID.fromString(str)).toOption
}

/**
 * REST routes for storage statistics and tier management.
 *
 * @param metadataRepo  catalog metadata repository for table lookups
 * @param tierManager   storage tier manager for tier evaluation and transitions
 */
class StorageRoutes(
  metadataRepo: MetadataRepository,
  tierManager: TierManager
) extends LazyLogging {

  private val LowAccessDefault: Int = 0

  val routes: HttpRoutes[IO] = ErrorHandler.middleware(HttpRoutes.of[IO] {

    case GET -> Root / "api" / "v1" / "storage" / "tables" / UUIDVar(tableId) / "stats" =>
      metadataRepo.getTable(tableId).flatMap {
        case None =>
          NotFound(ApiError("NOT_FOUND", "Table not found", None).asJson)
        case Some(table) =>
          val response = StorageStatsResponse(
            tableId   = table.id,
            tableName = table.name,
            rowCount  = table.rowCount,
            sizeBytes = table.sizeBytes,
            currentTier = table.tier,
            createdAt = table.createdAt.toString,
            updatedAt = table.updatedAt.toString
          )
          Ok(ApiResponse(data = response, meta = None).asJson)
      }

    case GET -> Root / "api" / "v1" / "storage" / "tables" / UUIDVar(tableId) / "tier" =>
      metadataRepo.getTable(tableId).flatMap {
        case None =>
          NotFound(ApiError("NOT_FOUND", "Table not found", None).asJson)
        case Some(table) =>
          val currentTier = parseTier(table.tier)
          val fileInfo = DataFileInfo(
            filePath            = table.name,
            sizeBytes           = table.sizeBytes,
            createdAt           = table.createdAt,
            lastAccessedAt      = table.updatedAt,
            accessCountLastMonth = LowAccessDefault,
            currentTier         = currentTier
          )
          tierManager.evaluateTier(fileInfo).flatMap { recommended =>
            val response = TierStatusResponse(
              tableId           = table.id,
              currentTier       = tierToString(currentTier),
              recommendedTier   = tierToString(recommended),
              tierChangePending = recommended != currentTier
            )
            Ok(ApiResponse(data = response, meta = None).asJson)
          }
      }

    case req @ POST -> Root / "api" / "v1" / "storage" / "tables" / UUIDVar(tableId) / "rebalance" =>
      req.decodeWith(jsonOf[IO, RebalanceRequest], strict = false) { body =>
        val targetTier = parseTier(body.targetTier)
        metadataRepo.getTable(tableId).flatMap {
          case None =>
            NotFound(ApiError("NOT_FOUND", "Table not found", None).asJson)
          case Some(table) =>
            val currentTier = parseTier(table.tier)
            if (currentTier == targetTier) {
              Ok(ApiResponse(
                data = RebalanceResponse(
                  tableId        = table.id,
                  previousTier   = tierToString(currentTier),
                  newTier        = tierToString(currentTier),
                  transitionedAt = Instant.now().toString
                ),
                meta = None
              ).asJson)
            } else {
              tierManager.transition(table.name, currentTier, targetTier).flatMap { result =>
                val updatedTable = table.copy(
                  tier      = tierToString(result.newTier),
                  updatedAt = result.transitionedAt
                )
                metadataRepo.updateTable(updatedTable).flatMap { _ =>
                  val response = RebalanceResponse(
                    tableId        = table.id,
                    previousTier   = tierToString(result.previousTier),
                    newTier        = tierToString(result.newTier),
                    transitionedAt = result.transitionedAt.toString
                  )
                  Ok(ApiResponse(data = response, meta = None).asJson)
                }
              }
            }
        }
      }
  })

  private def parseTier(tier: String): StorageTier = tier.toLowerCase match {
    case "hot"     => StorageTier.Hot
    case "warm"    => StorageTier.Warm
    case "cold"    => StorageTier.Cold
    case "archive" => StorageTier.Archive
    case other     =>
      throw new IllegalArgumentException(s"Unknown storage tier: $other")
  }

  private def tierToString(tier: StorageTier): String = tier match {
    case StorageTier.Hot     => "hot"
    case StorageTier.Warm    => "warm"
    case StorageTier.Cold    => "cold"
    case StorageTier.Archive => "archive"
  }
}
