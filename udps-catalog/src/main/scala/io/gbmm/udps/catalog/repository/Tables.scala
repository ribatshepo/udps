package io.gbmm.udps.catalog.repository

import java.time.Instant
import java.util.UUID

/** Catalog domain case classes representing database rows.
  * These are the CATALOG persistence model, separate from core domain types.
  */

final case class CatalogDatabase(
    id: UUID,
    name: String,
    description: Option[String],
    createdAt: Instant,
    updatedAt: Instant
)

final case class CatalogSchema(
    id: UUID,
    databaseId: UUID,
    name: String,
    createdAt: Instant,
    updatedAt: Instant
)

final case class CatalogTable(
    id: UUID,
    schemaId: UUID,
    name: String,
    rowCount: Long,
    sizeBytes: Long,
    tier: String,
    createdAt: Instant,
    updatedAt: Instant
)

final case class CatalogColumn(
    id: UUID,
    tableId: UUID,
    name: String,
    dataType: String,
    nullable: Boolean,
    indexed: Boolean,
    ftsEnabled: Boolean,
    piiClassified: Boolean,
    ordinalPosition: Int
)

final case class CatalogPartition(
    id: UUID,
    tableId: UUID,
    partitionKey: String,
    partitionValue: String
)

final case class CatalogLineageEdge(
    id: UUID,
    sourceTableId: Option[UUID],
    sourceColumnId: Option[UUID],
    targetTableId: Option[UUID],
    targetColumnId: Option[UUID],
    queryId: Option[UUID],
    createdAt: Instant
)

final case class CatalogProfile(
    id: UUID,
    tableId: Option[UUID],
    columnId: Option[UUID],
    statsJson: String,
    createdAt: Instant
)

final case class CatalogTag(
    id: UUID,
    name: String,
    category: Option[String]
)

final case class CatalogTableTag(
    tableId: UUID,
    tagId: UUID
)

final case class CatalogGlossaryTerm(
    id: UUID,
    term: String,
    definition: Option[String],
    relatedColumns: List[UUID]
)

final case class CatalogSnapshot(
    id: UUID,
    tableId: UUID,
    snapshotTime: Instant,
    filePathsJson: String
)

final case class CatalogQueryHistory(
    id: UUID,
    sqlText: String,
    userId: Option[String],
    startTime: Instant,
    endTime: Option[Instant],
    durationMs: Option[Long],
    rowsReturned: Option[Long],
    bytesScanned: Option[Long],
    status: String,
    errorMessage: Option[String]
)

final case class CatalogQualityViolation(
    id: UUID,
    tableId: Option[UUID],
    columnId: Option[UUID],
    ruleName: String,
    violationCount: Option[Long],
    violationRate: Option[Double],
    details: Option[String],
    createdAt: Instant
)

final case class CatalogScanCheckpoint(
    id: UUID,
    sourceType: String,
    sourceIdentifier: String,
    lastScanTime: Instant,
    checkpointData: Option[String]
)
