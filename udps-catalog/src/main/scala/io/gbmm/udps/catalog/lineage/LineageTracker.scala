package io.gbmm.udps.catalog.lineage

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

/** A directed edge in the data lineage graph. */
final case class LineageEdge(
  id: UUID,
  sourceTableId: UUID,
  sourceColumnId: Option[UUID],
  targetTableId: UUID,
  targetColumnId: Option[UUID],
  queryId: Option[UUID],
  createdAt: Instant
)

/** Storage abstraction for lineage edges. */
trait LineageStore {
  def addEdge(edge: LineageEdge): IO[Unit]
  def getEdgesForTable(tableId: UUID): IO[Seq[LineageEdge]]
  def getUpstream(tableId: UUID): IO[Seq[LineageEdge]]
  def getDownstream(tableId: UUID): IO[Seq[LineageEdge]]
  def deleteEdgesForTable(tableId: UUID): IO[Unit]
}

/** Doobie-backed implementation of LineageStore. */
class DoobieLineageStore(xa: Transactor[IO]) extends LineageStore {

  override def addEdge(edge: LineageEdge): IO[Unit] =
    sql"""INSERT INTO lineage_edges (id, source_table_id, source_column_id, target_table_id, target_column_id, query_id, created_at)
          VALUES (${edge.id}, ${edge.sourceTableId}, ${edge.sourceColumnId}, ${edge.targetTableId}, ${edge.targetColumnId}, ${edge.queryId}, ${edge.createdAt})
          ON CONFLICT (id) DO NOTHING"""
      .update.run.transact(xa).void

  override def getEdgesForTable(tableId: UUID): IO[Seq[LineageEdge]] =
    sql"""SELECT id, source_table_id, source_column_id, target_table_id, target_column_id, query_id, created_at
          FROM lineage_edges
          WHERE source_table_id = $tableId OR target_table_id = $tableId
          ORDER BY created_at DESC"""
      .query[LineageEdge].to[Seq].transact(xa)

  override def getUpstream(tableId: UUID): IO[Seq[LineageEdge]] =
    sql"""SELECT id, source_table_id, source_column_id, target_table_id, target_column_id, query_id, created_at
          FROM lineage_edges
          WHERE target_table_id = $tableId
          ORDER BY created_at DESC"""
      .query[LineageEdge].to[Seq].transact(xa)

  override def getDownstream(tableId: UUID): IO[Seq[LineageEdge]] =
    sql"""SELECT id, source_table_id, source_column_id, target_table_id, target_column_id, query_id, created_at
          FROM lineage_edges
          WHERE source_table_id = $tableId
          ORDER BY created_at DESC"""
      .query[LineageEdge].to[Seq].transact(xa)

  override def deleteEdgesForTable(tableId: UUID): IO[Unit] =
    sql"""DELETE FROM lineage_edges
          WHERE source_table_id = $tableId OR target_table_id = $tableId"""
      .update.run.transact(xa).void
}

/**
 * Tracks data lineage by analyzing SQL queries and recording edges
 * between source and target tables/columns.
 */
class LineageTracker(store: LineageStore, extractor: LineageExtractor) extends LazyLogging {

  /**
   * Analyzes a SQL query, extracts lineage information, and persists edges.
   *
   * @param sql          the SQL query to analyze
   * @param queryId      unique identifier for this query execution
   * @param tableMapping mapping of table names (lowercase) to their catalog UUIDs
   * @return the lineage edges that were created
   */
  def trackQuery(
    sql: String,
    queryId: UUID,
    tableMapping: Map[String, UUID]
  ): IO[Seq[LineageEdge]] = {
    val lineageInfo = extractor.extract(sql)
    val now = Instant.now()

    val edges = for {
      targetName <- lineageInfo.targetTable.toSeq
      targetId   <- tableMapping.get(targetName).toSeq
      sourceName <- lineageInfo.sourceTables.toSeq
      sourceId   <- tableMapping.get(sourceName).toSeq
    } yield LineageEdge(
      id = UUID.randomUUID(),
      sourceTableId = sourceId,
      sourceColumnId = None,
      targetTableId = targetId,
      targetColumnId = None,
      queryId = Some(queryId),
      createdAt = now
    )

    IO(logger.info("Tracking lineage for query {}: {} source(s) -> target {}",
      queryId, lineageInfo.sourceTables.size.toString, lineageInfo.targetTable.getOrElse("none"))) *>
      edges.traverse_(store.addEdge).as(edges)
  }
}
