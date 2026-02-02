package io.gbmm.udps.query.optimizer

import io.gbmm.udps.core.domain.{ColumnStatistics => CoreColumnStatistics}

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/** Column-level statistics used by the cost-based optimizer. */
final case class ColumnStats(
  distinctCount: Long,
  nullCount: Long,
  minValue: Option[Comparable[_]],
  maxValue: Option[Comparable[_]],
  avgSize: Option[Long]
) {

  /** Selectivity estimate for an equality predicate on this column.
    * Returns a value in [0.0, 1.0].
    */
  def equalitySelectivity: Double =
    if (distinctCount > 0) 1.0 / distinctCount.toDouble
    else 1.0

  /** Selectivity estimate for a range predicate on this column.
    * Uses a heuristic of 1/3 when min/max are unavailable.
    */
  def rangeSelectivity: Double = {
    val RANGE_SELECTIVITY_HEURISTIC = 1.0 / 3.0
    RANGE_SELECTIVITY_HEURISTIC
  }
}

/** Table-level statistics aggregating row count, byte size, and per-column stats. */
final case class TableStatistics(
  rowCount: Long,
  sizeBytes: Long,
  columnStats: Map[String, ColumnStats]
) {

  /** Average row size in bytes, derived from total size and row count. */
  def avgRowSize: Long =
    if (rowCount > 0) Math.max(1L, sizeBytes / rowCount)
    else 1L
}

/** Provides statistics for tables and columns to the cost-based optimizer. */
trait StatisticsProvider {

  /** Retrieve table-level statistics for the given table in the given namespace. */
  def getTableStatistics(tableName: String, namespace: String): Option[TableStatistics]

  /** Retrieve column-level statistics for a specific column. */
  def getColumnStatistics(
    tableName: String,
    namespace: String,
    columnName: String
  ): Option[ColumnStats]
}

object StatisticsProvider {

  /** Convert a core domain ColumnStatistics instance to the optimizer ColumnStats.
    *
    * Core domain stores min/max as Option[String]. The optimizer wraps
    * them as Comparable[_] for use in Calcite metadata handlers.
    */
  def fromCoreColumnStatistics(core: CoreColumnStatistics): ColumnStats =
    ColumnStats(
      distinctCount = core.distinctCount,
      nullCount = core.nullCount,
      minValue = core.minValue.map(v => v: Comparable[_]),
      maxValue = core.maxValue.map(v => v: Comparable[_]),
      avgSize = core.avgSize
    )
}

/** Thread-safe in-memory implementation of StatisticsProvider.
  *
  * Stores statistics in a ConcurrentHashMap keyed by (namespace, tableName).
  * Suitable for use until the catalog service is wired in.
  */
final class InMemoryStatisticsProvider extends StatisticsProvider {

  private val store: ConcurrentHashMap[StatisticsKey, TableStatistics] =
    new ConcurrentHashMap[StatisticsKey, TableStatistics]()

  override def getTableStatistics(tableName: String, namespace: String): Option[TableStatistics] =
    Option(store.get(StatisticsKey(namespace, tableName)))

  override def getColumnStatistics(
    tableName: String,
    namespace: String,
    columnName: String
  ): Option[ColumnStats] =
    getTableStatistics(tableName, namespace).flatMap(_.columnStats.get(columnName))

  /** Register or replace statistics for a table. */
  def putTableStatistics(
    tableName: String,
    namespace: String,
    statistics: TableStatistics
  ): Unit = {
    val _ = store.put(StatisticsKey(namespace, tableName), statistics)
  }

  /** Remove statistics for a table. Returns true if an entry was removed. */
  def removeTableStatistics(tableName: String, namespace: String): Boolean =
    store.remove(StatisticsKey(namespace, tableName)) != null

  /** Return all registered table keys. */
  def registeredTables: Set[StatisticsKey] =
    store.keySet().asScala.toSet

  /** Clear all stored statistics. */
  def clear(): Unit = store.clear()
}

/** Composite key for the statistics store. */
final case class StatisticsKey(namespace: String, tableName: String)
