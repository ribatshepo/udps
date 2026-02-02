package io.gbmm.udps.query.calcite

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import io.gbmm.udps.core.domain.TableMetadata
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractSchema

import java.util
import java.util.concurrent.ConcurrentHashMap

/** Provides table metadata for a UDPS namespace to the Calcite schema. */
trait MetadataProvider {

  /** List all table names available in this namespace. */
  def listTableNames: IO[Seq[String]]

  /** Retrieve metadata for a specific table by name. Returns `None` if the table does not exist. */
  def getTableMetadata(tableName: String): IO[Option[TableMetadata]]
}

/** A Calcite Schema backed by a UDPS `MetadataProvider` and `TableDataProvider`.
  *
  * Tables are created lazily on first access and cached in a thread-safe map.
  * The full table map (`getTableMap`) is populated by listing all table names
  * from the metadata provider and then resolving each one.
  *
  * @param metadataProvider supplies table metadata from the UDPS catalog
  * @param dataProvider     supplies row data for table scans
  * @param runtime          Cats Effect IORuntime used to materialise IO operations
  *                         at the Calcite boundary
  */
final class UDPSSchema(
  metadataProvider: MetadataProvider,
  dataProvider: TableDataProvider,
  runtime: IORuntime
) extends AbstractSchema {

  private val tableCache: ConcurrentHashMap[String, Table] = new ConcurrentHashMap[String, Table]()

  override def getTableMap: util.Map[String, Table] = {
    val names = metadataProvider.listTableNames.unsafeRunSync()(runtime)
    names.foreach(resolveAndCache)
    new util.HashMap[String, Table](tableCache)
  }

  /** Resolve a single table from the metadata provider and cache it.
    * If the table does not exist in the catalog, no entry is added to the cache.
    */
  private def resolveAndCache(tableName: String): Unit = {
    val _ = tableCache.computeIfAbsent(
      tableName,
      (_: String) => {
        val maybeMeta = metadataProvider.getTableMetadata(tableName).unsafeRunSync()(runtime)
        maybeMeta match {
          case Some(meta) => new UDPSTable(meta, dataProvider)
          case None       => null // Calcite interprets null as "table not found"
        }
      }
    )
  }

  /** Invalidate the cached table entry, forcing a fresh lookup on next access. */
  def invalidateTable(tableName: String): Unit = {
    val _ = tableCache.remove(tableName)
  }

  /** Invalidate all cached tables. */
  def invalidateAll(): Unit =
    tableCache.clear()
}

object UDPSSchema {

  /** Create a new UDPSSchema wired to the given providers.
    *
    * @param metadataProvider catalog-backed metadata supplier
    * @param dataProvider     row data supplier for table scans
    * @param runtime          IORuntime for materialising effects at the Calcite boundary
    * @return a fully configured UDPSSchema instance
    */
  def apply(
    metadataProvider: MetadataProvider,
    dataProvider: TableDataProvider,
    runtime: IORuntime
  ): UDPSSchema =
    new UDPSSchema(metadataProvider, dataProvider, runtime)
}
