package io.gbmm.udps.query.calcite

import io.gbmm.udps.core.domain.TableMetadata
import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.schema.{FilterableTable, Statistic}
import org.apache.calcite.util.ImmutableBitSet

import java.util
import scala.jdk.CollectionConverters._

/** Provider of row data for a UDPS table.
  *
  * Each invocation of `scanRows` returns an iterator of rows.
  * A row is represented as an `Array[AnyRef]` where each element corresponds
  * to a column in the table schema, in order.
  */
trait TableDataProvider {

  /** Return all rows for the given table, optionally applying the supplied
    * Calcite filter expressions for predicate pushdown.
    *
    * Implementations that cannot interpret a filter should leave it in the
    * `filters` list so that Calcite applies it post-scan. Filters that have
    * been fully handled should be removed from the mutable list.
    *
    * @param tableMetadata metadata of the table being scanned
    * @param filters       mutable list of conjunctive filter predicates;
    *                      implementations may remove entries they fully evaluate
    * @return an iterator over rows (each row is an `Array[AnyRef]`)
    */
  def scanRows(
    tableMetadata: TableMetadata,
    filters: java.util.List[RexNode]
  ): Iterator[Array[AnyRef]]
}

/** A Calcite table backed by UDPS TableMetadata and a pluggable data provider.
  *
  * Implements `FilterableTable` to support both full scans and predicate
  * pushdown. When Calcite invokes `scan` with filters, they are forwarded to
  * the `TableDataProvider` which may choose to evaluate some or all of them.
  */
final class UDPSTable(
  val tableMetadata: TableMetadata,
  dataProvider: TableDataProvider
) extends AbstractTable
    with FilterableTable {

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType =
    TypeMapper.buildRowType(tableMetadata.schema.columns, typeFactory)

  override def scan(
    root: DataContext,
    filters: java.util.List[RexNode]
  ): Enumerable[Array[AnyRef]] = {
    val meta = tableMetadata
    val provider = dataProvider

    new AbstractEnumerable[Array[AnyRef]] {
      override def enumerator(): Enumerator[Array[AnyRef]] = {
        val rows = provider.scanRows(meta, filters)
        new IteratorEnumerator(rows)
      }
    }
  }

  override def getStatistic: Statistic = new UDPSStatistic(tableMetadata)
}

/** Wraps a Scala `Iterator` as a Calcite `Enumerator`. */
private[calcite] final class IteratorEnumerator(iter: Iterator[Array[AnyRef]])
    extends Enumerator[Array[AnyRef]] {

  private var _current: Array[AnyRef] = _

  override def current(): Array[AnyRef] = _current

  override def moveNext(): Boolean =
    if (iter.hasNext) {
      _current = iter.next()
      true
    } else {
      false
    }

  override def reset(): Unit =
    throw new UnsupportedOperationException(
      "Reset is not supported on a streaming enumerator"
    )

  override def close(): Unit = ()
}

/** Exposes UDPS table-level and column-level statistics to the Calcite planner. */
private[calcite] final class UDPSStatistic(meta: TableMetadata) extends Statistic {

  override def getRowCount: java.lang.Double =
    java.lang.Double.valueOf(meta.rowCount.toDouble)

  override def isKey(columns: ImmutableBitSet): Boolean =
    meta.schema.primaryKey.exists { pkCols =>
      val pkIndices = pkCols.flatMap { pkName =>
        val idx = meta.schema.columns.indexWhere(_.name == pkName)
        if (idx >= 0) Some(idx) else None
      }.toSet
      val requestedIndices = columns.asSet().asScala.map(_.intValue()).toSet
      pkIndices.nonEmpty && pkIndices.subsetOf(requestedIndices)
    }

  override def getKeys: util.List[ImmutableBitSet] =
    meta.schema.primaryKey match {
      case Some(pkCols) =>
        val indices = pkCols.flatMap { pkName =>
          val idx = meta.schema.columns.indexWhere(_.name == pkName)
          if (idx >= 0) Some(idx) else None
        }
        if (indices.nonEmpty) {
          val bitSet = ImmutableBitSet.of(indices: _*)
          java.util.Collections.singletonList(bitSet)
        } else {
          java.util.Collections.emptyList()
        }
      case None => java.util.Collections.emptyList()
    }

  override def getCollations: util.List[org.apache.calcite.rel.RelCollation] =
    java.util.Collections.emptyList()

  override def getReferentialConstraints: util.List[org.apache.calcite.rel.RelReferentialConstraint] =
    java.util.Collections.emptyList()

  override def getDistribution: org.apache.calcite.rel.RelDistribution =
    org.apache.calcite.rel.RelDistributions.ANY
}

object UDPSTable {

  /** Retrieve column-level distinct count from UDPS statistics, if available.
    * Useful for external cost estimation.
    */
  def columnCardinality(meta: TableMetadata, columnName: String): Option[Long] =
    meta.schema.columns
      .find(_.name == columnName)
      .flatMap(_.statistics)
      .map(_.distinctCount)

  /** Retrieve column-level null count from UDPS statistics, if available. */
  def columnNullCount(meta: TableMetadata, columnName: String): Option[Long] =
    meta.schema.columns
      .find(_.name == columnName)
      .flatMap(_.statistics)
      .map(_.nullCount)
}
