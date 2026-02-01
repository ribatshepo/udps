package io.gbmm.udps.storage.parquet

import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary

/**
 * Predicate ADT for expressing row-group filter conditions that can be pushed
 * down into the Parquet reader.  The companion [[PredicatePushdown]] object
 * converts these predicates into Parquet [[FilterCompat.Filter]] instances.
 */
sealed trait Predicate extends Product with Serializable

object Predicate {
  final case class Eq(column: String, value: Any) extends Predicate
  final case class NotEq(column: String, value: Any) extends Predicate
  final case class Lt(column: String, value: Any) extends Predicate
  final case class LtEq(column: String, value: Any) extends Predicate
  final case class Gt(column: String, value: Any) extends Predicate
  final case class GtEq(column: String, value: Any) extends Predicate
  final case class In(column: String, values: Set[Any]) extends Predicate
  final case class IsNull(column: String) extends Predicate
  final case class IsNotNull(column: String) extends Predicate
  final case class And(left: Predicate, right: Predicate) extends Predicate
  final case class Or(left: Predicate, right: Predicate) extends Predicate
  final case class Not(predicate: Predicate) extends Predicate
}

/**
 * Converts [[Predicate]] instances into Parquet [[FilterCompat.Filter]] using
 * the Parquet [[FilterApi]].  Supports Int, Long, Float, Double, Boolean, and
 * String value types.  Unrecognised value types cause an
 * [[IllegalArgumentException]].
 */
object PredicatePushdown {

  /**
   * Convert a [[Predicate]] to a Parquet [[FilterCompat.Filter]] suitable for
   * passing to [[org.apache.parquet.hadoop.ParquetFileReader]].
   */
  def toFilter(predicate: Predicate): FilterCompat.Filter =
    FilterCompat.get(toFilterPredicate(predicate))

  /**
   * Convert a [[Predicate]] to the low-level Parquet [[FilterPredicate]].
   */
  def toFilterPredicate(predicate: Predicate): FilterPredicate = predicate match {
    case Predicate.Eq(col, value)    => buildEq(col, value)
    case Predicate.NotEq(col, value) => buildNotEq(col, value)
    case Predicate.Lt(col, value)    => buildLt(col, value)
    case Predicate.LtEq(col, value)  => buildLtEq(col, value)
    case Predicate.Gt(col, value)    => buildGt(col, value)
    case Predicate.GtEq(col, value)  => buildGtEq(col, value)

    case Predicate.In(col, values) =>
      buildIn(col, values)

    case Predicate.IsNull(col) =>
      // IsNull is expressed as eq(col, null) — Parquet handles null semantics
      // for statistics-based filtering.  We use the first value type hint we
      // can infer; default to IntColumn for null-only predicates.
      FilterApi.eq(FilterApi.intColumn(col), null.asInstanceOf[java.lang.Integer])

    case Predicate.IsNotNull(col) =>
      FilterApi.notEq(FilterApi.intColumn(col), null.asInstanceOf[java.lang.Integer])

    case Predicate.And(left, right) =>
      FilterApi.and(toFilterPredicate(left), toFilterPredicate(right))

    case Predicate.Or(left, right) =>
      FilterApi.or(toFilterPredicate(left), toFilterPredicate(right))

    case Predicate.Not(inner) =>
      FilterApi.not(toFilterPredicate(inner))
  }

  // ---------------------------------------------------------------------------
  // Eq
  // ---------------------------------------------------------------------------

  private def buildEq(col: String, value: Any): FilterPredicate = value match {
    case v: java.lang.Integer => FilterApi.eq(FilterApi.intColumn(col), v)
    case v: java.lang.Long    => FilterApi.eq(FilterApi.longColumn(col), v)
    case v: java.lang.Float   => FilterApi.eq(FilterApi.floatColumn(col), v)
    case v: java.lang.Double  => FilterApi.eq(FilterApi.doubleColumn(col), v)
    case v: java.lang.Boolean => FilterApi.eq(FilterApi.booleanColumn(col), v)
    case v: String            => FilterApi.eq(FilterApi.binaryColumn(col), Binary.fromString(v))
    case v: Binary            => FilterApi.eq(FilterApi.binaryColumn(col), v)
    case null                 => FilterApi.eq(FilterApi.intColumn(col), null.asInstanceOf[java.lang.Integer])
    case other                => throw new IllegalArgumentException(
      s"Unsupported value type for Eq predicate on column '$col': ${other.getClass.getName}"
    )
  }

  // ---------------------------------------------------------------------------
  // NotEq
  // ---------------------------------------------------------------------------

  private def buildNotEq(col: String, value: Any): FilterPredicate = value match {
    case v: java.lang.Integer => FilterApi.notEq(FilterApi.intColumn(col), v)
    case v: java.lang.Long    => FilterApi.notEq(FilterApi.longColumn(col), v)
    case v: java.lang.Float   => FilterApi.notEq(FilterApi.floatColumn(col), v)
    case v: java.lang.Double  => FilterApi.notEq(FilterApi.doubleColumn(col), v)
    case v: java.lang.Boolean => FilterApi.notEq(FilterApi.booleanColumn(col), v)
    case v: String            => FilterApi.notEq(FilterApi.binaryColumn(col), Binary.fromString(v))
    case v: Binary            => FilterApi.notEq(FilterApi.binaryColumn(col), v)
    case null                 => FilterApi.notEq(FilterApi.intColumn(col), null.asInstanceOf[java.lang.Integer])
    case other                => throw new IllegalArgumentException(
      s"Unsupported value type for NotEq predicate on column '$col': ${other.getClass.getName}"
    )
  }

  // ---------------------------------------------------------------------------
  // Lt
  // ---------------------------------------------------------------------------

  private def buildLt(col: String, value: Any): FilterPredicate = value match {
    case v: java.lang.Integer => FilterApi.lt(FilterApi.intColumn(col), v)
    case v: java.lang.Long    => FilterApi.lt(FilterApi.longColumn(col), v)
    case v: java.lang.Float   => FilterApi.lt(FilterApi.floatColumn(col), v)
    case v: java.lang.Double  => FilterApi.lt(FilterApi.doubleColumn(col), v)
    case v: String            => FilterApi.lt(FilterApi.binaryColumn(col), Binary.fromString(v))
    case v: Binary            => FilterApi.lt(FilterApi.binaryColumn(col), v)
    case other                => throw new IllegalArgumentException(
      s"Unsupported value type for Lt predicate on column '$col': ${other.getClass.getName}"
    )
  }

  // ---------------------------------------------------------------------------
  // LtEq
  // ---------------------------------------------------------------------------

  private def buildLtEq(col: String, value: Any): FilterPredicate = value match {
    case v: java.lang.Integer => FilterApi.ltEq(FilterApi.intColumn(col), v)
    case v: java.lang.Long    => FilterApi.ltEq(FilterApi.longColumn(col), v)
    case v: java.lang.Float   => FilterApi.ltEq(FilterApi.floatColumn(col), v)
    case v: java.lang.Double  => FilterApi.ltEq(FilterApi.doubleColumn(col), v)
    case v: String            => FilterApi.ltEq(FilterApi.binaryColumn(col), Binary.fromString(v))
    case v: Binary            => FilterApi.ltEq(FilterApi.binaryColumn(col), v)
    case other                => throw new IllegalArgumentException(
      s"Unsupported value type for LtEq predicate on column '$col': ${other.getClass.getName}"
    )
  }

  // ---------------------------------------------------------------------------
  // Gt
  // ---------------------------------------------------------------------------

  private def buildGt(col: String, value: Any): FilterPredicate = value match {
    case v: java.lang.Integer => FilterApi.gt(FilterApi.intColumn(col), v)
    case v: java.lang.Long    => FilterApi.gt(FilterApi.longColumn(col), v)
    case v: java.lang.Float   => FilterApi.gt(FilterApi.floatColumn(col), v)
    case v: java.lang.Double  => FilterApi.gt(FilterApi.doubleColumn(col), v)
    case v: String            => FilterApi.gt(FilterApi.binaryColumn(col), Binary.fromString(v))
    case v: Binary            => FilterApi.gt(FilterApi.binaryColumn(col), v)
    case other                => throw new IllegalArgumentException(
      s"Unsupported value type for Gt predicate on column '$col': ${other.getClass.getName}"
    )
  }

  // ---------------------------------------------------------------------------
  // GtEq
  // ---------------------------------------------------------------------------

  private def buildGtEq(col: String, value: Any): FilterPredicate = value match {
    case v: java.lang.Integer => FilterApi.gtEq(FilterApi.intColumn(col), v)
    case v: java.lang.Long    => FilterApi.gtEq(FilterApi.longColumn(col), v)
    case v: java.lang.Float   => FilterApi.gtEq(FilterApi.floatColumn(col), v)
    case v: java.lang.Double  => FilterApi.gtEq(FilterApi.doubleColumn(col), v)
    case v: String            => FilterApi.gtEq(FilterApi.binaryColumn(col), Binary.fromString(v))
    case v: Binary            => FilterApi.gtEq(FilterApi.binaryColumn(col), v)
    case other                => throw new IllegalArgumentException(
      s"Unsupported value type for GtEq predicate on column '$col': ${other.getClass.getName}"
    )
  }

  // ---------------------------------------------------------------------------
  // In — expressed as a chain of Or(Eq, Eq, ...)
  // ---------------------------------------------------------------------------

  private def buildIn(col: String, values: Set[Any]): FilterPredicate = {
    require(values.nonEmpty, s"In predicate on column '$col' requires at least one value")
    values.iterator
      .map(v => buildEq(col, v))
      .reduceLeft[FilterPredicate] { (acc, pred) =>
        FilterApi.or(acc, pred)
      }
  }
}
