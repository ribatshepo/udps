package io.gbmm.udps.query.vectorized

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import java.nio.charset.StandardCharsets
import java.util.{ArrayList => JArrayList}
import scala.collection.mutable

/** Aggregate function specifications. */
sealed trait AggFunc extends Product with Serializable

object AggFunc {
  /** SUM(column) - accumulates numeric values. */
  final case class Sum(column: String) extends AggFunc

  /** COUNT(column) - counts non-null values. */
  final case class Count(column: String) extends AggFunc

  /** COUNT(*) - counts all rows. */
  case object CountStar extends AggFunc

  /** AVG(column) - average of numeric values. */
  final case class Avg(column: String) extends AggFunc

  /** MIN(column) - minimum value (numeric or string). */
  final case class Min(column: String) extends AggFunc

  /** MAX(column) - maximum value (numeric or string). */
  final case class Max(column: String) extends AggFunc

  /** COUNT(DISTINCT column) - counts distinct non-null values. */
  final case class CountDistinct(column: String) extends AggFunc
}

/**
 * Specification for an aggregation: the output alias and the aggregate function.
 */
final case class AggSpec(alias: String, func: AggFunc)

/**
 * Vectorized aggregation evaluator over Arrow VectorSchemaRoot.
 *
 * Supports both grouped and ungrouped (scalar) aggregation. When `groupByColumns`
 * is empty, the entire batch is treated as a single group.
 *
 * Thread-safety: stateless object; all mutable state is method-local.
 */
object VectorizedAggregate {

  /**
   * Execute grouped or ungrouped aggregation on the input batch.
   *
   * @param root            input VectorSchemaRoot (NOT closed by this method)
   * @param groupByColumns  column names to group by (empty for scalar aggregation)
   * @param aggregations    list of aggregate specifications
   * @param allocator       Arrow BufferAllocator for output vectors
   * @return new VectorSchemaRoot with group columns (if any) + aggregate result columns
   */
  def apply(
    root: VectorSchemaRoot,
    groupByColumns: Seq[String],
    aggregations: Seq[AggSpec],
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val rowCount = root.getRowCount

    if (groupByColumns.isEmpty) {
      scalarAggregate(root, aggregations, rowCount, allocator)
    } else {
      groupedAggregate(root, groupByColumns, aggregations, rowCount, allocator)
    }
  }

  // ---------------------------------------------------------------------------
  // Scalar aggregation (no GROUP BY)
  // ---------------------------------------------------------------------------

  private def scalarAggregate(
    root: VectorSchemaRoot,
    aggregations: Seq[AggSpec],
    rowCount: Int,
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val accumulators = aggregations.map(spec => createAccumulator(spec.func))

    var i = 0
    while (i < rowCount) {
      accumulators.zip(aggregations).foreach { case (acc, spec) =>
        accumulateRow(acc, spec.func, root, i)
      }
      i += 1
    }

    buildScalarResult(accumulators, aggregations, allocator)
  }

  // ---------------------------------------------------------------------------
  // Grouped aggregation
  // ---------------------------------------------------------------------------

  /**
   * GroupKey wraps a Seq of boxed column values so it can be used as a HashMap key.
   * Equality and hashCode are derived from the underlying sequence.
   */
  private final class GroupKey(val values: Seq[Any]) {
    override def hashCode(): Int = values.hashCode()
    override def equals(obj: Any): Boolean = obj match {
      case other: GroupKey => values == other.values
      case _               => false
    }
  }

  private def groupedAggregate(
    root: VectorSchemaRoot,
    groupByColumns: Seq[String],
    aggregations: Seq[AggSpec],
    rowCount: Int,
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val groupVectors = groupByColumns.map { col =>
      val v = root.getVector(col)
      if (v == null) throw new IllegalArgumentException(s"Group-by column '$col' not found")
      v
    }

    val groups = new mutable.LinkedHashMap[GroupKey, Array[Accumulator]]()

    var i = 0
    while (i < rowCount) {
      val keyValues = groupVectors.map(v => readBoxedValue(v, i))
      val key = new GroupKey(keyValues)

      val accs = groups.getOrElseUpdate(key, aggregations.map(spec => createAccumulator(spec.func)).toArray)
      var ai = 0
      while (ai < aggregations.length) {
        accumulateRow(accs(ai), aggregations(ai).func, root, i)
        ai += 1
      }
      i += 1
    }

    buildGroupedResult(groups, groupByColumns, groupVectors, aggregations, allocator)
  }

  private def readBoxedValue(vector: FieldVector, index: Int): Any = {
    if (vector.isNull(index)) null
    else VectorizedFilter.readSingleValue(vector, index)
  }

  // ---------------------------------------------------------------------------
  // Accumulators
  // ---------------------------------------------------------------------------

  private sealed trait Accumulator

  private final class SumAccumulator(var longSum: Long, var doubleSum: Double, var isFloat: Boolean, var count: Long) extends Accumulator
  private final class CountAccumulator(var count: Long) extends Accumulator
  private final class CountStarAccumulator(var count: Long) extends Accumulator
  private final class AvgAccumulator(var sum: Double, var count: Long) extends Accumulator
  private final class MinAccumulator(var value: Any, var initialized: Boolean) extends Accumulator
  private final class MaxAccumulator(var value: Any, var initialized: Boolean) extends Accumulator
  private final class CountDistinctAccumulator(val seen: mutable.HashSet[Any]) extends Accumulator

  private def createAccumulator(func: AggFunc): Accumulator = func match {
    case _: AggFunc.Sum           => new SumAccumulator(0L, 0.0, isFloat = false, 0L)
    case _: AggFunc.Count         => new CountAccumulator(0L)
    case AggFunc.CountStar        => new CountStarAccumulator(0L)
    case _: AggFunc.Avg           => new AvgAccumulator(0.0, 0L)
    case _: AggFunc.Min           => new MinAccumulator(null, initialized = false)
    case _: AggFunc.Max           => new MaxAccumulator(null, initialized = false)
    case _: AggFunc.CountDistinct => new CountDistinctAccumulator(new mutable.HashSet[Any]())
  }

  private def accumulateRow(acc: Accumulator, func: AggFunc, root: VectorSchemaRoot, rowIndex: Int): Unit = {
    (acc, func) match {
      case (a: SumAccumulator, AggFunc.Sum(col)) =>
        val vec = root.getVector(col)
        if (!vec.isNull(rowIndex)) {
          a.count += 1
          vec match {
            case v: IntVector      => a.longSum += v.get(rowIndex).toLong
            case v: BigIntVector   => a.longSum += v.get(rowIndex)
            case v: Float4Vector   => a.doubleSum += v.get(rowIndex).toDouble; a.isFloat = true
            case v: Float8Vector   => a.doubleSum += v.get(rowIndex); a.isFloat = true
            case v: SmallIntVector => a.longSum += v.get(rowIndex).toLong
            case v: TinyIntVector  => a.longSum += v.get(rowIndex).toLong
            case _ => throw new UnsupportedOperationException(s"SUM not supported for ${vec.getClass.getSimpleName}")
          }
        }

      case (a: CountAccumulator, AggFunc.Count(col)) =>
        val vec = root.getVector(col)
        if (!vec.isNull(rowIndex)) a.count += 1

      case (a: CountStarAccumulator, AggFunc.CountStar) =>
        a.count += 1

      case (a: AvgAccumulator, AggFunc.Avg(col)) =>
        val vec = root.getVector(col)
        if (!vec.isNull(rowIndex)) {
          a.count += 1
          vec match {
            case v: IntVector      => a.sum += v.get(rowIndex).toDouble
            case v: BigIntVector   => a.sum += v.get(rowIndex).toDouble
            case v: Float4Vector   => a.sum += v.get(rowIndex).toDouble
            case v: Float8Vector   => a.sum += v.get(rowIndex)
            case v: SmallIntVector => a.sum += v.get(rowIndex).toDouble
            case v: TinyIntVector  => a.sum += v.get(rowIndex).toDouble
            case _ => throw new UnsupportedOperationException(s"AVG not supported for ${vec.getClass.getSimpleName}")
          }
        }

      case (a: MinAccumulator, AggFunc.Min(col)) =>
        val vec = root.getVector(col)
        if (!vec.isNull(rowIndex)) {
          val currentVal = VectorizedFilter.readSingleValue(vec, rowIndex)
          if (!a.initialized) {
            a.value = currentVal
            a.initialized = true
          } else {
            if (compareForMinMax(currentVal, a.value) < 0) a.value = currentVal
          }
        }

      case (a: MaxAccumulator, AggFunc.Max(col)) =>
        val vec = root.getVector(col)
        if (!vec.isNull(rowIndex)) {
          val currentVal = VectorizedFilter.readSingleValue(vec, rowIndex)
          if (!a.initialized) {
            a.value = currentVal
            a.initialized = true
          } else {
            if (compareForMinMax(currentVal, a.value) > 0) a.value = currentVal
          }
        }

      case (a: CountDistinctAccumulator, AggFunc.CountDistinct(col)) =>
        val vec = root.getVector(col)
        if (!vec.isNull(rowIndex)) {
          a.seen += VectorizedFilter.readSingleValue(vec, rowIndex)
        }

      case _ =>
        throw new IllegalStateException(
          s"Accumulator/function mismatch: ${acc.getClass.getSimpleName} / ${func.getClass.getSimpleName}"
        )
    }
  }

  private def compareForMinMax(a: Any, b: Any): Int = (a, b) match {
    case (x: Int, y: Int)       => Integer.compare(x, y)
    case (x: Long, y: Long)     => java.lang.Long.compare(x, y)
    case (x: Float, y: Float)   => java.lang.Float.compare(x, y)
    case (x: Double, y: Double) => java.lang.Double.compare(x, y)
    case (x: String, y: String) => x.compareTo(y)
    case (x: Number, y: Number) => java.lang.Double.compare(x.doubleValue(), y.doubleValue())
    case _ =>
      throw new UnsupportedOperationException(
        s"MIN/MAX comparison not supported between ${a.getClass.getSimpleName} and ${b.getClass.getSimpleName}"
      )
  }

  // ---------------------------------------------------------------------------
  // Result building (scalar)
  // ---------------------------------------------------------------------------

  private def buildScalarResult(
    accumulators: Seq[Accumulator],
    aggregations: Seq[AggSpec],
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val outputRowCount = 1
    val fields  = new JArrayList[Field](aggregations.size)
    val vectors = new JArrayList[FieldVector](aggregations.size)

    accumulators.zip(aggregations).foreach { case (acc, spec) =>
      val vec = writeAccumulatorToVector(acc, spec.alias, outputRowCount, allocator)
      vec.setValueCount(outputRowCount)
      fields.add(vec.getField)
      vectors.add(vec)
    }

    val schema = new Schema(fields)
    new VectorSchemaRoot(schema, vectors, outputRowCount)
  }

  // ---------------------------------------------------------------------------
  // Result building (grouped)
  // ---------------------------------------------------------------------------

  private def buildGroupedResult(
    groups: mutable.LinkedHashMap[GroupKey, Array[Accumulator]],
    groupByColumns: Seq[String],
    groupVectors: Seq[FieldVector],
    aggregations: Seq[AggSpec],
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val groupCount = groups.size
    val fields  = new JArrayList[Field]()
    val vectors = new JArrayList[FieldVector]()

    val groupKeys = groups.keys.toArray

    groupByColumns.zip(groupVectors).zipWithIndex.foreach { case ((colName, srcVec), colIdx) =>
      val outVec = createOutputGroupVector(srcVec, colName, groupCount, allocator)
      var gi = 0
      while (gi < groupCount) {
        val keyVal = groupKeys(gi).values(colIdx)
        writeBoxedValue(outVec, gi, keyVal)
        gi += 1
      }
      outVec.setValueCount(groupCount)
      fields.add(outVec.getField)
      vectors.add(outVec)
    }

    val groupAccs = groups.values.toArray

    aggregations.zipWithIndex.foreach { case (spec, aggIdx) =>
      val aggVec = createAggResultVector(spec, groupCount, allocator)
      var gi = 0
      while (gi < groupCount) {
        writeAccumulatorValue(groupAccs(gi)(aggIdx), aggVec, gi)
        gi += 1
      }
      aggVec.setValueCount(groupCount)
      fields.add(aggVec.getField)
      vectors.add(aggVec)
    }

    val schema = new Schema(fields)
    new VectorSchemaRoot(schema, vectors, groupCount)
  }

  // ---------------------------------------------------------------------------
  // Vector creation and writing helpers
  // ---------------------------------------------------------------------------

  private def writeAccumulatorToVector(
    acc: Accumulator,
    alias: String,
    rowCount: Int,
    allocator: BufferAllocator
  ): FieldVector = acc match {

    case a: SumAccumulator =>
      if (a.isFloat) {
        val v = newFloat8Vector(alias, allocator, rowCount)
        if (a.count == 0L) v.setNull(0) else v.setSafe(0, a.doubleSum + a.longSum.toDouble)
        v
      } else {
        val v = newBigIntVector(alias, allocator, rowCount)
        if (a.count == 0L) v.setNull(0) else v.setSafe(0, a.longSum)
        v
      }

    case a: CountAccumulator =>
      val v = newBigIntVector(alias, allocator, rowCount)
      v.setSafe(0, a.count)
      v

    case a: CountStarAccumulator =>
      val v = newBigIntVector(alias, allocator, rowCount)
      v.setSafe(0, a.count)
      v

    case a: AvgAccumulator =>
      val v = newFloat8Vector(alias, allocator, rowCount)
      if (a.count == 0L) v.setNull(0) else v.setSafe(0, a.sum / a.count.toDouble)
      v

    case a: MinAccumulator =>
      if (!a.initialized) {
        val v = newBigIntVector(alias, allocator, rowCount)
        v.setNull(0)
        v
      } else {
        writeMinMaxValue(a.value, alias, allocator)
      }

    case a: MaxAccumulator =>
      if (!a.initialized) {
        val v = newBigIntVector(alias, allocator, rowCount)
        v.setNull(0)
        v
      } else {
        writeMinMaxValue(a.value, alias, allocator)
      }

    case a: CountDistinctAccumulator =>
      val v = newBigIntVector(alias, allocator, rowCount)
      v.setSafe(0, a.seen.size.toLong)
      v
  }

  private def writeMinMaxValue(value: Any, alias: String, allocator: BufferAllocator): FieldVector = {
    val rowCount = 1
    value match {
      case v: Int =>
        val vec = newIntVector(alias, allocator, rowCount)
        vec.setSafe(0, v)
        vec
      case v: Long =>
        val vec = newBigIntVector(alias, allocator, rowCount)
        vec.setSafe(0, v)
        vec
      case v: Float =>
        val vec = newFloat4Vector(alias, allocator, rowCount)
        vec.setSafe(0, v)
        vec
      case v: Double =>
        val vec = newFloat8Vector(alias, allocator, rowCount)
        vec.setSafe(0, v)
        vec
      case v: String =>
        val vec = newVarCharVector(alias, allocator, rowCount)
        vec.setSafe(0, v.getBytes(StandardCharsets.UTF_8))
        vec
      case _ =>
        throw new UnsupportedOperationException(s"MIN/MAX result type not supported: ${value.getClass.getSimpleName}")
    }
  }

  private def createOutputGroupVector(
    srcVec: FieldVector,
    name: String,
    rows: Int,
    allocator: BufferAllocator
  ): FieldVector = srcVec match {
    case _: IntVector      => newIntVector(name, allocator, rows)
    case _: BigIntVector   => newBigIntVector(name, allocator, rows)
    case _: Float4Vector   => newFloat4Vector(name, allocator, rows)
    case _: Float8Vector   => newFloat8Vector(name, allocator, rows)
    case _: VarCharVector  => newVarCharVector(name, allocator, rows)
    case _: BitVector      => newBitVector(name, allocator, rows)
    case _: SmallIntVector => newIntVector(name, allocator, rows)
    case _: TinyIntVector  => newIntVector(name, allocator, rows)
    case _ =>
      throw new UnsupportedOperationException(
        s"Group-by not supported for vector type ${srcVec.getClass.getSimpleName}"
      )
  }

  private def writeBoxedValue(vec: FieldVector, index: Int, value: Any): Unit = {
    if (value == null) {
      vec match {
        case v: IntVector     => v.setNull(index)
        case v: BigIntVector  => v.setNull(index)
        case v: Float4Vector  => v.setNull(index)
        case v: Float8Vector  => v.setNull(index)
        case v: VarCharVector => v.setNull(index)
        case v: BitVector     => v.setNull(index)
        case _ => throw new UnsupportedOperationException(s"setNull not supported for ${vec.getClass.getSimpleName}")
      }
    } else {
      (vec, value) match {
        case (v: IntVector, i: Int)         => v.setSafe(index, i)
        case (v: IntVector, s: Short)       => v.setSafe(index, s.toInt)
        case (v: IntVector, b: Byte)        => v.setSafe(index, b.toInt)
        case (v: BigIntVector, l: Long)     => v.setSafe(index, l)
        case (v: BigIntVector, i: Int)      => v.setSafe(index, i.toLong)
        case (v: Float4Vector, f: Float)    => v.setSafe(index, f)
        case (v: Float8Vector, d: Double)   => v.setSafe(index, d)
        case (v: VarCharVector, s: String)  => v.setSafe(index, s.getBytes(StandardCharsets.UTF_8))
        case (v: BitVector, i: Int)         => v.setSafe(index, i)
        case _ =>
          throw new UnsupportedOperationException(
            s"writeBoxedValue not supported: vec=${vec.getClass.getSimpleName}, val=${value.getClass.getSimpleName}"
          )
      }
    }
  }

  private def createAggResultVector(spec: AggSpec, rows: Int, allocator: BufferAllocator): FieldVector = spec.func match {
    case _: AggFunc.Sum           => newFloat8Vector(spec.alias, allocator, rows)
    case _: AggFunc.Count         => newBigIntVector(spec.alias, allocator, rows)
    case AggFunc.CountStar        => newBigIntVector(spec.alias, allocator, rows)
    case _: AggFunc.Avg           => newFloat8Vector(spec.alias, allocator, rows)
    case _: AggFunc.Min           => newFloat8Vector(spec.alias, allocator, rows)
    case _: AggFunc.Max           => newFloat8Vector(spec.alias, allocator, rows)
    case _: AggFunc.CountDistinct => newBigIntVector(spec.alias, allocator, rows)
  }

  private def writeAccumulatorValue(acc: Accumulator, vec: FieldVector, index: Int): Unit = (acc, vec) match {
    case (a: SumAccumulator, v: Float8Vector) =>
      if (a.count == 0L) v.setNull(index)
      else if (a.isFloat) v.setSafe(index, a.doubleSum + a.longSum.toDouble)
      else v.setSafe(index, a.longSum.toDouble)

    case (a: CountAccumulator, v: BigIntVector) =>
      v.setSafe(index, a.count)

    case (a: CountStarAccumulator, v: BigIntVector) =>
      v.setSafe(index, a.count)

    case (a: AvgAccumulator, v: Float8Vector) =>
      if (a.count == 0L) v.setNull(index)
      else v.setSafe(index, a.sum / a.count.toDouble)

    case (a: MinAccumulator, v: Float8Vector) =>
      if (!a.initialized) v.setNull(index)
      else v.setSafe(index, toDouble(a.value))

    case (a: MaxAccumulator, v: Float8Vector) =>
      if (!a.initialized) v.setNull(index)
      else v.setSafe(index, toDouble(a.value))

    case (a: CountDistinctAccumulator, v: BigIntVector) =>
      v.setSafe(index, a.seen.size.toLong)

    case _ =>
      throw new IllegalStateException(
        s"Cannot write ${acc.getClass.getSimpleName} to ${vec.getClass.getSimpleName}"
      )
  }

  private def toDouble(value: Any): Double = value match {
    case v: Int    => v.toDouble
    case v: Long   => v.toDouble
    case v: Float  => v.toDouble
    case v: Double => v
    case v: Short  => v.toDouble
    case v: Byte   => v.toDouble
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot convert ${value.getClass.getSimpleName} to Double for aggregate result"
      )
  }

  // ---------------------------------------------------------------------------
  // Vector factory helpers
  // ---------------------------------------------------------------------------

  private def newIntVector(name: String, alloc: BufferAllocator, rows: Int): IntVector = {
    val ft = new FieldType(true, new ArrowType.Int(32, true), null)
    val v  = new IntVector(name, ft, alloc)
    v.allocateNew(rows)
    v
  }

  private def newBigIntVector(name: String, alloc: BufferAllocator, rows: Int): BigIntVector = {
    val ft = new FieldType(true, new ArrowType.Int(64, true), null)
    val v  = new BigIntVector(name, ft, alloc)
    v.allocateNew(rows)
    v
  }

  private def newFloat4Vector(name: String, alloc: BufferAllocator, rows: Int): Float4Vector = {
    val ft = new FieldType(true, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE), null)
    val v  = new Float4Vector(name, ft, alloc)
    v.allocateNew(rows)
    v
  }

  private def newFloat8Vector(name: String, alloc: BufferAllocator, rows: Int): Float8Vector = {
    val ft = new FieldType(true, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE), null)
    val v  = new Float8Vector(name, ft, alloc)
    v.allocateNew(rows)
    v
  }

  private def newVarCharVector(name: String, alloc: BufferAllocator, rows: Int): VarCharVector = {
    val ft = new FieldType(true, ArrowType.Utf8.INSTANCE, null)
    val v  = new VarCharVector(name, ft, alloc)
    v.allocateNew(rows)
    v
  }

  private def newBitVector(name: String, alloc: BufferAllocator, rows: Int): BitVector = {
    val ft = new FieldType(true, ArrowType.Bool.INSTANCE, null)
    val v  = new BitVector(name, ft, alloc)
    v.allocateNew(rows)
    v
  }
}
