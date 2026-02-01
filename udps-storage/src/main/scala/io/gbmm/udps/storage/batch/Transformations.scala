package io.gbmm.udps.storage.batch

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.{Field, Schema}

import java.nio.charset.StandardCharsets
import java.util.{ArrayList => JArrayList}

import scala.jdk.CollectionConverters._

/**
 * Vectorized transformations on Arrow VectorSchemaRoot batches.
 *
 * All operations produce new VectorSchemaRoot instances; callers are
 * responsible for closing both the source and the result when done.
 */
object Transformations {

  // -------------------------------------------------------------------------
  // Sort direction
  // -------------------------------------------------------------------------

  sealed trait SortOrder extends Product with Serializable
  object SortOrder {
    case object Ascending extends SortOrder
    case object Descending extends SortOrder
  }

  final case class SortColumn(name: String, order: SortOrder)

  // -------------------------------------------------------------------------
  // Aggregation types
  // -------------------------------------------------------------------------

  sealed trait AggregateFunction extends Product with Serializable
  object AggregateFunction {
    case object Sum extends AggregateFunction
    case object Count extends AggregateFunction
    case object Min extends AggregateFunction
    case object Max extends AggregateFunction
    case object Avg extends AggregateFunction
  }

  final case class Aggregation(column: String, function: AggregateFunction)

  // -------------------------------------------------------------------------
  // Filter: predicate on row index -> Boolean
  // -------------------------------------------------------------------------

  def filter(
    root: VectorSchemaRoot,
    predicate: Int => Boolean,
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val rowCount = root.getRowCount
    val selectedIndices = buildSelectionVector(rowCount, predicate)
    sliceByIndices(root, selectedIndices, allocator)
  }

  // -------------------------------------------------------------------------
  // Project: keep only named columns
  // -------------------------------------------------------------------------

  def project(
    root: VectorSchemaRoot,
    columnNames: Seq[String],
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val nameSet = columnNames.toSet
    val sourceFields = root.getSchema.getFields.asScala.toSeq
    val selectedFields = sourceFields.filter(f => nameSet.contains(f.getName))

    val newFields = new JArrayList[Field]()
    val newVectors = new JArrayList[FieldVector]()

    selectedFields.foreach { field =>
      val sourceVector = root.getVector(field.getName)
      val tp = sourceVector.getTransferPair(allocator)
      tp.splitAndTransfer(0, root.getRowCount)
      val target = tp.getTo.asInstanceOf[FieldVector]
      newFields.add(target.getField)
      newVectors.add(target)
    }

    val schema = new Schema(newFields)
    val result = new VectorSchemaRoot(schema, newVectors, root.getRowCount)
    result
  }

  // -------------------------------------------------------------------------
  // Sort: sort rows by one or more columns
  // -------------------------------------------------------------------------

  def sort(
    root: VectorSchemaRoot,
    sortColumns: Seq[SortColumn],
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val rowCount = root.getRowCount
    if (rowCount <= 1) {
      return copyRoot(root, allocator)
    }

    val boxedIndices: Array[java.lang.Integer] =
      Array.tabulate(rowCount)(i => java.lang.Integer.valueOf(i))

    val comparators = sortColumns.map { sc =>
      val vec = root.getVector(sc.name)
      val multiplier = sc.order match {
        case SortOrder.Ascending  => 1
        case SortOrder.Descending => -1
      }
      (vec, multiplier)
    }

    java.util.Arrays.sort(boxedIndices, new java.util.Comparator[java.lang.Integer] {
      override def compare(a: java.lang.Integer, b: java.lang.Integer): Int = {
        val ai = a.intValue()
        val bi = b.intValue()
        var i = 0
        while (i < comparators.length) {
          val (vec, mult) = comparators(i)
          val cmp = compareVectorValues(vec, ai, bi)
          if (cmp != 0) return cmp * mult
          i += 1
        }
        0
      }
    })

    val sortedIndices = new Array[Int](rowCount)
    var si = 0
    while (si < rowCount) {
      sortedIndices(si) = boxedIndices(si).intValue()
      si += 1
    }
    sliceByIndices(root, sortedIndices, allocator)
  }

  // -------------------------------------------------------------------------
  // Aggregate: compute aggregate functions, return single-row result
  // -------------------------------------------------------------------------

  def aggregate(
    root: VectorSchemaRoot,
    aggregations: Seq[Aggregation],
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val resultFields = new JArrayList[Field]()
    val resultVectors = new JArrayList[FieldVector]()
    val singleRow = 1

    aggregations.foreach { agg =>
      val sourceVec = root.getVector(agg.column)
      val fieldName = s"${functionName(agg.function)}_${agg.column}"

      agg.function match {
        case AggregateFunction.Count =>
          val vec = new BigIntVector(fieldName, allocator)
          vec.allocateNew(singleRow)
          vec.set(0, countNonNull(sourceVec))
          vec.setValueCount(singleRow)
          resultFields.add(vec.getField)
          resultVectors.add(vec)

        case AggregateFunction.Sum =>
          val vec = new Float8Vector(fieldName, allocator)
          vec.allocateNew(singleRow)
          vec.set(0, sumAsDouble(sourceVec))
          vec.setValueCount(singleRow)
          resultFields.add(vec.getField)
          resultVectors.add(vec)

        case AggregateFunction.Min =>
          val vec = new Float8Vector(fieldName, allocator)
          vec.allocateNew(singleRow)
          val minVal = minAsDouble(sourceVec)
          if (minVal.isDefined) vec.set(0, minVal.get) else vec.setNull(0)
          vec.setValueCount(singleRow)
          resultFields.add(vec.getField)
          resultVectors.add(vec)

        case AggregateFunction.Max =>
          val vec = new Float8Vector(fieldName, allocator)
          vec.allocateNew(singleRow)
          val maxVal = maxAsDouble(sourceVec)
          if (maxVal.isDefined) vec.set(0, maxVal.get) else vec.setNull(0)
          vec.setValueCount(singleRow)
          resultFields.add(vec.getField)
          resultVectors.add(vec)

        case AggregateFunction.Avg =>
          val vec = new Float8Vector(fieldName, allocator)
          vec.allocateNew(singleRow)
          val count = countNonNull(sourceVec)
          if (count > 0) {
            vec.set(0, sumAsDouble(sourceVec) / count.toDouble)
          } else {
            vec.setNull(0)
          }
          vec.setValueCount(singleRow)
          resultFields.add(vec.getField)
          resultVectors.add(vec)
      }
    }

    val schema = new Schema(resultFields)
    new VectorSchemaRoot(schema, resultVectors, singleRow)
  }

  // =========================================================================
  // Internal helpers
  // =========================================================================

  private def functionName(f: AggregateFunction): String = f match {
    case AggregateFunction.Sum   => "sum"
    case AggregateFunction.Count => "count"
    case AggregateFunction.Min   => "min"
    case AggregateFunction.Max   => "max"
    case AggregateFunction.Avg   => "avg"
  }

  /** Build a selection vector (array of selected row indices). */
  private def buildSelectionVector(rowCount: Int, predicate: Int => Boolean): Array[Int] = {
    val builder = Array.newBuilder[Int]
    builder.sizeHint(rowCount)
    var i = 0
    while (i < rowCount) {
      if (predicate(i)) builder += i
      i += 1
    }
    builder.result()
  }

  /**
   * Create a new VectorSchemaRoot by gathering rows at the given indices
   * from the source root. Uses TransferPair for each column, copying
   * only the selected rows.
   */
  private[batch] def sliceByIndices(
    root: VectorSchemaRoot,
    indices: Array[Int],
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val selectedCount = indices.length
    val newFields = new JArrayList[Field]()
    val newVectors = new JArrayList[FieldVector]()

    root.getFieldVectors.asScala.foreach { sourceVec =>
      val targetVec = sourceVec.getField.createVector(allocator)
      targetVec.allocateNew()
      copySelectedRows(sourceVec, targetVec, indices, selectedCount)
      targetVec.setValueCount(selectedCount)
      newFields.add(targetVec.getField)
      newVectors.add(targetVec)
    }

    val schema = new Schema(newFields)
    new VectorSchemaRoot(schema, newVectors, selectedCount)
  }

  /** Copy rows from source to target at specified indices. */
  private def copySelectedRows(
    source: FieldVector,
    target: FieldVector,
    indices: Array[Int],
    count: Int
  ): Unit = {
    var outIdx = 0
    while (outIdx < count) {
      val srcIdx = indices(outIdx)
      copyValue(source, srcIdx, target, outIdx)
      outIdx += 1
    }
  }

  /** Copy a single value from one vector position to another. */
  private def copyValue(
    source: FieldVector,
    srcIdx: Int,
    target: FieldVector,
    destIdx: Int
  ): Unit = {
    if (source.isNull(srcIdx)) {
      setNull(target, destIdx)
    } else {
      (source, target) match {
        case (s: IntVector, t: IntVector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: BigIntVector, t: BigIntVector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: Float4Vector, t: Float4Vector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: Float8Vector, t: Float8Vector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: VarCharVector, t: VarCharVector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: BitVector, t: BitVector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: DateDayVector, t: DateDayVector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: TimeStampMilliVector, t: TimeStampMilliVector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: SmallIntVector, t: SmallIntVector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: TinyIntVector, t: TinyIntVector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: UInt1Vector, t: UInt1Vector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: UInt2Vector, t: UInt2Vector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: UInt4Vector, t: UInt4Vector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: UInt8Vector, t: UInt8Vector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: VarBinaryVector, t: VarBinaryVector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: TimeStampMicroVector, t: TimeStampMicroVector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: TimeStampNanoVector, t: TimeStampNanoVector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: TimeStampSecVector, t: TimeStampSecVector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case (s: DateMilliVector, t: DateMilliVector) =>
          t.setSafe(destIdx, s.get(srcIdx))
        case _ =>
          // Generic fallback using the copyFrom API
          target.copyFrom(srcIdx, destIdx, source)
      }
    }
  }

  private def setNull(vec: FieldVector, idx: Int): Unit = {
    vec match {
      case v: IntVector           => v.setNull(idx)
      case v: BigIntVector        => v.setNull(idx)
      case v: Float4Vector        => v.setNull(idx)
      case v: Float8Vector        => v.setNull(idx)
      case v: VarCharVector       => v.setNull(idx)
      case v: BitVector           => v.setNull(idx)
      case v: DateDayVector       => v.setNull(idx)
      case v: TimeStampMilliVector => v.setNull(idx)
      case v: SmallIntVector      => v.setNull(idx)
      case v: TinyIntVector       => v.setNull(idx)
      case v: UInt1Vector         => v.setNull(idx)
      case v: UInt2Vector         => v.setNull(idx)
      case v: UInt4Vector         => v.setNull(idx)
      case v: UInt8Vector         => v.setNull(idx)
      case v: VarBinaryVector     => v.setNull(idx)
      case v: TimeStampMicroVector => v.setNull(idx)
      case v: TimeStampNanoVector  => v.setNull(idx)
      case v: TimeStampSecVector   => v.setNull(idx)
      case v: DateMilliVector      => v.setNull(idx)
      case v: BaseFixedWidthVector => v.setNull(idx)
      case _ =>
        // For variable-width and complex vectors, we rely on the fact that
        // the vector was freshly allocated (all values default to null)
        ()
    }
  }

  /** Compare two values in the same vector, returning standard comparator int. */
  private def compareVectorValues(vec: FieldVector, a: Int, b: Int): Int = {
    val aNull = vec.isNull(a)
    val bNull = vec.isNull(b)
    if (aNull && bNull) return 0
    if (aNull) return 1  // nulls sort last
    if (bNull) return -1

    vec match {
      case v: IntVector           => java.lang.Integer.compare(v.get(a), v.get(b))
      case v: BigIntVector        => java.lang.Long.compare(v.get(a), v.get(b))
      case v: Float4Vector        => java.lang.Float.compare(v.get(a), v.get(b))
      case v: Float8Vector        => java.lang.Double.compare(v.get(a), v.get(b))
      case v: SmallIntVector      => java.lang.Short.compare(v.get(a), v.get(b))
      case v: TinyIntVector       => java.lang.Byte.compare(v.get(a), v.get(b))
      case v: UInt1Vector         => java.lang.Byte.compareUnsigned(v.get(a), v.get(b))
      case v: UInt2Vector         => java.lang.Character.compare(v.get(a), v.get(b))
      case v: UInt4Vector         => java.lang.Integer.compareUnsigned(v.get(a), v.get(b))
      case v: UInt8Vector         => java.lang.Long.compareUnsigned(v.get(a), v.get(b))
      case v: BitVector           => java.lang.Integer.compare(v.get(a), v.get(b))
      case v: DateDayVector       => java.lang.Integer.compare(v.get(a), v.get(b))
      case v: DateMilliVector     => java.lang.Long.compare(v.get(a), v.get(b))
      case v: TimeStampMilliVector => java.lang.Long.compare(v.get(a), v.get(b))
      case v: TimeStampMicroVector => java.lang.Long.compare(v.get(a), v.get(b))
      case v: TimeStampNanoVector  => java.lang.Long.compare(v.get(a), v.get(b))
      case v: TimeStampSecVector   => java.lang.Long.compare(v.get(a), v.get(b))
      case v: VarCharVector =>
        val aBytes = v.get(a)
        val bBytes = v.get(b)
        new String(aBytes, StandardCharsets.UTF_8).compareTo(new String(bBytes, StandardCharsets.UTF_8))
      case _ => 0
    }
  }

  /** Extract a numeric value as Double for aggregation purposes. */
  private def getNumericValue(vec: FieldVector, idx: Int): Double = vec match {
    case v: IntVector           => v.get(idx).toDouble
    case v: BigIntVector        => v.get(idx).toDouble
    case v: Float4Vector        => v.get(idx).toDouble
    case v: Float8Vector        => v.get(idx)
    case v: SmallIntVector      => v.get(idx).toDouble
    case v: TinyIntVector       => v.get(idx).toDouble
    case v: UInt1Vector         => (v.get(idx) & 0xFF).toDouble
    case v: UInt2Vector         => v.get(idx).toDouble
    case v: UInt4Vector         => (v.get(idx) & 0xFFFFFFFFL).toDouble
    case v: UInt8Vector         => java.lang.Long.toUnsignedString(v.get(idx)).toDouble
    case v: BitVector           => v.get(idx).toDouble
    case v: DateDayVector       => v.get(idx).toDouble
    case v: DateMilliVector     => v.get(idx).toDouble
    case v: TimeStampMilliVector => v.get(idx).toDouble
    case v: TimeStampMicroVector => v.get(idx).toDouble
    case v: TimeStampNanoVector  => v.get(idx).toDouble
    case v: TimeStampSecVector   => v.get(idx).toDouble
    case _ => 0.0
  }

  private def countNonNull(vec: FieldVector): Long = {
    val total = vec.getValueCount
    var count = 0L
    var i = 0
    while (i < total) {
      if (!vec.isNull(i)) count += 1
      i += 1
    }
    count
  }

  private def sumAsDouble(vec: FieldVector): Double = {
    val total = vec.getValueCount
    var sum = 0.0
    var i = 0
    while (i < total) {
      if (!vec.isNull(i)) {
        sum += getNumericValue(vec, i)
      }
      i += 1
    }
    sum
  }

  private def minAsDouble(vec: FieldVector): Option[Double] = {
    val total = vec.getValueCount
    var found = false
    var min = Double.MaxValue
    var i = 0
    while (i < total) {
      if (!vec.isNull(i)) {
        val v = getNumericValue(vec, i)
        if (!found || v < min) {
          min = v
          found = true
        }
      }
      i += 1
    }
    if (found) Some(min) else None
  }

  private def maxAsDouble(vec: FieldVector): Option[Double] = {
    val total = vec.getValueCount
    var found = false
    var max = Double.MinValue
    var i = 0
    while (i < total) {
      if (!vec.isNull(i)) {
        val v = getNumericValue(vec, i)
        if (!found || v > max) {
          max = v
          found = true
        }
      }
      i += 1
    }
    if (found) Some(max) else None
  }

  /** Full copy of a VectorSchemaRoot. */
  private[batch] def copyRoot(
    root: VectorSchemaRoot,
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val newFields = new JArrayList[Field]()
    val newVectors = new JArrayList[FieldVector]()

    root.getFieldVectors.asScala.foreach { sourceVec =>
      val tp = sourceVec.getTransferPair(allocator)
      tp.splitAndTransfer(0, root.getRowCount)
      val target = tp.getTo.asInstanceOf[FieldVector]
      newFields.add(target.getField)
      newVectors.add(target)
    }

    val schema = new Schema(newFields)
    new VectorSchemaRoot(schema, newVectors, root.getRowCount)
  }
}
