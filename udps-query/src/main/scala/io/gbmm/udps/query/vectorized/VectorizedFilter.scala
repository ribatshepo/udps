package io.gbmm.udps.query.vectorized

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.Field

import java.nio.charset.StandardCharsets
import java.util.{ArrayList => JArrayList}
import scala.collection.mutable
import scala.util.matching.Regex

/** Vectorized filter expressions evaluated on Arrow VectorSchemaRoot batches. */
sealed trait FilterExpr extends Product with Serializable

object FilterExpr {

  /** Column reference by name. */
  final case class ColumnRef(name: String) extends FilterExpr

  /** Literal values for filter comparisons. */
  sealed trait LiteralValue extends FilterExpr
  final case class IntLiteral(value: Int) extends LiteralValue
  final case class LongLiteral(value: Long) extends LiteralValue
  final case class FloatLiteral(value: Float) extends LiteralValue
  final case class DoubleLiteral(value: Double) extends LiteralValue
  final case class StringLiteral(value: String) extends LiteralValue
  final case class BooleanLiteral(value: Boolean) extends LiteralValue

  /** Comparison operators. */
  sealed trait CompOp extends Product with Serializable
  object CompOp {
    case object Eq  extends CompOp
    case object Neq extends CompOp
    case object Lt  extends CompOp
    case object Gt  extends CompOp
    case object Lte extends CompOp
    case object Gte extends CompOp
  }

  /** Comparison: lhs <op> rhs. */
  final case class Comparison(lhs: FilterExpr, op: CompOp, rhs: FilterExpr) extends FilterExpr

  /** Logical combinators. */
  final case class And(left: FilterExpr, right: FilterExpr) extends FilterExpr
  final case class Or(left: FilterExpr, right: FilterExpr) extends FilterExpr
  final case class Not(child: FilterExpr) extends FilterExpr

  /** Null checks. */
  final case class IsNull(column: String) extends FilterExpr
  final case class IsNotNull(column: String) extends FilterExpr

  /** Set membership: column IN (values). */
  final case class In(column: String, values: Seq[FilterExpr]) extends FilterExpr

  /** Range: column BETWEEN low AND high. */
  final case class Between(column: String, low: FilterExpr, high: FilterExpr) extends FilterExpr

  /** Pattern matching: column LIKE pattern. Uses SQL LIKE syntax (% and _). */
  final case class Like(column: String, pattern: String) extends FilterExpr
}

/**
 * Evaluates filter expressions on an Arrow VectorSchemaRoot in a vectorized fashion.
 *
 * Produces a boolean bitmap over the batch, then copies only matching rows
 * into a new VectorSchemaRoot using TransferPair / value copy.
 *
 * Thread-safety: instances are stateless; all mutable state is confined to
 * method-local variables. Multiple threads may call `apply` concurrently on
 * different batches with separate allocators.
 */
object VectorizedFilter {

  /**
   * Filter a batch, returning a new VectorSchemaRoot containing only the rows
   * that satisfy `filter`. The caller owns the returned root and MUST close it
   * when done. The input `root` is NOT closed by this method.
   */
  def apply(
    root: VectorSchemaRoot,
    filter: FilterExpr,
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val rowCount  = root.getRowCount
    val selection = evaluateBitmap(root, filter, rowCount)
    buildFilteredBatch(root, selection, allocator)
  }

  // ---------------------------------------------------------------------------
  // Bitmap evaluation
  // ---------------------------------------------------------------------------

  private def evaluateBitmap(
    root: VectorSchemaRoot,
    expr: FilterExpr,
    rowCount: Int
  ): Array[Boolean] = expr match {

    case FilterExpr.Comparison(lhs, op, rhs) =>
      evaluateComparison(root, lhs, op, rhs, rowCount)

    case FilterExpr.And(left, right) =>
      val l = evaluateBitmap(root, left, rowCount)
      val r = evaluateBitmap(root, right, rowCount)
      combineBitmaps(l, r, rowCount, _ && _)

    case FilterExpr.Or(left, right) =>
      val l = evaluateBitmap(root, left, rowCount)
      val r = evaluateBitmap(root, right, rowCount)
      combineBitmaps(l, r, rowCount, _ || _)

    case FilterExpr.Not(child) =>
      val c = evaluateBitmap(root, child, rowCount)
      negateBitmap(c, rowCount)

    case FilterExpr.IsNull(column) =>
      evalNullCheck(root, column, rowCount, expectNull = true)

    case FilterExpr.IsNotNull(column) =>
      evalNullCheck(root, column, rowCount, expectNull = false)

    case FilterExpr.In(column, values) =>
      evaluateIn(root, column, values, rowCount)

    case FilterExpr.Between(column, low, high) =>
      evaluateBetween(root, column, low, high, rowCount)

    case FilterExpr.Like(column, pattern) =>
      evaluateLike(root, column, pattern, rowCount)

    case _ =>
      throw new UnsupportedOperationException(
        s"Expression type ${expr.getClass.getSimpleName} cannot be used as a top-level filter"
      )
  }

  // ---------------------------------------------------------------------------
  // Comparison
  // ---------------------------------------------------------------------------

  private def evaluateComparison(
    root: VectorSchemaRoot,
    lhs: FilterExpr,
    op: FilterExpr.CompOp,
    rhs: FilterExpr,
    rowCount: Int
  ): Array[Boolean] = {
    val bitmap = new Array[Boolean](rowCount)
    val leftVals  = resolveValues(root, lhs, rowCount)
    val rightVals = resolveValues(root, rhs, rowCount)

    var i = 0
    while (i < rowCount) {
      bitmap(i) = (leftVals(i), rightVals(i)) match {
        case (null, _) | (_, null) => false
        case (l, r)                => compareValues(l, r, op)
      }
      i += 1
    }
    bitmap
  }

  private def compareValues(l: Any, r: Any, op: FilterExpr.CompOp): Boolean = {
    val cmp = compareAny(l, r)
    op match {
      case FilterExpr.CompOp.Eq  => cmp == 0
      case FilterExpr.CompOp.Neq => cmp != 0
      case FilterExpr.CompOp.Lt  => cmp < 0
      case FilterExpr.CompOp.Gt  => cmp > 0
      case FilterExpr.CompOp.Lte => cmp <= 0
      case FilterExpr.CompOp.Gte => cmp >= 0
    }
  }

  private def compareAny(a: Any, b: Any): Int = (a, b) match {
    case (x: Int, y: Int)       => Integer.compare(x, y)
    case (x: Long, y: Long)     => java.lang.Long.compare(x, y)
    case (x: Float, y: Float)   => java.lang.Float.compare(x, y)
    case (x: Double, y: Double) => java.lang.Double.compare(x, y)
    case (x: String, y: String) => x.compareTo(y)
    case (x: Number, y: Number) => java.lang.Double.compare(x.doubleValue(), y.doubleValue())
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot compare ${a.getClass.getSimpleName} with ${b.getClass.getSimpleName}"
      )
  }

  /**
   * Resolve an expression to an array of boxed values (one per row).
   * For column refs, reads from the vector; for literals, fills a constant array.
   */
  private def resolveValues(
    root: VectorSchemaRoot,
    expr: FilterExpr,
    rowCount: Int
  ): Array[Any] = expr match {

    case FilterExpr.ColumnRef(name) =>
      val vector = root.getVector(name)
      if (vector == null)
        throw new IllegalArgumentException(s"Column '$name' not found in batch")
      readVectorValues(vector, rowCount)

    case FilterExpr.IntLiteral(v)     => Array.fill[Any](rowCount)(v)
    case FilterExpr.LongLiteral(v)    => Array.fill[Any](rowCount)(v)
    case FilterExpr.FloatLiteral(v)   => Array.fill[Any](rowCount)(v)
    case FilterExpr.DoubleLiteral(v)  => Array.fill[Any](rowCount)(v)
    case FilterExpr.StringLiteral(v)  => Array.fill[Any](rowCount)(v)
    case FilterExpr.BooleanLiteral(v) => Array.fill[Any](rowCount)(if (v) 1 else 0)

    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot resolve ${expr.getClass.getSimpleName} to row values"
      )
  }

  /** Read all values from an Arrow vector into a boxed array (null-aware). */
  private[vectorized] def readVectorValues(vector: FieldVector, rowCount: Int): Array[Any] = {
    val out = new Array[Any](rowCount)
    var i = 0
    while (i < rowCount) {
      if (vector.isNull(i)) {
        out(i) = null
      } else {
        out(i) = readSingleValue(vector, i)
      }
      i += 1
    }
    out
  }

  private[vectorized] def readSingleValue(vector: FieldVector, index: Int): Any = vector match {
    case v: IntVector      => v.get(index)
    case v: BigIntVector   => v.get(index)
    case v: Float4Vector   => v.get(index)
    case v: Float8Vector   => v.get(index)
    case v: VarCharVector  => new String(v.get(index), StandardCharsets.UTF_8)
    case v: BitVector      => v.get(index)
    case v: SmallIntVector => v.get(index).toInt
    case v: TinyIntVector  => v.get(index).toInt
    case _ =>
      throw new UnsupportedOperationException(
        s"Unsupported Arrow vector type: ${vector.getClass.getSimpleName}"
      )
  }

  // ---------------------------------------------------------------------------
  // Null checks
  // ---------------------------------------------------------------------------

  private def evalNullCheck(
    root: VectorSchemaRoot,
    column: String,
    rowCount: Int,
    expectNull: Boolean
  ): Array[Boolean] = {
    val vector = root.getVector(column)
    if (vector == null)
      throw new IllegalArgumentException(s"Column '$column' not found in batch")
    val bitmap = new Array[Boolean](rowCount)
    var i = 0
    while (i < rowCount) {
      bitmap(i) = if (expectNull) vector.isNull(i) else !vector.isNull(i)
      i += 1
    }
    bitmap
  }

  // ---------------------------------------------------------------------------
  // IN
  // ---------------------------------------------------------------------------

  private def evaluateIn(
    root: VectorSchemaRoot,
    column: String,
    values: Seq[FilterExpr],
    rowCount: Int
  ): Array[Boolean] = {
    val vector = root.getVector(column)
    if (vector == null)
      throw new IllegalArgumentException(s"Column '$column' not found in batch")

    val literalSet: Set[Any] = values.map(literalToValue).toSet
    val bitmap = new Array[Boolean](rowCount)
    var i = 0
    while (i < rowCount) {
      if (vector.isNull(i)) {
        bitmap(i) = false
      } else {
        bitmap(i) = literalSet.contains(readSingleValue(vector, i))
      }
      i += 1
    }
    bitmap
  }

  private def literalToValue(expr: FilterExpr): Any = expr match {
    case FilterExpr.IntLiteral(v)     => v
    case FilterExpr.LongLiteral(v)    => v
    case FilterExpr.FloatLiteral(v)   => v
    case FilterExpr.DoubleLiteral(v)  => v
    case FilterExpr.StringLiteral(v)  => v
    case FilterExpr.BooleanLiteral(v) => if (v) 1 else 0
    case _ =>
      throw new IllegalArgumentException(
        s"IN list elements must be literals, got ${expr.getClass.getSimpleName}"
      )
  }

  // ---------------------------------------------------------------------------
  // BETWEEN
  // ---------------------------------------------------------------------------

  private def evaluateBetween(
    root: VectorSchemaRoot,
    column: String,
    low: FilterExpr,
    high: FilterExpr,
    rowCount: Int
  ): Array[Boolean] = {
    val gteFilter = FilterExpr.Comparison(FilterExpr.ColumnRef(column), FilterExpr.CompOp.Gte, low)
    val lteFilter = FilterExpr.Comparison(FilterExpr.ColumnRef(column), FilterExpr.CompOp.Lte, high)
    val l = evaluateBitmap(root, gteFilter, rowCount)
    val r = evaluateBitmap(root, lteFilter, rowCount)
    combineBitmaps(l, r, rowCount, _ && _)
  }

  // ---------------------------------------------------------------------------
  // LIKE
  // ---------------------------------------------------------------------------

  private val LIKE_CACHE_MAX_SIZE = 256

  @volatile private var likePatternCache: Map[String, Regex] = Map.empty

  private def sqlLikeToRegex(pattern: String): Regex = {
    likePatternCache.get(pattern) match {
      case Some(r) => r
      case None =>
        val sb = new java.lang.StringBuilder("^")
        var i = 0
        while (i < pattern.length) {
          val ch = pattern.charAt(i)
          if (ch == '%') {
            sb.append(".*")
          } else if (ch == '_') {
            sb.append('.')
          } else if ("\\[](){}^$.|*+?".indexOf(ch.toInt) >= 0) {
            sb.append('\\').append(ch)
          } else {
            sb.append(ch)
          }
          i += 1
        }
        sb.append('$')
        val regex = sb.toString.r
        if (likePatternCache.size < LIKE_CACHE_MAX_SIZE) {
          likePatternCache = likePatternCache + (pattern -> regex)
        }
        regex
    }
  }

  private def evaluateLike(
    root: VectorSchemaRoot,
    column: String,
    pattern: String,
    rowCount: Int
  ): Array[Boolean] = {
    val vector = root.getVector(column)
    if (vector == null)
      throw new IllegalArgumentException(s"Column '$column' not found in batch")

    vector match {
      case vc: VarCharVector =>
        val regex  = sqlLikeToRegex(pattern)
        val bitmap = new Array[Boolean](rowCount)
        var i = 0
        while (i < rowCount) {
          if (vc.isNull(i)) {
            bitmap(i) = false
          } else {
            val str = new String(vc.get(i), StandardCharsets.UTF_8)
            bitmap(i) = regex.pattern.matcher(str).matches()
          }
          i += 1
        }
        bitmap
      case _ =>
        throw new UnsupportedOperationException(
          s"LIKE is only supported on VarCharVector, got ${vector.getClass.getSimpleName}"
        )
    }
  }

  // ---------------------------------------------------------------------------
  // Bitmap utilities
  // ---------------------------------------------------------------------------

  private def combineBitmaps(
    l: Array[Boolean],
    r: Array[Boolean],
    rowCount: Int,
    f: (Boolean, Boolean) => Boolean
  ): Array[Boolean] = {
    val out = new Array[Boolean](rowCount)
    var i = 0
    while (i < rowCount) {
      out(i) = f(l(i), r(i))
      i += 1
    }
    out
  }

  private def negateBitmap(b: Array[Boolean], rowCount: Int): Array[Boolean] = {
    val out = new Array[Boolean](rowCount)
    var i = 0
    while (i < rowCount) {
      out(i) = !b(i)
      i += 1
    }
    out
  }

  // ---------------------------------------------------------------------------
  // Build filtered output batch
  // ---------------------------------------------------------------------------

  private def buildFilteredBatch(
    root: VectorSchemaRoot,
    selection: Array[Boolean],
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val selectedIndices = collectSelectedIndices(selection)
    val selectedCount   = selectedIndices.length
    val fieldCount      = root.getFieldVectors.size()

    val newVectors = new JArrayList[FieldVector](fieldCount)
    var fi = 0
    while (fi < fieldCount) {
      val srcVector = root.getFieldVectors.get(fi)
      val dstVector = createVector(srcVector.getField, allocator)
      dstVector.allocateNew()
      copySelectedRows(srcVector, dstVector, selectedIndices, selectedCount)
      dstVector.setValueCount(selectedCount)
      newVectors.add(dstVector)
      fi += 1
    }

    val schema  = root.getSchema
    val newRoot = new VectorSchemaRoot(schema, newVectors, selectedCount)
    newRoot
  }

  private def collectSelectedIndices(selection: Array[Boolean]): Array[Int] = {
    val buf = new mutable.ArrayBuilder.ofInt
    buf.sizeHint(selection.length)
    var i = 0
    while (i < selection.length) {
      if (selection(i)) buf += i
      i += 1
    }
    buf.result()
  }

  private def createVector(field: Field, allocator: BufferAllocator): FieldVector =
    field.createVector(allocator)

  private def copySelectedRows(
    src: FieldVector,
    dst: FieldVector,
    indices: Array[Int],
    count: Int
  ): Unit = {
    var dstIdx = 0
    while (dstIdx < count) {
      val srcIdx = indices(dstIdx)
      if (src.isNull(srcIdx)) {
        setNull(dst, dstIdx)
      } else {
        copyValue(src, srcIdx, dst, dstIdx)
      }
      dstIdx += 1
    }
  }

  private def setNull(dst: FieldVector, index: Int): Unit = dst match {
    case v: IntVector     => v.setNull(index)
    case v: BigIntVector  => v.setNull(index)
    case v: Float4Vector  => v.setNull(index)
    case v: Float8Vector  => v.setNull(index)
    case v: VarCharVector => v.setNull(index)
    case v: BitVector     => v.setNull(index)
    case v: SmallIntVector => v.setNull(index)
    case v: TinyIntVector  => v.setNull(index)
    case _ =>
      throw new UnsupportedOperationException(
        s"setNull not supported for ${dst.getClass.getSimpleName}"
      )
  }

  private def copyValue(src: FieldVector, srcIdx: Int, dst: FieldVector, dstIdx: Int): Unit =
    (src, dst) match {
      case (s: IntVector, d: IntVector)         => d.setSafe(dstIdx, s.get(srcIdx))
      case (s: BigIntVector, d: BigIntVector)   => d.setSafe(dstIdx, s.get(srcIdx))
      case (s: Float4Vector, d: Float4Vector)   => d.setSafe(dstIdx, s.get(srcIdx))
      case (s: Float8Vector, d: Float8Vector)   => d.setSafe(dstIdx, s.get(srcIdx))
      case (s: VarCharVector, d: VarCharVector) => d.setSafe(dstIdx, s.get(srcIdx))
      case (s: BitVector, d: BitVector)         => d.setSafe(dstIdx, s.get(srcIdx))
      case (s: SmallIntVector, d: SmallIntVector) => d.setSafe(dstIdx, s.get(srcIdx))
      case (s: TinyIntVector, d: TinyIntVector)   => d.setSafe(dstIdx, s.get(srcIdx))
      case _ =>
        throw new UnsupportedOperationException(
          s"copyValue not supported for ${src.getClass.getSimpleName} -> ${dst.getClass.getSimpleName}"
        )
    }
}
