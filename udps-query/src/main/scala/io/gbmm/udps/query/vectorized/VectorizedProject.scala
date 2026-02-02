package io.gbmm.udps.query.vectorized

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import java.nio.charset.StandardCharsets
import java.util.{ArrayList => JArrayList}

/** Projection expressions for vectorized evaluation. */
sealed trait ProjectExpr extends Product with Serializable

object ProjectExpr {

  /** Reference an existing column by name. */
  final case class ColumnRef(name: String) extends ProjectExpr

  /** Arithmetic on two numeric expressions. */
  sealed trait ArithOp extends Product with Serializable
  object ArithOp {
    case object Add extends ArithOp
    case object Sub extends ArithOp
    case object Mul extends ArithOp
    case object Div extends ArithOp
  }

  final case class Arithmetic(left: ProjectExpr, op: ArithOp, right: ProjectExpr) extends ProjectExpr

  /** String functions. */
  sealed trait StringFn extends Product with Serializable
  object StringFn {
    case object Upper     extends StringFn
    case object Lower     extends StringFn
    /** SUBSTRING(expr, start, length) - 1-based start position. */
    final case class Substring(start: Int, length: Int) extends StringFn
  }

  final case class StringFunc(fn: StringFn, operand: ProjectExpr) extends ProjectExpr

  /** CONCAT(exprs...) - concatenate multiple string expressions. */
  final case class Concat(operands: Seq[ProjectExpr]) extends ProjectExpr

  /** CAST(expr AS targetType). */
  final case class Cast(operand: ProjectExpr, targetType: CastTarget) extends ProjectExpr

  /** Supported cast targets. */
  sealed trait CastTarget extends Product with Serializable
  object CastTarget {
    case object ToInt     extends CastTarget
    case object ToLong    extends CastTarget
    case object ToFloat   extends CastTarget
    case object ToDouble  extends CastTarget
    case object ToString  extends CastTarget
  }

  /** Literal values broadcast to every row. */
  sealed trait LiteralExpr extends ProjectExpr
  final case class IntLiteral(value: Int) extends LiteralExpr
  final case class LongLiteral(value: Long) extends LiteralExpr
  final case class FloatLiteral(value: Float) extends LiteralExpr
  final case class DoubleLiteral(value: Double) extends LiteralExpr
  final case class StringLiteral(value: String) extends LiteralExpr
  final case class BoolLiteral(value: Boolean) extends LiteralExpr
}

/**
 * A projection specification: a named output column paired with its expression.
 */
final case class Projection(alias: String, expr: ProjectExpr)

/**
 * Vectorized projection evaluator on Arrow VectorSchemaRoot.
 *
 * Evaluates a list of [[Projection]]s against an input batch and produces a new
 * VectorSchemaRoot containing the projected columns. The caller is responsible
 * for closing the returned root.
 *
 * Thread-safety: stateless; safe for concurrent use.
 */
object VectorizedProject {

  /**
   * Evaluate projections against `root` and return a new VectorSchemaRoot.
   * The input `root` is NOT closed by this method.
   */
  def apply(
    root: VectorSchemaRoot,
    projections: Seq[Projection],
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val rowCount = root.getRowCount
    val fields   = new JArrayList[Field](projections.size)
    val vectors  = new JArrayList[FieldVector](projections.size)

    projections.foreach { proj =>
      val vec = evaluateExpr(root, proj.expr, proj.alias, rowCount, allocator)
      vec.setValueCount(rowCount)
      fields.add(vec.getField)
      vectors.add(vec)
    }

    val schema = new Schema(fields)
    new VectorSchemaRoot(schema, vectors, rowCount)
  }

  // ---------------------------------------------------------------------------
  // Expression evaluator (returns a new FieldVector owned by the allocator)
  // ---------------------------------------------------------------------------

  private def evaluateExpr(
    root: VectorSchemaRoot,
    expr: ProjectExpr,
    alias: String,
    rowCount: Int,
    allocator: BufferAllocator
  ): FieldVector = expr match {

    case ProjectExpr.ColumnRef(name) =>
      val src = root.getVector(name)
      if (src == null)
        throw new IllegalArgumentException(s"Column '$name' not found in batch")
      copyVector(src, alias, rowCount, allocator)

    case ProjectExpr.Arithmetic(left, op, right) =>
      evaluateArithmetic(root, left, op, right, alias, rowCount, allocator)

    case ProjectExpr.StringFunc(fn, operand) =>
      evaluateStringFunc(root, fn, operand, alias, rowCount, allocator)

    case ProjectExpr.Concat(operands) =>
      evaluateConcat(root, operands, alias, rowCount, allocator)

    case ProjectExpr.Cast(operand, target) =>
      evaluateCast(root, operand, target, alias, rowCount, allocator)

    case lit: ProjectExpr.LiteralExpr =>
      buildLiteralVector(lit, alias, rowCount, allocator)
  }

  // ---------------------------------------------------------------------------
  // Column copy (rename)
  // ---------------------------------------------------------------------------

  private def copyVector(
    src: FieldVector,
    alias: String,
    rowCount: Int,
    allocator: BufferAllocator
  ): FieldVector = src match {
    case s: IntVector =>
      val d = newIntVector(alias, allocator, rowCount)
      var i = 0
      while (i < rowCount) {
        if (s.isNull(i)) d.setNull(i) else d.setSafe(i, s.get(i))
        i += 1
      }
      d

    case s: BigIntVector =>
      val d = newBigIntVector(alias, allocator, rowCount)
      var i = 0
      while (i < rowCount) {
        if (s.isNull(i)) d.setNull(i) else d.setSafe(i, s.get(i))
        i += 1
      }
      d

    case s: Float4Vector =>
      val d = newFloat4Vector(alias, allocator, rowCount)
      var i = 0
      while (i < rowCount) {
        if (s.isNull(i)) d.setNull(i) else d.setSafe(i, s.get(i))
        i += 1
      }
      d

    case s: Float8Vector =>
      val d = newFloat8Vector(alias, allocator, rowCount)
      var i = 0
      while (i < rowCount) {
        if (s.isNull(i)) d.setNull(i) else d.setSafe(i, s.get(i))
        i += 1
      }
      d

    case s: VarCharVector =>
      val d = newVarCharVector(alias, allocator, rowCount)
      var i = 0
      while (i < rowCount) {
        if (s.isNull(i)) d.setNull(i) else d.setSafe(i, s.get(i))
        i += 1
      }
      d

    case s: BitVector =>
      val d = newBitVector(alias, allocator, rowCount)
      var i = 0
      while (i < rowCount) {
        if (s.isNull(i)) d.setNull(i) else d.setSafe(i, s.get(i))
        i += 1
      }
      d

    case _ =>
      throw new UnsupportedOperationException(
        s"Column copy not supported for ${src.getClass.getSimpleName}"
      )
  }

  // ---------------------------------------------------------------------------
  // Arithmetic
  // ---------------------------------------------------------------------------

  private def evaluateArithmetic(
    root: VectorSchemaRoot,
    left: ProjectExpr,
    op: ProjectExpr.ArithOp,
    right: ProjectExpr,
    alias: String,
    rowCount: Int,
    allocator: BufferAllocator
  ): FieldVector = {
    val lVec = evaluateExpr(root, left, "__arith_l", rowCount, allocator)
    val rVec = evaluateExpr(root, right, "__arith_r", rowCount, allocator)
    try {
      val result = computeArithmetic(lVec, rVec, op, alias, rowCount, allocator)
      result
    } finally {
      lVec.close()
      rVec.close()
    }
  }

  private def computeArithmetic(
    lVec: FieldVector,
    rVec: FieldVector,
    op: ProjectExpr.ArithOp,
    alias: String,
    rowCount: Int,
    allocator: BufferAllocator
  ): FieldVector = {
    if (isFloatingPoint(lVec) || isFloatingPoint(rVec)) {
      computeDoubleArithmetic(lVec, rVec, op, alias, rowCount, allocator)
    } else if (isLongType(lVec) || isLongType(rVec)) {
      computeLongArithmetic(lVec, rVec, op, alias, rowCount, allocator)
    } else {
      computeIntArithmetic(lVec, rVec, op, alias, rowCount, allocator)
    }
  }

  private def isFloatingPoint(v: FieldVector): Boolean = v match {
    case _: Float4Vector | _: Float8Vector => true
    case _ => false
  }

  private def isLongType(v: FieldVector): Boolean = v match {
    case _: BigIntVector => true
    case _ => false
  }

  private def computeIntArithmetic(
    lVec: FieldVector,
    rVec: FieldVector,
    op: ProjectExpr.ArithOp,
    alias: String,
    rowCount: Int,
    allocator: BufferAllocator
  ): IntVector = {
    val result = newIntVector(alias, allocator, rowCount)
    var i = 0
    while (i < rowCount) {
      if (lVec.isNull(i) || rVec.isNull(i)) {
        result.setNull(i)
      } else {
        val l = readAsInt(lVec, i)
        val r = readAsInt(rVec, i)
        op match {
          case ProjectExpr.ArithOp.Add => result.setSafe(i, l + r)
          case ProjectExpr.ArithOp.Sub => result.setSafe(i, l - r)
          case ProjectExpr.ArithOp.Mul => result.setSafe(i, l * r)
          case ProjectExpr.ArithOp.Div =>
            if (r == 0) result.setNull(i) else result.setSafe(i, l / r)
        }
      }
      i += 1
    }
    result
  }

  private def computeLongArithmetic(
    lVec: FieldVector,
    rVec: FieldVector,
    op: ProjectExpr.ArithOp,
    alias: String,
    rowCount: Int,
    allocator: BufferAllocator
  ): BigIntVector = {
    val result = newBigIntVector(alias, allocator, rowCount)
    var i = 0
    while (i < rowCount) {
      if (lVec.isNull(i) || rVec.isNull(i)) {
        result.setNull(i)
      } else {
        val l = readAsLong(lVec, i)
        val r = readAsLong(rVec, i)
        op match {
          case ProjectExpr.ArithOp.Add => result.setSafe(i, l + r)
          case ProjectExpr.ArithOp.Sub => result.setSafe(i, l - r)
          case ProjectExpr.ArithOp.Mul => result.setSafe(i, l * r)
          case ProjectExpr.ArithOp.Div =>
            if (r == 0L) result.setNull(i) else result.setSafe(i, l / r)
        }
      }
      i += 1
    }
    result
  }

  private def computeDoubleArithmetic(
    lVec: FieldVector,
    rVec: FieldVector,
    op: ProjectExpr.ArithOp,
    alias: String,
    rowCount: Int,
    allocator: BufferAllocator
  ): Float8Vector = {
    val result = newFloat8Vector(alias, allocator, rowCount)
    var i = 0
    while (i < rowCount) {
      if (lVec.isNull(i) || rVec.isNull(i)) {
        result.setNull(i)
      } else {
        val l = readAsDouble(lVec, i)
        val r = readAsDouble(rVec, i)
        op match {
          case ProjectExpr.ArithOp.Add => result.setSafe(i, l + r)
          case ProjectExpr.ArithOp.Sub => result.setSafe(i, l - r)
          case ProjectExpr.ArithOp.Mul => result.setSafe(i, l * r)
          case ProjectExpr.ArithOp.Div =>
            if (r == 0.0) result.setNull(i) else result.setSafe(i, l / r)
        }
      }
      i += 1
    }
    result
  }

  private def readAsInt(v: FieldVector, i: Int): Int = v match {
    case iv: IntVector      => iv.get(i)
    case sv: SmallIntVector => sv.get(i).toInt
    case tv: TinyIntVector  => tv.get(i).toInt
    case bv: BitVector      => bv.get(i)
    case _ => readAsLong(v, i).toInt
  }

  private def readAsLong(v: FieldVector, i: Int): Long = v match {
    case bv: BigIntVector   => bv.get(i)
    case iv: IntVector      => iv.get(i).toLong
    case sv: SmallIntVector => sv.get(i).toLong
    case tv: TinyIntVector  => tv.get(i).toLong
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot read ${v.getClass.getSimpleName} as Long"
      )
  }

  private def readAsDouble(v: FieldVector, i: Int): Double = v match {
    case fv: Float8Vector   => fv.get(i)
    case fv: Float4Vector   => fv.get(i).toDouble
    case bv: BigIntVector   => bv.get(i).toDouble
    case iv: IntVector      => iv.get(i).toDouble
    case sv: SmallIntVector => sv.get(i).toDouble
    case tv: TinyIntVector  => tv.get(i).toDouble
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot read ${v.getClass.getSimpleName} as Double"
      )
  }

  // ---------------------------------------------------------------------------
  // String functions
  // ---------------------------------------------------------------------------

  private def evaluateStringFunc(
    root: VectorSchemaRoot,
    fn: ProjectExpr.StringFn,
    operand: ProjectExpr,
    alias: String,
    rowCount: Int,
    allocator: BufferAllocator
  ): VarCharVector = {
    val srcVec = evaluateExpr(root, operand, "__str_src", rowCount, allocator)
    try {
      val result = newVarCharVector(alias, allocator, rowCount)
      var i = 0
      while (i < rowCount) {
        if (srcVec.isNull(i)) {
          result.setNull(i)
        } else {
          val str = readAsString(srcVec, i)
          val transformed = fn match {
            case ProjectExpr.StringFn.Upper              => str.toUpperCase
            case ProjectExpr.StringFn.Lower              => str.toLowerCase
            case ProjectExpr.StringFn.Substring(start, length) =>
              val zeroStart = math.max(start - 1, 0)
              val end       = math.min(zeroStart + length, str.length)
              if (zeroStart >= str.length) "" else str.substring(zeroStart, end)
          }
          result.setSafe(i, transformed.getBytes(StandardCharsets.UTF_8))
        }
        i += 1
      }
      result
    } finally {
      srcVec.close()
    }
  }

  private def readAsString(v: FieldVector, i: Int): String = v match {
    case vc: VarCharVector  => new String(vc.get(i), StandardCharsets.UTF_8)
    case iv: IntVector      => iv.get(i).toString
    case bv: BigIntVector   => bv.get(i).toString
    case fv: Float4Vector   => fv.get(i).toString
    case dv: Float8Vector   => dv.get(i).toString
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot read ${v.getClass.getSimpleName} as String"
      )
  }

  // ---------------------------------------------------------------------------
  // CONCAT
  // ---------------------------------------------------------------------------

  private def evaluateConcat(
    root: VectorSchemaRoot,
    operands: Seq[ProjectExpr],
    alias: String,
    rowCount: Int,
    allocator: BufferAllocator
  ): VarCharVector = {
    val evalVecs = operands.zipWithIndex.map { case (op, idx) =>
      evaluateExpr(root, op, s"__concat_$idx", rowCount, allocator)
    }
    try {
      val result = newVarCharVector(alias, allocator, rowCount)
      var i = 0
      while (i < rowCount) {
        val anyNull = evalVecs.exists(_.isNull(i))
        if (anyNull) {
          result.setNull(i)
        } else {
          val sb = new java.lang.StringBuilder()
          evalVecs.foreach { v => sb.append(readAsString(v, i)) }
          result.setSafe(i, sb.toString.getBytes(StandardCharsets.UTF_8))
        }
        i += 1
      }
      result
    } finally {
      evalVecs.foreach(_.close())
    }
  }

  // ---------------------------------------------------------------------------
  // CAST
  // ---------------------------------------------------------------------------

  private def evaluateCast(
    root: VectorSchemaRoot,
    operand: ProjectExpr,
    target: ProjectExpr.CastTarget,
    alias: String,
    rowCount: Int,
    allocator: BufferAllocator
  ): FieldVector = {
    val srcVec = evaluateExpr(root, operand, "__cast_src", rowCount, allocator)
    try {
      target match {
        case ProjectExpr.CastTarget.ToInt    => castToInt(srcVec, alias, rowCount, allocator)
        case ProjectExpr.CastTarget.ToLong   => castToLong(srcVec, alias, rowCount, allocator)
        case ProjectExpr.CastTarget.ToFloat  => castToFloat(srcVec, alias, rowCount, allocator)
        case ProjectExpr.CastTarget.ToDouble => castToDouble(srcVec, alias, rowCount, allocator)
        case ProjectExpr.CastTarget.ToString => castToString(srcVec, alias, rowCount, allocator)
      }
    } finally {
      srcVec.close()
    }
  }

  private def castToInt(src: FieldVector, alias: String, rowCount: Int, alloc: BufferAllocator): IntVector = {
    val result = newIntVector(alias, alloc, rowCount)
    var i = 0
    while (i < rowCount) {
      if (src.isNull(i)) { result.setNull(i) }
      else {
        src match {
          case v: IntVector      => result.setSafe(i, v.get(i))
          case v: BigIntVector   => result.setSafe(i, v.get(i).toInt)
          case v: Float4Vector   => result.setSafe(i, v.get(i).toInt)
          case v: Float8Vector   => result.setSafe(i, v.get(i).toInt)
          case v: VarCharVector  => result.setSafe(i, new String(v.get(i), StandardCharsets.UTF_8).trim.toInt)
          case v: SmallIntVector => result.setSafe(i, v.get(i).toInt)
          case v: TinyIntVector  => result.setSafe(i, v.get(i).toInt)
          case _ => throw new UnsupportedOperationException(s"Cast to Int not supported from ${src.getClass.getSimpleName}")
        }
      }
      i += 1
    }
    result
  }

  private def castToLong(src: FieldVector, alias: String, rowCount: Int, alloc: BufferAllocator): BigIntVector = {
    val result = newBigIntVector(alias, alloc, rowCount)
    var i = 0
    while (i < rowCount) {
      if (src.isNull(i)) { result.setNull(i) }
      else {
        src match {
          case v: IntVector      => result.setSafe(i, v.get(i).toLong)
          case v: BigIntVector   => result.setSafe(i, v.get(i))
          case v: Float4Vector   => result.setSafe(i, v.get(i).toLong)
          case v: Float8Vector   => result.setSafe(i, v.get(i).toLong)
          case v: VarCharVector  => result.setSafe(i, new String(v.get(i), StandardCharsets.UTF_8).trim.toLong)
          case v: SmallIntVector => result.setSafe(i, v.get(i).toLong)
          case v: TinyIntVector  => result.setSafe(i, v.get(i).toLong)
          case _ => throw new UnsupportedOperationException(s"Cast to Long not supported from ${src.getClass.getSimpleName}")
        }
      }
      i += 1
    }
    result
  }

  private def castToFloat(src: FieldVector, alias: String, rowCount: Int, alloc: BufferAllocator): Float4Vector = {
    val result = newFloat4Vector(alias, alloc, rowCount)
    var i = 0
    while (i < rowCount) {
      if (src.isNull(i)) { result.setNull(i) }
      else {
        src match {
          case v: IntVector      => result.setSafe(i, v.get(i).toFloat)
          case v: BigIntVector   => result.setSafe(i, v.get(i).toFloat)
          case v: Float4Vector   => result.setSafe(i, v.get(i))
          case v: Float8Vector   => result.setSafe(i, v.get(i).toFloat)
          case v: VarCharVector  => result.setSafe(i, new String(v.get(i), StandardCharsets.UTF_8).trim.toFloat)
          case v: SmallIntVector => result.setSafe(i, v.get(i).toFloat)
          case v: TinyIntVector  => result.setSafe(i, v.get(i).toFloat)
          case _ => throw new UnsupportedOperationException(s"Cast to Float not supported from ${src.getClass.getSimpleName}")
        }
      }
      i += 1
    }
    result
  }

  private def castToDouble(src: FieldVector, alias: String, rowCount: Int, alloc: BufferAllocator): Float8Vector = {
    val result = newFloat8Vector(alias, alloc, rowCount)
    var i = 0
    while (i < rowCount) {
      if (src.isNull(i)) { result.setNull(i) }
      else {
        src match {
          case v: IntVector      => result.setSafe(i, v.get(i).toDouble)
          case v: BigIntVector   => result.setSafe(i, v.get(i).toDouble)
          case v: Float4Vector   => result.setSafe(i, v.get(i).toDouble)
          case v: Float8Vector   => result.setSafe(i, v.get(i))
          case v: VarCharVector  => result.setSafe(i, new String(v.get(i), StandardCharsets.UTF_8).trim.toDouble)
          case v: SmallIntVector => result.setSafe(i, v.get(i).toDouble)
          case v: TinyIntVector  => result.setSafe(i, v.get(i).toDouble)
          case _ => throw new UnsupportedOperationException(s"Cast to Double not supported from ${src.getClass.getSimpleName}")
        }
      }
      i += 1
    }
    result
  }

  private def castToString(src: FieldVector, alias: String, rowCount: Int, alloc: BufferAllocator): VarCharVector = {
    val result = newVarCharVector(alias, alloc, rowCount)
    var i = 0
    while (i < rowCount) {
      if (src.isNull(i)) { result.setNull(i) }
      else {
        val str = readAsString(src, i)
        result.setSafe(i, str.getBytes(StandardCharsets.UTF_8))
      }
      i += 1
    }
    result
  }

  // ---------------------------------------------------------------------------
  // Literal vectors
  // ---------------------------------------------------------------------------

  private def buildLiteralVector(
    lit: ProjectExpr.LiteralExpr,
    alias: String,
    rowCount: Int,
    allocator: BufferAllocator
  ): FieldVector = lit match {

    case ProjectExpr.IntLiteral(value) =>
      val v = newIntVector(alias, allocator, rowCount)
      var i = 0
      while (i < rowCount) { v.setSafe(i, value); i += 1 }
      v

    case ProjectExpr.LongLiteral(value) =>
      val v = newBigIntVector(alias, allocator, rowCount)
      var i = 0
      while (i < rowCount) { v.setSafe(i, value); i += 1 }
      v

    case ProjectExpr.FloatLiteral(value) =>
      val v = newFloat4Vector(alias, allocator, rowCount)
      var i = 0
      while (i < rowCount) { v.setSafe(i, value); i += 1 }
      v

    case ProjectExpr.DoubleLiteral(value) =>
      val v = newFloat8Vector(alias, allocator, rowCount)
      var i = 0
      while (i < rowCount) { v.setSafe(i, value); i += 1 }
      v

    case ProjectExpr.StringLiteral(value) =>
      val v     = newVarCharVector(alias, allocator, rowCount)
      val bytes = value.getBytes(StandardCharsets.UTF_8)
      var i = 0
      while (i < rowCount) { v.setSafe(i, bytes); i += 1 }
      v

    case ProjectExpr.BoolLiteral(value) =>
      val v   = newBitVector(alias, allocator, rowCount)
      val bit = if (value) 1 else 0
      var i = 0
      while (i < rowCount) { v.setSafe(i, bit); i += 1 }
      v
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
