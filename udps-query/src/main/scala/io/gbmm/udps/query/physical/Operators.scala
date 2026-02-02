package io.gbmm.udps.query.physical

// ---------------------------------------------------------------------------
// Join types
// ---------------------------------------------------------------------------

sealed trait JoinType extends Product with Serializable

object JoinType {
  case object Inner     extends JoinType
  case object LeftOuter extends JoinType
  case object RightOuter extends JoinType
  case object FullOuter extends JoinType
  case object Cross     extends JoinType
}

// ---------------------------------------------------------------------------
// Comparison operators used in filter expressions
// ---------------------------------------------------------------------------

sealed trait ComparisonOp extends Product with Serializable

object ComparisonOp {
  case object Eq            extends ComparisonOp
  case object NotEq         extends ComparisonOp
  case object LessThan      extends ComparisonOp
  case object LessThanOrEq  extends ComparisonOp
  case object GreaterThan   extends ComparisonOp
  case object GreaterThanOrEq extends ComparisonOp
}

// ---------------------------------------------------------------------------
// Arithmetic / expression operators used in projections
// ---------------------------------------------------------------------------

sealed trait ArithmeticOp extends Product with Serializable

object ArithmeticOp {
  case object Add      extends ArithmeticOp
  case object Subtract extends ArithmeticOp
  case object Multiply extends ArithmeticOp
  case object Divide   extends ArithmeticOp
  case object Modulo   extends ArithmeticOp
  case object Negate   extends ArithmeticOp
}

// ---------------------------------------------------------------------------
// Filter expressions (predicates)
// ---------------------------------------------------------------------------

sealed trait FilterExpression extends Product with Serializable

object FilterExpression {

  /** Comparison between two operands (column refs, literals, or nested expressions). */
  final case class Comparison(
    op: ComparisonOp,
    left: FilterExpression,
    right: FilterExpression
  ) extends FilterExpression

  /** Logical conjunction. */
  final case class And(children: Seq[FilterExpression]) extends FilterExpression

  /** Logical disjunction. */
  final case class Or(children: Seq[FilterExpression]) extends FilterExpression

  /** Logical negation. */
  final case class Not(child: FilterExpression) extends FilterExpression

  /** NULL check. */
  final case class IsNull(operand: FilterExpression) extends FilterExpression

  /** NOT NULL check. */
  final case class IsNotNull(operand: FilterExpression) extends FilterExpression

  /** IN list predicate. */
  final case class In(
    operand: FilterExpression,
    values: Seq[FilterExpression]
  ) extends FilterExpression

  /** BETWEEN predicate (inclusive on both ends). */
  final case class Between(
    operand: FilterExpression,
    lower: FilterExpression,
    upper: FilterExpression
  ) extends FilterExpression

  /** LIKE pattern match. */
  final case class Like(
    operand: FilterExpression,
    pattern: FilterExpression
  ) extends FilterExpression

  /** Reference to a column by its zero-based index in the input schema. */
  final case class ColumnRef(index: Int) extends FilterExpression

  /** A typed literal value.
    *
    * @param value    string-encoded value; `None` represents SQL NULL
    * @param dataType UDPS DataType of this literal
    */
  final case class Literal(
    value: Option[String],
    dataType: io.gbmm.udps.core.domain.DataType
  ) extends FilterExpression

  /** A CAST expression. */
  final case class Cast(
    operand: FilterExpression,
    dataType: io.gbmm.udps.core.domain.DataType
  ) extends FilterExpression
}

// ---------------------------------------------------------------------------
// Projection expressions
// ---------------------------------------------------------------------------

sealed trait Projection extends Product with Serializable

object Projection {

  /** Direct reference to an input column by zero-based index. */
  final case class ColumnRef(index: Int) extends Projection

  /** A literal value with a UDPS DataType. */
  final case class Literal(
    value: Option[String],
    dataType: io.gbmm.udps.core.domain.DataType
  ) extends Projection

  /** A computed expression (arithmetic, function call, cast). */
  final case class Expression(
    op: ArithmeticOp,
    operands: Seq[Projection]
  ) extends Projection

  /** A CAST projection. */
  final case class Cast(
    operand: Projection,
    dataType: io.gbmm.udps.core.domain.DataType
  ) extends Projection
}

// ---------------------------------------------------------------------------
// Aggregate expressions
// ---------------------------------------------------------------------------

sealed trait AggregateExpression extends Product with Serializable {
  def columnIndex: Int
}

object AggregateExpression {
  final case class Sum(columnIndex: Int)           extends AggregateExpression
  final case class Count(columnIndex: Int)         extends AggregateExpression
  final case class Avg(columnIndex: Int)           extends AggregateExpression
  final case class Min(columnIndex: Int)           extends AggregateExpression
  final case class Max(columnIndex: Int)           extends AggregateExpression
  final case class CountDistinct(columnIndex: Int) extends AggregateExpression

  /** COUNT(*) uses a sentinel column index of -1. */
  val CountStarIndex: Int = -1
}

// ---------------------------------------------------------------------------
// Sort key
// ---------------------------------------------------------------------------

final case class SortKey(
  columnIndex: Int,
  ascending: Boolean,
  nullsFirst: Boolean
)

// ---------------------------------------------------------------------------
// Physical operator hierarchy
// ---------------------------------------------------------------------------

sealed trait PhysicalOperator extends Product with Serializable {

  /** Column names produced by this operator. */
  def outputSchema: Seq[String]

  /** Direct child operators. */
  def children: Seq[PhysicalOperator]

  /** Estimated row count produced by this operator. */
  def estimatedRows: Long
}

/** Full or partial table scan against the storage layer. */
final case class TableScanOp(
  tableName: String,
  namespace: String,
  columns: Seq[String],
  predicate: Option[FilterExpression],
  estimatedRows: Long
) extends PhysicalOperator {
  override val outputSchema: Seq[String]          = columns
  override val children: Seq[PhysicalOperator]    = Seq.empty
}

/** Row-level filter applied after reading. */
final case class FilterOp(
  child: PhysicalOperator,
  condition: FilterExpression,
  estimatedRows: Long
) extends PhysicalOperator {
  override val outputSchema: Seq[String]          = child.outputSchema
  override val children: Seq[PhysicalOperator]    = Seq(child)
}

/** Column projection and/or computed column generation. */
final case class ProjectOp(
  child: PhysicalOperator,
  projections: Seq[Projection],
  outputColumns: Seq[String],
  estimatedRows: Long
) extends PhysicalOperator {
  override val outputSchema: Seq[String]          = outputColumns
  override val children: Seq[PhysicalOperator]    = Seq(child)
}

/** Hash-based equi-join. */
final case class HashJoinOp(
  left: PhysicalOperator,
  right: PhysicalOperator,
  joinType: JoinType,
  leftKeys: Seq[Int],
  rightKeys: Seq[Int],
  estimatedRows: Long
) extends PhysicalOperator {
  override val outputSchema: Seq[String]          = left.outputSchema ++ right.outputSchema
  override val children: Seq[PhysicalOperator]    = Seq(left, right)
}

/** Nested-loop join for non-equi or cross joins. */
final case class NestedLoopJoinOp(
  left: PhysicalOperator,
  right: PhysicalOperator,
  joinType: JoinType,
  condition: Option[FilterExpression],
  estimatedRows: Long
) extends PhysicalOperator {
  override val outputSchema: Seq[String]          = left.outputSchema ++ right.outputSchema
  override val children: Seq[PhysicalOperator]    = Seq(left, right)
}

/** Hash-based aggregation. */
final case class HashAggregateOp(
  child: PhysicalOperator,
  groupByKeys: Seq[Int],
  aggregations: Seq[AggregateExpression],
  outputColumns: Seq[String],
  estimatedRows: Long
) extends PhysicalOperator {
  override val outputSchema: Seq[String]          = outputColumns
  override val children: Seq[PhysicalOperator]    = Seq(child)
}

/** In-memory sort. */
final case class SortOp(
  child: PhysicalOperator,
  sortKeys: Seq[SortKey],
  estimatedRows: Long
) extends PhysicalOperator {
  override val outputSchema: Seq[String]          = child.outputSchema
  override val children: Seq[PhysicalOperator]    = Seq(child)
}

/** LIMIT / OFFSET. */
final case class LimitOp(
  child: PhysicalOperator,
  offset: Long,
  fetch: Long,
  estimatedRows: Long
) extends PhysicalOperator {
  override val outputSchema: Seq[String]          = child.outputSchema
  override val children: Seq[PhysicalOperator]    = Seq(child)
}

/** UNION / UNION ALL. */
final case class UnionOp(
  inputs: Seq[PhysicalOperator],
  all: Boolean,
  estimatedRows: Long
) extends PhysicalOperator {
  override val outputSchema: Seq[String] =
    if (inputs.nonEmpty) inputs.head.outputSchema else Seq.empty
  override val children: Seq[PhysicalOperator]    = inputs
}
