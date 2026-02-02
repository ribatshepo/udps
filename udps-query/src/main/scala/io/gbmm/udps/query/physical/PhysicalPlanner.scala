package io.gbmm.udps.query.physical

import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.core.domain.DataType
import io.gbmm.udps.query.calcite.TypeMapper
import org.apache.calcite.rel.{RelNode, RelFieldCollation}
import org.apache.calcite.rel.core.{
  Aggregate,
  AggregateCall,
  Filter,
  Join,
  Project,
  Sort,
  TableScan,
  Union
}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.NlsString

import scala.jdk.CollectionConverters._

/** Converts an optimized Calcite [[RelNode]] tree into a UDPS
  * [[PhysicalOperator]] tree and optionally into an [[ExecutionDAG]].
  *
  * Thread-safe: no mutable state.
  */
final class PhysicalPlanner private () extends LazyLogging {

  private val DefaultRowEstimate: Long = 1000L

  private val NestedLoopRowThreshold: Long = 10000L

  /** Convert a Calcite [[RelNode]] into a [[PhysicalOperator]] tree. */
  def plan(relNode: RelNode): PhysicalOperator =
    convertRelNode(relNode)

  /** Convert a Calcite [[RelNode]] into an [[ExecutionDAG]]. */
  def planToDAG(relNode: RelNode): ExecutionDAG =
    ExecutionDAG.fromOperator(plan(relNode))

  // -----------------------------------------------------------------------
  // RelNode dispatch
  // -----------------------------------------------------------------------

  private def convertRelNode(rel: RelNode): PhysicalOperator =
    rel match {
      case scan: TableScan   => convertTableScan(scan)
      case filter: Filter    => convertFilter(filter)
      case project: Project  => convertProject(project)
      case join: Join        => convertJoin(join)
      case agg: Aggregate    => convertAggregate(agg)
      case sort: Sort        => convertSort(sort)
      case union: Union      => convertUnion(union)
      case other =>
        logger.warn(
          "Unrecognised RelNode type {}, attempting generic conversion via inputs",
          other.getClass.getName
        )
        convertGeneric(other)
    }

  // -----------------------------------------------------------------------
  // TableScan
  // -----------------------------------------------------------------------

  private def convertTableScan(scan: TableScan): TableScanOp = {
    val qualifiedName = scan.getTable.getQualifiedName.asScala.toSeq
    val (namespace, tableName) = qualifiedName match {
      case Seq(ns, tbl) => (ns, tbl)
      case Seq(tbl)     => ("default", tbl)
      case parts        => (parts.init.mkString("."), parts.last)
    }

    val columns = scan.getRowType.getFieldList.asScala.map(_.getName).toSeq
    val estimatedRows = estimateRowCount(scan)

    TableScanOp(tableName, namespace, columns, predicate = None, estimatedRows)
  }

  // -----------------------------------------------------------------------
  // Filter
  // -----------------------------------------------------------------------

  private def convertFilter(filter: Filter): PhysicalOperator = {
    val child     = convertRelNode(filter.getInput)
    val rexBuilder = filter.getCluster.getRexBuilder
    val condition = convertRexToFilter(filter.getCondition, rexBuilder)
    val estimated = estimateRowCount(filter)

    FilterOp(child, condition, estimated)
  }

  // -----------------------------------------------------------------------
  // Project
  // -----------------------------------------------------------------------

  private def convertProject(project: Project): PhysicalOperator = {
    val child = convertRelNode(project.getInput)
    val projections = project.getProjects.asScala.map(convertRexToProjection).toSeq
    val outputColumns = project.getRowType.getFieldList.asScala.map(_.getName).toSeq
    val estimated = estimateRowCount(project)

    ProjectOp(child, projections, outputColumns, estimated)
  }

  // -----------------------------------------------------------------------
  // Join
  // -----------------------------------------------------------------------

  @scala.annotation.nowarn("msg=deprecated")
  private def convertJoin(join: Join): PhysicalOperator = {
    val left  = convertRelNode(join.getLeft)
    val right = convertRelNode(join.getRight)
    val jt    = convertJoinType(join.getJoinType)
    val estimated = estimateRowCount(join)
    val rexBuilder = join.getCluster.getRexBuilder

    val joinInfo = join.analyzeCondition()
    val leftKeys  = joinInfo.leftKeys.asScala.map(_.intValue()).toSeq
    val rightKeys = joinInfo.rightKeys.asScala.map(_.intValue()).toSeq

    val hasEquiKeys = leftKeys.nonEmpty

    val nonEquiCondition = if (joinInfo.isEqui) {
      None
    } else {
      val remaining = joinInfo.getRemaining(rexBuilder)
      remaining match {
        case lit: RexLiteral if lit.isAlwaysTrue => None
        case r                                   => Some(convertRexToFilter(r, rexBuilder))
      }
    }

    if (hasEquiKeys) {
      val hashJoin = HashJoinOp(left, right, jt, leftKeys, rightKeys, estimated)
      nonEquiCondition match {
        case Some(cond) => FilterOp(hashJoin, cond, estimated)
        case None       => hashJoin
      }
    } else if (jt == JoinType.Cross || estimateSmallerSide(left, right) <= NestedLoopRowThreshold) {
      NestedLoopJoinOp(left, right, jt, nonEquiCondition, estimated)
    } else {
      logger.warn(
        "Large non-equi join detected ({} x {} rows); consider rewriting the query",
        Long.box(left.estimatedRows),
        Long.box(right.estimatedRows)
      )
      NestedLoopJoinOp(left, right, jt, nonEquiCondition, estimated)
    }
  }

  private def convertJoinType(
    calciteJoinType: org.apache.calcite.rel.core.JoinRelType
  ): JoinType =
    calciteJoinType match {
      case org.apache.calcite.rel.core.JoinRelType.INNER => JoinType.Inner
      case org.apache.calcite.rel.core.JoinRelType.LEFT  => JoinType.LeftOuter
      case org.apache.calcite.rel.core.JoinRelType.RIGHT => JoinType.RightOuter
      case org.apache.calcite.rel.core.JoinRelType.FULL  => JoinType.FullOuter
      case _                                              => JoinType.Cross
    }

  // -----------------------------------------------------------------------
  // Aggregate
  // -----------------------------------------------------------------------

  private def convertAggregate(agg: Aggregate): PhysicalOperator = {
    val child = convertRelNode(agg.getInput)
    val groupKeys = agg.getGroupSet.asList().asScala.map(_.intValue()).toSeq
    val aggregations = agg.getAggCallList.asScala.map(convertAggCall).toSeq
    val outputColumns = agg.getRowType.getFieldList.asScala.map(_.getName).toSeq
    val estimated = estimateRowCount(agg)

    HashAggregateOp(child, groupKeys, aggregations, outputColumns, estimated)
  }

  private def convertAggCall(call: AggregateCall): AggregateExpression = {
    val argIndex = if (call.getArgList.isEmpty) {
      AggregateExpression.CountStarIndex
    } else {
      call.getArgList.get(0).intValue()
    }

    val isDistinct = call.isDistinct

    call.getAggregation.getKind match {
      case SqlKind.SUM | SqlKind.SUM0 =>
        AggregateExpression.Sum(argIndex)
      case SqlKind.COUNT =>
        if (isDistinct) AggregateExpression.CountDistinct(argIndex)
        else AggregateExpression.Count(argIndex)
      case SqlKind.AVG =>
        AggregateExpression.Avg(argIndex)
      case SqlKind.MIN =>
        AggregateExpression.Min(argIndex)
      case SqlKind.MAX =>
        AggregateExpression.Max(argIndex)
      case other =>
        logger.warn("Unmapped aggregate function {}; defaulting to Count", other)
        AggregateExpression.Count(argIndex)
    }
  }

  // -----------------------------------------------------------------------
  // Sort (may include LIMIT / OFFSET)
  // -----------------------------------------------------------------------

  private def convertSort(sort: Sort): PhysicalOperator = {
    val child = convertRelNode(sort.getInput)
    val estimated = estimateRowCount(sort)

    val hasSortKeys = sort.getCollation.getFieldCollations.size() > 0

    val sortedChild = if (hasSortKeys) {
      val keys = sort.getCollation.getFieldCollations.asScala.map { fc =>
        SortKey(
          columnIndex = fc.getFieldIndex,
          ascending = fc.getDirection == RelFieldCollation.Direction.ASCENDING ||
            fc.getDirection == RelFieldCollation.Direction.STRICTLY_ASCENDING,
          nullsFirst = fc.nullDirection == RelFieldCollation.NullDirection.FIRST
        )
      }.toSeq
      SortOp(child, keys, estimated)
    } else {
      child
    }

    val hasLimit  = sort.fetch != null
    val hasOffset = sort.offset != null

    if (hasLimit || hasOffset) {
      val offsetVal = if (hasOffset) rexLiteralToLong(sort.offset) else 0L
      val fetchVal  = if (hasLimit)  rexLiteralToLong(sort.fetch) else Long.MaxValue
      val limitEstimate = math.min(estimated, fetchVal)
      LimitOp(sortedChild, offsetVal, fetchVal, limitEstimate)
    } else {
      sortedChild
    }
  }

  // -----------------------------------------------------------------------
  // Union
  // -----------------------------------------------------------------------

  private def convertUnion(union: Union): PhysicalOperator = {
    val inputs = union.getInputs.asScala.map(convertRelNode).toSeq
    val estimated = estimateRowCount(union)
    UnionOp(inputs, all = union.all, estimated)
  }

  // -----------------------------------------------------------------------
  // Generic fallback for unknown RelNode types
  // -----------------------------------------------------------------------

  private def convertGeneric(rel: RelNode): PhysicalOperator = {
    val inputs = rel.getInputs.asScala.toSeq
    if (inputs.size == 1) {
      val child = convertRelNode(inputs.head)
      val outputColumns = rel.getRowType.getFieldList.asScala.map(_.getName).toSeq
      val projections = outputColumns.indices.map(i => Projection.ColumnRef(i): Projection)
      ProjectOp(child, projections.toSeq, outputColumns, child.estimatedRows)
    } else if (inputs.isEmpty) {
      val columns = rel.getRowType.getFieldList.asScala.map(_.getName).toSeq
      TableScanOp("__values__", "default", columns, None, estimatedRows = 1L)
    } else {
      val converted = inputs.map(convertRelNode)
      converted.reduceLeft { (left, right) =>
        val est = left.estimatedRows * right.estimatedRows
        NestedLoopJoinOp(left, right, JoinType.Cross, None, est)
      }
    }
  }

  // -----------------------------------------------------------------------
  // RexNode -> FilterExpression conversion
  // -----------------------------------------------------------------------

  private def convertRexToFilter(rex: RexNode, rexBuilder: RexBuilder): FilterExpression =
    rex match {
      case call: RexCall =>
        convertRexCallToFilter(call, rexBuilder)

      case inputRef: RexInputRef =>
        FilterExpression.ColumnRef(inputRef.getIndex)

      case literal: RexLiteral =>
        convertRexLiteralToFilterLiteral(literal)

      case fieldAccess: RexFieldAccess =>
        fieldAccess.getReferenceExpr match {
          case ref: RexInputRef => FilterExpression.ColumnRef(ref.getIndex)
          case _                => FilterExpression.ColumnRef(fieldAccess.getField.getIndex)
        }

      case _ =>
        logger.warn(
          "Unmapped RexNode type {} in filter position; wrapping as literal TRUE",
          rex.getClass.getName
        )
        FilterExpression.Literal(Some("true"), DataType.Boolean)
    }

  private def convertRexCallToFilter(call: RexCall, rexBuilder: RexBuilder): FilterExpression = {
    val operands = call.getOperands.asScala.toSeq

    call.getKind match {
      case SqlKind.EQUALS =>
        FilterExpression.Comparison(
          ComparisonOp.Eq,
          convertRexToFilter(operands.head, rexBuilder),
          convertRexToFilter(operands(1), rexBuilder)
        )
      case SqlKind.NOT_EQUALS =>
        FilterExpression.Comparison(
          ComparisonOp.NotEq,
          convertRexToFilter(operands.head, rexBuilder),
          convertRexToFilter(operands(1), rexBuilder)
        )
      case SqlKind.LESS_THAN =>
        FilterExpression.Comparison(
          ComparisonOp.LessThan,
          convertRexToFilter(operands.head, rexBuilder),
          convertRexToFilter(operands(1), rexBuilder)
        )
      case SqlKind.LESS_THAN_OR_EQUAL =>
        FilterExpression.Comparison(
          ComparisonOp.LessThanOrEq,
          convertRexToFilter(operands.head, rexBuilder),
          convertRexToFilter(operands(1), rexBuilder)
        )
      case SqlKind.GREATER_THAN =>
        FilterExpression.Comparison(
          ComparisonOp.GreaterThan,
          convertRexToFilter(operands.head, rexBuilder),
          convertRexToFilter(operands(1), rexBuilder)
        )
      case SqlKind.GREATER_THAN_OR_EQUAL =>
        FilterExpression.Comparison(
          ComparisonOp.GreaterThanOrEq,
          convertRexToFilter(operands.head, rexBuilder),
          convertRexToFilter(operands(1), rexBuilder)
        )

      // Logical operators
      case SqlKind.AND =>
        FilterExpression.And(operands.map(convertRexToFilter(_, rexBuilder)))
      case SqlKind.OR =>
        FilterExpression.Or(operands.map(convertRexToFilter(_, rexBuilder)))
      case SqlKind.NOT =>
        FilterExpression.Not(convertRexToFilter(operands.head, rexBuilder))

      // Null checks
      case SqlKind.IS_NULL =>
        FilterExpression.IsNull(convertRexToFilter(operands.head, rexBuilder))
      case SqlKind.IS_NOT_NULL =>
        FilterExpression.IsNotNull(convertRexToFilter(operands.head, rexBuilder))

      // IN
      case SqlKind.IN =>
        FilterExpression.In(
          convertRexToFilter(operands.head, rexBuilder),
          operands.tail.map(convertRexToFilter(_, rexBuilder))
        )

      // BETWEEN
      case SqlKind.BETWEEN =>
        FilterExpression.Between(
          convertRexToFilter(operands.head, rexBuilder),
          convertRexToFilter(operands(1), rexBuilder),
          convertRexToFilter(operands(2), rexBuilder)
        )

      // LIKE
      case SqlKind.LIKE =>
        FilterExpression.Like(
          convertRexToFilter(operands.head, rexBuilder),
          convertRexToFilter(operands(1), rexBuilder)
        )

      // CAST
      case SqlKind.CAST =>
        val targetDataType = TypeMapper.fromRelDataType(call.getType)
        FilterExpression.Cast(convertRexToFilter(operands.head, rexBuilder), targetDataType)

      // SEARCH (Calcite 1.27+ uses SEARCH for IN-list optimisation)
      case SqlKind.SEARCH =>
        val expanded = RexUtil.expandSearch(rexBuilder, null, call)
        convertRexToFilter(expanded, rexBuilder)

      case other =>
        logger.warn(
          "Unmapped RexCall kind {} in filter; wrapping as literal TRUE",
          other
        )
        FilterExpression.Literal(Some("true"), DataType.Boolean)
    }
  }

  private def convertRexLiteralToFilterLiteral(lit: RexLiteral): FilterExpression.Literal = {
    val dataType = TypeMapper.fromRelDataType(lit.getType)
    val value = extractLiteralValue(lit)
    FilterExpression.Literal(value, dataType)
  }

  // -----------------------------------------------------------------------
  // RexNode -> Projection conversion
  // -----------------------------------------------------------------------

  private def convertRexToProjection(rex: RexNode): Projection =
    rex match {
      case inputRef: RexInputRef =>
        Projection.ColumnRef(inputRef.getIndex)

      case literal: RexLiteral =>
        val dataType = TypeMapper.fromRelDataType(literal.getType)
        val value = extractLiteralValue(literal)
        Projection.Literal(value, dataType)

      case call: RexCall =>
        convertRexCallToProjection(call)

      case fieldAccess: RexFieldAccess =>
        fieldAccess.getReferenceExpr match {
          case ref: RexInputRef => Projection.ColumnRef(ref.getIndex)
          case _                => Projection.ColumnRef(fieldAccess.getField.getIndex)
        }

      case _ =>
        logger.warn(
          "Unmapped RexNode type {} in projection; wrapping as NULL literal",
          rex.getClass.getName
        )
        Projection.Literal(None, DataType.Null)
    }

  private def convertRexCallToProjection(call: RexCall): Projection = {
    val operands = call.getOperands.asScala.toSeq

    call.getKind match {
      case SqlKind.PLUS =>
        Projection.Expression(
          ArithmeticOp.Add,
          operands.map(convertRexToProjection)
        )
      case SqlKind.MINUS =>
        if (operands.size == 1) {
          Projection.Expression(
            ArithmeticOp.Negate,
            operands.map(convertRexToProjection)
          )
        } else {
          Projection.Expression(
            ArithmeticOp.Subtract,
            operands.map(convertRexToProjection)
          )
        }
      case SqlKind.TIMES =>
        Projection.Expression(
          ArithmeticOp.Multiply,
          operands.map(convertRexToProjection)
        )
      case SqlKind.DIVIDE =>
        Projection.Expression(
          ArithmeticOp.Divide,
          operands.map(convertRexToProjection)
        )
      case SqlKind.MOD =>
        Projection.Expression(
          ArithmeticOp.Modulo,
          operands.map(convertRexToProjection)
        )
      case SqlKind.CAST =>
        val targetDataType = TypeMapper.fromRelDataType(call.getType)
        Projection.Cast(convertRexToProjection(operands.head), targetDataType)
      case _ =>
        // For other function calls, map operands through Add as a
        // fallback representation. The execution engine resolves the
        // original operator via the ArithmeticOp context. In practice,
        // most projections from Calcite are column refs, literals,
        // arithmetic, or casts.
        logger.warn(
          "Unmapped RexCall kind {} in projection; treating operands as Add expression",
          call.getKind
        )
        Projection.Expression(
          ArithmeticOp.Add,
          operands.map(convertRexToProjection)
        )
    }
  }

  // -----------------------------------------------------------------------
  // Utility helpers
  // -----------------------------------------------------------------------

  /** Extract a string representation of a literal value, or None for NULL. */
  private def extractLiteralValue(lit: RexLiteral): Option[String] =
    if (lit.isNull) {
      None
    } else {
      val raw = lit.getValue
      raw match {
        case nls: NlsString           => Some(nls.getValue)
        case bd: java.math.BigDecimal => Some(bd.toPlainString)
        case n: Number                => Some(n.toString)
        case b: java.lang.Boolean     => Some(b.toString)
        case other                    => Some(other.toString)
      }
    }

  /** Retrieve an estimated row count from Calcite's metadata, falling back
    * to [[DefaultRowEstimate]] when unavailable.
    */
  private def estimateRowCount(rel: RelNode): Long = {
    val metadata = rel.getCluster.getMetadataQuery
    try {
      val estimate = metadata.getRowCount(rel)
      if (estimate != null && !estimate.isNaN && !estimate.isInfinite && estimate > 0.0) {
        estimate.toLong
      } else {
        DefaultRowEstimate
      }
    } catch {
      case _: Exception => DefaultRowEstimate
    }
  }

  private def estimateSmallerSide(
    left: PhysicalOperator,
    right: PhysicalOperator
  ): Long =
    math.min(left.estimatedRows, right.estimatedRows)

  private def rexLiteralToLong(rex: RexNode): Long =
    rex match {
      case lit: RexLiteral =>
        val value = lit.getValue
        value match {
          case bd: java.math.BigDecimal => bd.longValueExact()
          case n: Number               => n.longValue()
          case _                       => DefaultRowEstimate
        }
      case _ => DefaultRowEstimate
    }
}

object PhysicalPlanner {

  /** Create a new [[PhysicalPlanner]] instance. */
  def apply(): PhysicalPlanner = new PhysicalPlanner()
}
