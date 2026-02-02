package io.gbmm.udps.query.execution

import cats.effect.{Clock, IO}
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.query.physical._

import java.util.concurrent.TimeUnit

/** Result produced by executing a single stage for a specific partition. */
final case class StageResult(
  stageId: Int,
  partitionId: Int,
  rows: Seq[Map[String, Any]],
  rowCount: Long,
  executionTimeMs: Long
)

/** Provides row data for table scans.
  *
  * Implementations connect to the underlying storage layer (e.g. Parquet files,
  * Arrow IPC, object storage) to materialise rows for a given table and partition.
  */
trait DataReader {

  /** Read rows from the specified table within the given namespace.
    *
    * @param tableName      table to read from
    * @param namespace      namespace/schema of the table
    * @param columns        columns to project (determines positional indices in result rows)
    * @param predicate      optional filter predicate pushed down to storage
    * @param partitionId    which partition to read
    * @param partitionCount total number of partitions for this scan
    * @return rows as column-name-to-value maps keyed by column name
    */
  def read(
    tableName: String,
    namespace: String,
    columns: Seq[String],
    predicate: Option[FilterExpression],
    partitionId: Int,
    partitionCount: Int
  ): IO[Seq[Map[String, Any]]]
}

/** Executes a single physical operator stage locally on a worker node.
  *
  * The executor is partition-aware: each call processes a specific partition
  * of the data as determined by the coordinator.
  *
  * @param dataReader        storage-backed data reader for table scans
  * @param dependencyResults results from completed dependency stages, keyed by stage ID
  */
final class StageExecutor(
  dataReader: DataReader,
  dependencyResults: Map[Int, StageResult]
) extends LazyLogging {

  /** Execute the given execution stage for a specific partition.
    *
    * @param stage       the stage definition from the ExecutionDAG
    * @param partitionId the partition index to process (0-based)
    * @return the stage result with timing metrics
    */
  def execute(stage: ExecutionStage, partitionId: Int): IO[StageResult] =
    for {
      startNanos <- Clock[IO].monotonic.map(_.toNanos)
      rows       <- executeOperator(stage.operator, partitionId, stage.partitionCount)
      endNanos   <- Clock[IO].monotonic.map(_.toNanos)
      elapsedMs   = TimeUnit.NANOSECONDS.toMillis(endNanos - startNanos)
      _          <- IO(logger.debug(
                      "Stage {} partition {} produced {} rows in {}ms",
                      stage.id.toString, partitionId.toString,
                      rows.size.toString, elapsedMs.toString
                    ))
    } yield StageResult(
      stageId = stage.id,
      partitionId = partitionId,
      rows = rows,
      rowCount = rows.size.toLong,
      executionTimeMs = elapsedMs
    )

  private def executeOperator(
    op: PhysicalOperator,
    partitionId: Int,
    partitionCount: Int
  ): IO[Seq[Map[String, Any]]] =
    op match {
      case scan: TableScanOp       => executeTableScan(scan, partitionId, partitionCount)
      case filter: FilterOp        => executeFilter(filter, partitionId, partitionCount)
      case project: ProjectOp      => executeProject(project, partitionId, partitionCount)
      case join: HashJoinOp        => executeHashJoin(join, partitionId, partitionCount)
      case join: NestedLoopJoinOp  => executeNestedLoopJoin(join, partitionId, partitionCount)
      case agg: HashAggregateOp    => executeHashAggregate(agg, partitionId, partitionCount)
      case sort: SortOp            => executeSort(sort, partitionId, partitionCount)
      case limit: LimitOp          => executeLimit(limit, partitionId, partitionCount)
      case union: UnionOp          => executeUnion(union, partitionId, partitionCount)
    }

  private def executeTableScan(
    scan: TableScanOp,
    partitionId: Int,
    partitionCount: Int
  ): IO[Seq[Map[String, Any]]] =
    dataReader.read(
      tableName = scan.tableName,
      namespace = scan.namespace,
      columns = scan.columns,
      predicate = scan.predicate,
      partitionId = partitionId,
      partitionCount = partitionCount
    )

  private def executeFilter(
    filter: FilterOp,
    partitionId: Int,
    partitionCount: Int
  ): IO[Seq[Map[String, Any]]] =
    for {
      input <- executeOperator(filter.child, partitionId, partitionCount)
      schema = filter.child.outputSchema
      filtered = input.filter(row => evaluateFilterExpr(filter.condition, row, schema))
    } yield filtered

  private def executeProject(
    project: ProjectOp,
    partitionId: Int,
    partitionCount: Int
  ): IO[Seq[Map[String, Any]]] =
    for {
      input <- executeOperator(project.child, partitionId, partitionCount)
      inputSchema = project.child.outputSchema
      projected = input.map { row =>
        project.projections.zip(project.outputColumns).map { case (proj, alias) =>
          alias -> evaluateProjection(proj, row, inputSchema)
        }.toMap
      }
    } yield projected

  private def executeHashJoin(
    join: HashJoinOp,
    partitionId: Int,
    partitionCount: Int
  ): IO[Seq[Map[String, Any]]] =
    for {
      leftRows  <- executeOperator(join.left, partitionId, partitionCount)
      rightRows <- executeOperator(join.right, partitionId, partitionCount)
      leftSchema  = join.left.outputSchema
      rightSchema = join.right.outputSchema
      leftKeyNames  = join.leftKeys.map(leftSchema)
      rightKeyNames = join.rightKeys.map(rightSchema)
      hashTable  = buildHashTable(leftRows, leftKeyNames)
      joined     = probeHashTable(hashTable, rightRows, rightKeyNames, leftSchema, join.joinType)
    } yield joined

  private def buildHashTable(
    rows: Seq[Map[String, Any]],
    keys: Seq[String]
  ): Map[Seq[Any], Seq[Map[String, Any]]] =
    rows.groupBy(row => keys.map(k => row.getOrElse(k, null)))

  private def probeHashTable(
    hashTable: Map[Seq[Any], Seq[Map[String, Any]]],
    probeRows: Seq[Map[String, Any]],
    probeKeys: Seq[String],
    buildSchema: Seq[String],
    joinType: JoinType
  ): Seq[Map[String, Any]] = {
    val matchedBuildKeys = scala.collection.mutable.Set.empty[Seq[Any]]

    val probeResults: Seq[Map[String, Any]] = probeRows.flatMap { probeRow =>
      val key = probeKeys.map(k => probeRow.getOrElse(k, null))
      hashTable.get(key) match {
        case Some(buildRows) =>
          matchedBuildKeys += key
          buildRows.map(buildRow => buildRow ++ probeRow)
        case None =>
          joinType match {
            case JoinType.RightOuter | JoinType.FullOuter =>
              Seq(nullPaddedRow(buildSchema) ++ probeRow)
            case _ =>
              Seq.empty
          }
      }
    }

    val probeCols = probeRows.headOption.map(_.keys.toSeq).getOrElse(Seq.empty)
    val unmatchedBuild: Seq[Map[String, Any]] = joinType match {
      case JoinType.LeftOuter | JoinType.FullOuter =>
        hashTable.flatMap { case (key, buildRows) =>
          if (matchedBuildKeys.contains(key)) Seq.empty
          else buildRows.map(buildRow => buildRow ++ nullPaddedRow(probeCols))
        }.toSeq
      case _ =>
        Seq.empty
    }

    probeResults ++ unmatchedBuild
  }

  private def nullPaddedRow(columns: Seq[String]): Map[String, Any] =
    columns.map(c => c -> (null: Any)).toMap

  private def executeNestedLoopJoin(
    join: NestedLoopJoinOp,
    partitionId: Int,
    partitionCount: Int
  ): IO[Seq[Map[String, Any]]] =
    for {
      leftRows  <- executeOperator(join.left, partitionId, partitionCount)
      rightRows <- executeOperator(join.right, partitionId, partitionCount)
      schema     = join.left.outputSchema ++ join.right.outputSchema
      joined     = nestedLoopJoinRows(leftRows, rightRows, join.condition, join.joinType, schema)
    } yield joined

  private def nestedLoopJoinRows(
    leftRows: Seq[Map[String, Any]],
    rightRows: Seq[Map[String, Any]],
    condition: Option[FilterExpression],
    joinType: JoinType,
    combinedSchema: Seq[String]
  ): Seq[Map[String, Any]] = {
    val rightCols = rightRows.headOption.map(_.keys.toSeq).getOrElse(Seq.empty)
    val leftCols = leftRows.headOption.map(_.keys.toSeq).getOrElse(Seq.empty)

    val results = scala.collection.mutable.ArrayBuffer.empty[Map[String, Any]]
    val matchedLeft = scala.collection.mutable.Set.empty[Int]
    val matchedRight = scala.collection.mutable.Set.empty[Int]

    leftRows.zipWithIndex.foreach { case (leftRow, li) =>
      var foundMatch = false
      rightRows.zipWithIndex.foreach { case (rightRow, ri) =>
        val combined = leftRow ++ rightRow
        val passes = condition.forall(pred => evaluateFilterExpr(pred, combined, combinedSchema))
        if (passes) {
          results += combined
          foundMatch = true
          matchedLeft += li
          matchedRight += ri
        }
      }
      if (!foundMatch && (joinType == JoinType.LeftOuter || joinType == JoinType.FullOuter)) {
        results += leftRow ++ nullPaddedRow(rightCols)
      }
    }

    if (joinType == JoinType.RightOuter || joinType == JoinType.FullOuter) {
      rightRows.zipWithIndex.foreach { case (rightRow, ri) =>
        if (!matchedRight.contains(ri)) {
          results += nullPaddedRow(leftCols) ++ rightRow
        }
      }
    }

    results.toSeq
  }

  private def executeHashAggregate(
    agg: HashAggregateOp,
    partitionId: Int,
    partitionCount: Int
  ): IO[Seq[Map[String, Any]]] =
    for {
      input <- executeOperator(agg.child, partitionId, partitionCount)
      inputSchema = agg.child.outputSchema
      groupKeyNames = agg.groupByKeys.map(inputSchema)
      grouped = if (groupKeyNames.nonEmpty) {
                  input.groupBy(row => groupKeyNames.map(k => k -> row.getOrElse(k, null)))
                } else {
                  Map(Seq.empty[(String, Any)] -> input)
                }
      aggregated = grouped.map { case (groupKey, groupRows) =>
        val keyMap = groupKey.toMap
        val aggResults = agg.aggregations.zip(
          agg.outputColumns.drop(agg.groupByKeys.size)
        ).map { case (aggExpr, alias) =>
          alias -> computeAggregate(aggExpr, groupRows, inputSchema)
        }.toMap
        keyMap ++ aggResults
      }.toSeq
    } yield aggregated

  private def computeAggregate(
    aggExpr: AggregateExpression,
    rows: Seq[Map[String, Any]],
    schema: Seq[String]
  ): Any = {
    val colIdx = aggExpr.columnIndex

    aggExpr match {
      case _: AggregateExpression.Count if colIdx == AggregateExpression.CountStarIndex =>
        rows.size.toLong

      case _: AggregateExpression.Count =>
        val colName = schema.lift(colIdx).getOrElse("")
        rows.count(row => row.get(colName).exists(_ != null)).toLong

      case _: AggregateExpression.CountDistinct =>
        val colName = schema.lift(colIdx).getOrElse("")
        rows.flatMap(row => row.get(colName).filter(_ != null)).distinct.size.toLong

      case _: AggregateExpression.Sum =>
        val colName = schema.lift(colIdx).getOrElse("")
        val values = rows.flatMap(row => row.get(colName).filter(_ != null))
        sumValues(values)

      case _: AggregateExpression.Avg =>
        val colName = schema.lift(colIdx).getOrElse("")
        val values = rows.flatMap(row => row.get(colName).filter(_ != null))
        avgValues(values)

      case _: AggregateExpression.Min =>
        val colName = schema.lift(colIdx).getOrElse("")
        val values = rows.flatMap(row => row.get(colName).filter(_ != null))
        minValue(values)

      case _: AggregateExpression.Max =>
        val colName = schema.lift(colIdx).getOrElse("")
        val values = rows.flatMap(row => row.get(colName).filter(_ != null))
        maxValue(values)
    }
  }

  private def sumValues(values: Seq[Any]): Any =
    if (values.isEmpty) 0L
    else {
      values.head match {
        case _: Long    => values.map(_.asInstanceOf[Long]).sum
        case _: Int     => values.map(_.asInstanceOf[Int].toLong).sum
        case _: Double  => values.map(_.asInstanceOf[Double]).sum
        case _: Float   => values.map(_.asInstanceOf[Float].toDouble).sum
        case _: BigDecimal => values.map(_.asInstanceOf[BigDecimal]).sum
        case _: java.math.BigDecimal =>
          values.map(v => BigDecimal(v.asInstanceOf[java.math.BigDecimal])).sum.bigDecimal
        case _ => values.size.toLong
      }
    }

  private def avgValues(values: Seq[Any]): Any =
    if (values.isEmpty) null
    else {
      val sum = sumValues(values)
      val count = values.size
      sum match {
        case l: Long       => l.toDouble / count
        case d: Double     => d / count
        case bd: BigDecimal => (bd / count).toDouble
        case jbd: java.math.BigDecimal =>
          BigDecimal(jbd)./(count).toDouble
        case _ => null
      }
    }

  private def minValue(values: Seq[Any]): Any =
    if (values.isEmpty) null
    else values.reduce { (a, b) => if (compareValues(a, b) <= 0) a else b }

  private def maxValue(values: Seq[Any]): Any =
    if (values.isEmpty) null
    else values.reduce { (a, b) => if (compareValues(a, b) >= 0) a else b }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def compareValues(a: Any, b: Any): Int =
    (a, b) match {
      case (null, null) => 0
      case (null, _)    => -1
      case (_, null)    => 1
      case (x: Comparable[_], y: Comparable[_]) =>
        x.asInstanceOf[Comparable[Any]].compareTo(y.asInstanceOf[Any])
      case _ => 0
    }

  private def executeSort(
    sort: SortOp,
    partitionId: Int,
    partitionCount: Int
  ): IO[Seq[Map[String, Any]]] =
    for {
      input <- executeOperator(sort.child, partitionId, partitionCount)
      schema = sort.child.outputSchema
      sorted = input.sortWith { (a, b) =>
        compareBySortKeys(a, b, sort.sortKeys, schema) < 0
      }
    } yield sorted

  private def compareBySortKeys(
    a: Map[String, Any],
    b: Map[String, Any],
    keys: Seq[SortKey],
    schema: Seq[String]
  ): Int = {
    keys.iterator.map { key =>
      val colName = schema.lift(key.columnIndex).getOrElse("")
      val av = a.getOrElse(colName, null)
      val bv = b.getOrElse(colName, null)

      if (av == null && bv == null) 0
      else if (av == null) if (key.nullsFirst) -1 else 1
      else if (bv == null) if (key.nullsFirst) 1 else -1
      else {
        val cmp = compareValues(av, bv)
        if (key.ascending) cmp else -cmp
      }
    }.find(_ != 0).getOrElse(0)
  }

  private def executeLimit(
    limit: LimitOp,
    partitionId: Int,
    partitionCount: Int
  ): IO[Seq[Map[String, Any]]] =
    for {
      input <- executeOperator(limit.child, partitionId, partitionCount)
      sliced = input.drop(limit.offset.toInt).take(limit.fetch.toInt)
    } yield sliced

  private def executeUnion(
    union: UnionOp,
    partitionId: Int,
    partitionCount: Int
  ): IO[Seq[Map[String, Any]]] =
    for {
      childResults <- union.inputs.foldLeft(IO.pure(Seq.empty[Map[String, Any]])) {
                        (accIO, child) =>
                          for {
                            acc  <- accIO
                            rows <- executeOperator(child, partitionId, partitionCount)
                          } yield acc ++ rows
                      }
      result = if (union.all) childResults else childResults.distinct
    } yield result

  // -----------------------------------------------------------------------
  // FilterExpression evaluation
  // -----------------------------------------------------------------------

  /** Evaluate a filter expression against a named-column row, returning true if the row passes. */
  private[execution] def evaluateFilterExpr(
    expr: FilterExpression,
    row: Map[String, Any],
    schema: Seq[String]
  ): Boolean =
    evaluateFilterValue(expr, row, schema) match {
      case b: Boolean              => b
      case java.lang.Boolean.TRUE  => true
      case java.lang.Boolean.FALSE => false
      case _                       => false
    }

  /** Evaluate a filter expression to a value. */
  private def evaluateFilterValue(
    expr: FilterExpression,
    row: Map[String, Any],
    schema: Seq[String]
  ): Any =
    expr match {
      case FilterExpression.ColumnRef(index) =>
        val colName = schema.lift(index).getOrElse("")
        row.getOrElse(colName, null)

      case FilterExpression.Literal(value, _) =>
        value.orNull

      case FilterExpression.Comparison(op, left, right) =>
        val lv = evaluateFilterValue(left, row, schema)
        val rv = evaluateFilterValue(right, row, schema)
        evaluateComparison(op, lv, rv)

      case FilterExpression.And(children) =>
        children.forall(c => evaluateFilterExpr(c, row, schema))

      case FilterExpression.Or(children) =>
        children.exists(c => evaluateFilterExpr(c, row, schema))

      case FilterExpression.Not(child) =>
        !evaluateFilterExpr(child, row, schema)

      case FilterExpression.IsNull(operand) =>
        evaluateFilterValue(operand, row, schema) == null

      case FilterExpression.IsNotNull(operand) =>
        evaluateFilterValue(operand, row, schema) != null

      case FilterExpression.In(operand, values) =>
        val ov = evaluateFilterValue(operand, row, schema)
        if (ov == null) false
        else {
          val coercedOv = coerceForComparison(ov)
          values.exists(v => coerceForComparison(evaluateFilterValue(v, row, schema)) == coercedOv)
        }

      case FilterExpression.Between(operand, lower, upper) =>
        val ov = evaluateFilterValue(operand, row, schema)
        val lv = evaluateFilterValue(lower, row, schema)
        val uv = evaluateFilterValue(upper, row, schema)
        if (ov == null || lv == null || uv == null) false
        else {
          val coercedOv = coerceForComparison(ov)
          compareValues(coercedOv, coerceForComparison(lv)) >= 0 &&
            compareValues(coercedOv, coerceForComparison(uv)) <= 0
        }

      case FilterExpression.Like(operand, pattern) =>
        val ov = evaluateFilterValue(operand, row, schema)
        val pv = evaluateFilterValue(pattern, row, schema)
        (ov, pv) match {
          case (s: String, p: String) =>
            val regex = ("^" + p.replace("%", ".*").replace("_", ".") + "$").r
            regex.findFirstIn(s).isDefined
          case _ => false
        }

      case FilterExpression.Cast(operand, _) =>
        evaluateFilterValue(operand, row, schema)
    }

  private def evaluateComparison(op: ComparisonOp, left: Any, right: Any): Boolean = {
    if (left == null || right == null) return false
    val cl = coerceForComparison(left)
    val cr = coerceForComparison(right)
    op match {
      case ComparisonOp.Eq              => cl == cr
      case ComparisonOp.NotEq           => cl != cr
      case ComparisonOp.LessThan        => compareValues(cl, cr) < 0
      case ComparisonOp.LessThanOrEq    => compareValues(cl, cr) <= 0
      case ComparisonOp.GreaterThan     => compareValues(cl, cr) > 0
      case ComparisonOp.GreaterThanOrEq => compareValues(cl, cr) >= 0
    }
  }

  /** Coerce string-encoded literal values to their numeric counterparts for comparison. */
  private def coerceForComparison(value: Any): Any =
    value match {
      case s: String =>
        try java.lang.Long.valueOf(s): Any
        catch { case _: NumberFormatException =>
          try java.lang.Double.valueOf(s): Any
          catch { case _: NumberFormatException => s }
        }
      case other => other
    }

  // -----------------------------------------------------------------------
  // Projection evaluation
  // -----------------------------------------------------------------------

  private def evaluateProjection(
    proj: Projection,
    row: Map[String, Any],
    schema: Seq[String]
  ): Any =
    proj match {
      case Projection.ColumnRef(index) =>
        val colName = schema.lift(index).getOrElse("")
        row.getOrElse(colName, null)

      case Projection.Literal(value, _) =>
        value.orNull

      case Projection.Expression(op, operands) =>
        val values = operands.map(o => evaluateProjection(o, row, schema))
        evaluateArithmetic(op, values)

      case Projection.Cast(operand, _) =>
        evaluateProjection(operand, row, schema)
    }

  private def evaluateArithmetic(op: ArithmeticOp, values: Seq[Any]): Any =
    op match {
      case ArithmeticOp.Negate =>
        values.headOption match {
          case Some(l: Long)   => -l
          case Some(d: Double) => -d
          case Some(i: Int)    => -i
          case _               => null
        }
      case _ if values.size >= 2 =>
        val left  = values.head
        val right = values(1)
        (left, right) match {
          case (null, _) | (_, null) => null
          case (a: Long, b: Long)     => applyLongOp(op, a, b)
          case (a: Int, b: Int)       => applyLongOp(op, a.toLong, b.toLong)
          case (a: Double, b: Double) => applyDoubleOp(op, a, b)
          case (a: Number, b: Number) => applyDoubleOp(op, a.doubleValue(), b.doubleValue())
          case _ => null
        }
      case _ => null
    }

  private def applyLongOp(op: ArithmeticOp, a: Long, b: Long): Any =
    op match {
      case ArithmeticOp.Add      => a + b
      case ArithmeticOp.Subtract => a - b
      case ArithmeticOp.Multiply => a * b
      case ArithmeticOp.Divide   => if (b == 0L) null else a / b
      case ArithmeticOp.Modulo   => if (b == 0L) null else a % b
      case ArithmeticOp.Negate   => -a
    }

  private def applyDoubleOp(op: ArithmeticOp, a: Double, b: Double): Any =
    op match {
      case ArithmeticOp.Add      => a + b
      case ArithmeticOp.Subtract => a - b
      case ArithmeticOp.Multiply => a * b
      case ArithmeticOp.Divide   => if (b == 0.0) null else a / b
      case ArithmeticOp.Modulo   => if (b == 0.0) null else a % b
      case ArithmeticOp.Negate   => -a
    }
}

object StageExecutor {

  /** Create a StageExecutor with provided data reader and dependency results. */
  def apply(
    dataReader: DataReader,
    dependencyResults: Map[Int, StageResult]
  ): StageExecutor =
    new StageExecutor(dataReader, dependencyResults)
}
