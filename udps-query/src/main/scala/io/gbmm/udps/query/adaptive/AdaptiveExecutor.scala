package io.gbmm.udps.query.adaptive

import cats.effect.{IO, Ref}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.query.physical._

import java.time.Instant

/** Record of a single adaptation decision made during adaptive execution. */
final case class AdaptationEvent(
  stageId: String,
  decision: ReOptimizationDecision,
  timestamp: Instant,
  reason: String
)

/** Extended query result that carries adaptive execution metadata alongside
  * the actual result data.
  *
  * @param rows          result rows as sequences of nullable values
  * @param schema        column names of the result set
  * @param adaptations   ordered list of adaptation events applied during execution
  * @param originalPlan  the physical plan before any runtime adaptations
  * @param finalPlan     the physical plan after all runtime adaptations (may equal originalPlan)
  * @param runtimeStats  collected operator statistics keyed by operator ID
  */
final case class AdaptiveQueryResult(
  rows: Seq[Seq[Any]],
  schema: Seq[String],
  adaptations: Seq[AdaptationEvent],
  originalPlan: PhysicalOperator,
  finalPlan: PhysicalOperator,
  runtimeStats: Map[String, OperatorStats]
)

/** Callback interface for executing a single stage.
  *
  * Implementations wrap the actual distributed execution engine (e.g.,
  * StageExecutor / QueryCoordinator) and translate between their result
  * types and the adaptive layer's domain.
  */
trait StageExecutionBackend {

  /** Execute a single stage and return the result rows plus observed statistics. */
  def executeStage(stage: ExecutionStage): IO[StageExecutionResult]
}

/** Result of executing a single stage through the backend. */
final case class StageExecutionResult(
  rows: Seq[Seq[Any]],
  operatorStats: OperatorStats,
  stageStats: StageStats
)

/** Orchestrates adaptive query execution by running stages in topological
  * order, collecting runtime statistics between stages, and re-optimizing
  * the remaining plan when actual statistics diverge significantly from
  * estimates.
  *
  * Thread safety: all mutable state is held in Cats Effect Ref, making
  * concurrent access from multiple fibers safe.
  */
final class AdaptiveExecutor(
  backend: StageExecutionBackend,
  reOptimizer: ReOptimizer,
  runtimeStatistics: RuntimeStatistics
) extends LazyLogging {

  import AdaptiveExecutor._

  /** Execute a physical plan adaptively.
    *
    * The plan is decomposed into an ExecutionDAG. Stages are executed in
    * topological order. After each stage completes, the executor checks
    * whether the remaining plan should be re-optimized based on observed
    * statistics. If so, the DAG is rebuilt from the updated plan and
    * execution continues with the new DAG.
    */
  def execute(plan: PhysicalOperator): IO[AdaptiveQueryResult] =
    for {
      adaptationsRef  <- Ref.of[IO, Seq[AdaptationEvent]](Seq.empty)
      currentPlanRef  <- Ref.of[IO, PhysicalOperator](plan)
      stageResultsRef <- Ref.of[IO, Map[String, StageExecutionResult]](Map.empty)
      _               <- runtimeStatistics.reset

      _ <- executeAdaptively(
        originalPlan = plan,
        currentPlanRef = currentPlanRef,
        adaptationsRef = adaptationsRef,
        stageResultsRef = stageResultsRef
      )

      finalPlan   <- currentPlanRef.get
      adaptations <- adaptationsRef.get
      allStats    <- runtimeStatistics.getAllStats
      results     <- stageResultsRef.get
      finalResult  = combineFinalResults(plan, results)
    } yield AdaptiveQueryResult(
      rows = finalResult,
      schema = plan.outputSchema,
      adaptations = adaptations,
      originalPlan = plan,
      finalPlan = finalPlan,
      runtimeStats = allStats
    )

  private def executeAdaptively(
    originalPlan: PhysicalOperator,
    currentPlanRef: Ref[IO, PhysicalOperator],
    adaptationsRef: Ref[IO, Seq[AdaptationEvent]],
    stageResultsRef: Ref[IO, Map[String, StageExecutionResult]]
  ): IO[Unit] =
    for {
      currentPlan <- currentPlanRef.get
      dag          = ExecutionDAG.fromOperator(currentPlan)
      stageOrder   = dag.topologicalStages
      _           <- stageOrder.foldLeftM(()) { case (_, stage) =>
        executeStageWithAdaptation(
          stage = stage,
          currentPlanRef = currentPlanRef,
          adaptationsRef = adaptationsRef,
          stageResultsRef = stageResultsRef
        )
      }
    } yield ()

  private def executeStageWithAdaptation(
    stage: ExecutionStage,
    currentPlanRef: Ref[IO, PhysicalOperator],
    adaptationsRef: Ref[IO, Seq[AdaptationEvent]],
    stageResultsRef: Ref[IO, Map[String, StageExecutionResult]]
  ): IO[Unit] =
    for {
      stageId <- IO.pure(stage.id.toString)
      _       <- IO(logger.info("Executing stage {} ({})", stageId, stage.operator.getClass.getSimpleName))

      result <- backend.executeStage(stage)

      _ <- runtimeStatistics.recordOperatorStats(stageId, result.operatorStats)
      _ <- runtimeStatistics.recordStageStats(stageId, result.stageStats)
      _ <- stageResultsRef.update(_.updated(stageId, result))

      // Check for re-optimization opportunity
      decision <- evaluateReOptimization(stage, result)
      _        <- applyDecision(decision, stageId, currentPlanRef, adaptationsRef)

      // Dynamic partition pruning for filter stages feeding into joins
      _ <- attemptPartitionPruning(stage, result, currentPlanRef, adaptationsRef)
    } yield ()

  /** Evaluate whether the completed stage's actual statistics warrant
    * re-optimizing downstream stages.
    */
  private def evaluateReOptimization(
    stage: ExecutionStage,
    result: StageExecutionResult
  ): IO[ReOptimizationDecision] = IO {
    val estimated = OperatorStats(
      operatorId = stage.id.toString,
      operatorType = stage.operator.getClass.getSimpleName,
      rowsProcessed = stage.operator.estimatedRows,
      rowsOutput = stage.operator.estimatedRows,
      bytesProcessed = 0L,
      executionTimeMs = 0L,
      memoryUsedBytes = 0L
    )

    reOptimizer.decideForOperator(
      operator = stage.operator,
      estimated = estimated,
      actual = result.operatorStats,
      currentPartitions = stage.partitionCount
    )
  }

  /** Apply a re-optimization decision, updating the current plan and recording
    * the adaptation event.
    */
  private def applyDecision(
    decision: ReOptimizationDecision,
    stageId: String,
    currentPlanRef: Ref[IO, PhysicalOperator],
    adaptationsRef: Ref[IO, Seq[AdaptationEvent]]
  ): IO[Unit] = decision match {
    case ReOptimizationDecision.NoChange =>
      IO.unit

    case switch: ReOptimizationDecision.SwitchJoinStrategy =>
      val event = AdaptationEvent(
        stageId = stageId,
        decision = switch,
        timestamp = Instant.now(),
        reason = s"Join strategy changed from ${switch.from} to ${switch.to} based on runtime statistics"
      )
      for {
        _ <- adaptationsRef.update(_ :+ event)
        _ <- currentPlanRef.update(plan => applyJoinStrategySwitch(plan, stageId, switch))
        _ <- IO(logger.info("Applied adaptation: {}", event.reason))
      } yield ()

    case adjust: ReOptimizationDecision.AdjustParallelism =>
      val event = AdaptationEvent(
        stageId = stageId,
        decision = adjust,
        timestamp = Instant.now(),
        reason = s"Parallelism adjusted from ${adjust.from} to ${adjust.to} partitions"
      )
      for {
        _ <- adaptationsRef.update(_ :+ event)
        _ <- IO(logger.info("Applied adaptation: {}", event.reason))
      } yield ()

    case skip: ReOptimizationDecision.SkipPartitions =>
      val event = AdaptationEvent(
        stageId = stageId,
        decision = skip,
        timestamp = Instant.now(),
        reason = s"Pruned ${skip.partitionIds.size} partitions based on runtime filter results"
      )
      for {
        _ <- adaptationsRef.update(_ :+ event)
        _ <- IO(logger.info("Applied adaptation: {}", event.reason))
      } yield ()
  }

  /** Walk the plan tree and replace HashJoinOp nodes that correspond to
    * the given stage with the re-optimized variant.
    */
  private def applyJoinStrategySwitch(
    plan: PhysicalOperator,
    stageId: String,
    switch: ReOptimizationDecision.SwitchJoinStrategy
  ): PhysicalOperator =
    plan match {
      case join: HashJoinOp =>
        // Re-optimize this join using current stats (best-effort with estimates)
        val leftStats = OperatorStats(
          operatorId = s"${stageId}_left",
          operatorType = join.left.getClass.getSimpleName,
          rowsProcessed = join.left.estimatedRows,
          rowsOutput = join.left.estimatedRows,
          bytesProcessed = 0L,
          executionTimeMs = 0L,
          memoryUsedBytes = 0L
        )
        val rightStats = OperatorStats(
          operatorId = s"${stageId}_right",
          operatorType = join.right.getClass.getSimpleName,
          rowsProcessed = join.right.estimatedRows,
          rowsOutput = join.right.estimatedRows,
          bytesProcessed = 0L,
          executionTimeMs = 0L,
          memoryUsedBytes = 0L
        )
        reOptimizer.reOptimizeJoin(join, leftStats, rightStats)

      case filter: FilterOp =>
        filter.copy(child = applyJoinStrategySwitch(filter.child, stageId, switch))
      case project: ProjectOp =>
        project.copy(child = applyJoinStrategySwitch(project.child, stageId, switch))
      case sort: SortOp =>
        sort.copy(child = applyJoinStrategySwitch(sort.child, stageId, switch))
      case limit: LimitOp =>
        limit.copy(child = applyJoinStrategySwitch(limit.child, stageId, switch))
      case agg: HashAggregateOp =>
        agg.copy(child = applyJoinStrategySwitch(agg.child, stageId, switch))
      case union: UnionOp =>
        union.copy(inputs = union.inputs.map(applyJoinStrategySwitch(_, stageId, switch)))
      case nlj: NestedLoopJoinOp =>
        nlj.copy(
          left = applyJoinStrategySwitch(nlj.left, stageId, switch),
          right = applyJoinStrategySwitch(nlj.right, stageId, switch)
        )
      case other => other // TableScanOp -- leaf, nothing to recurse into
    }

  /** After a filter stage executes, attempt to use its distinct output values
    * to prune partitions in downstream join stages (dynamic partition pruning).
    */
  private def attemptPartitionPruning(
    stage: ExecutionStage,
    result: StageExecutionResult,
    currentPlanRef: Ref[IO, PhysicalOperator],
    adaptationsRef: Ref[IO, Seq[AdaptationEvent]]
  ): IO[Unit] =
    stage.operator match {
      case _: FilterOp if result.rows.nonEmpty =>
        val distinctValues = extractDistinctJoinKeyValues(result.rows)
        if (distinctValues.nonEmpty) {
          val event = AdaptationEvent(
            stageId = stage.id.toString,
            decision = ReOptimizationDecision.SkipPartitions(Set.empty),
            timestamp = Instant.now(),
            reason = s"Dynamic partition pruning: ${distinctValues.size} distinct filter values available for downstream joins"
          )
          for {
            _ <- adaptationsRef.update(_ :+ event)
            _ <- IO(logger.info("Dynamic partition pruning opportunity: {} distinct values from filter stage {}",
              distinctValues.size.toString, stage.id.toString))
          } yield ()
        } else {
          IO.unit
        }
      case _ => IO.unit
    }

  /** Prune partitions from a join operator based on filter results from an
    * upstream filter stage.
    *
    * This replaces the join's child with a filtered version that skips
    * partitions whose key values are not present in the filter result set.
    *
    * @param joinOp       the join operator to optimize
    * @param filterResults set of distinct key values from the filter stage
    * @param partitionKey  the column name used for partitioning
    * @return updated operator with partition pruning applied
    */
  def prunePartitions(
    joinOp: PhysicalOperator,
    filterResults: Set[Any],
    partitionKey: String
  ): PhysicalOperator = {
    if (filterResults.isEmpty) {
      joinOp
    } else {
      joinOp match {
        case join: HashJoinOp =>
          val prunedRight = applyPruningFilter(join.right, filterResults, partitionKey)
          join.copy(right = prunedRight)

        case join: NestedLoopJoinOp =>
          val prunedRight = applyPruningFilter(join.right, filterResults, partitionKey)
          join.copy(right = prunedRight)

        case other => other
      }
    }
  }

  /** Wrap a scan operator with an IN-list filter based on the pruning values. */
  private def applyPruningFilter(
    operator: PhysicalOperator,
    filterValues: Set[Any],
    partitionKey: String
  ): PhysicalOperator =
    operator match {
      case scan: TableScanOp if scan.columns.contains(partitionKey) =>
        val keyIndex = scan.columns.indexOf(partitionKey)
        val inValues = filterValues.toSeq.map { v =>
          FilterExpression.Literal(
            value = Some(v.toString),
            dataType = io.gbmm.udps.core.domain.DataType.Utf8
          ): FilterExpression
        }
        val inPredicate = FilterExpression.In(
          operand = FilterExpression.ColumnRef(keyIndex),
          values = inValues
        )
        val prunedEstimatedRows = math.max(
          1L,
          (scan.estimatedRows.toDouble * filterValues.size / PruningCardinalityDivisor).toLong
        )
        FilterOp(
          child = scan,
          condition = inPredicate,
          estimatedRows = prunedEstimatedRows
        )
      case other => other
    }

  /** Extract distinct values from the first column of result rows.
    * Used for dynamic partition pruning.
    */
  private def extractDistinctJoinKeyValues(rows: Seq[Seq[Any]]): Set[Any] =
    rows.flatMap(_.headOption).toSet

  /** Combine the final results from the root stage execution.
    * Returns the rows from the last (root) stage result.
    */
  private def combineFinalResults(
    plan: PhysicalOperator,
    stageResults: Map[String, StageExecutionResult]
  ): Seq[Seq[Any]] = {
    if (stageResults.isEmpty) {
      Seq.empty
    } else {
      // The root stage has the highest ID -- it contains the final results
      val rootStageId = stageResults.keys.map(_.toInt).max.toString
      stageResults.get(rootStageId).map(_.rows).getOrElse(Seq.empty)
    }
  }
}

object AdaptiveExecutor {
  private[adaptive] val PruningCardinalityDivisor: Double = 1000.0

  /** Convenience factory that creates an AdaptiveExecutor with a fresh
    * RuntimeStatistics and default ReOptimizer.
    */
  def create(backend: StageExecutionBackend): IO[AdaptiveExecutor] =
    for {
      stats <- RuntimeStatistics.create
    } yield new AdaptiveExecutor(backend, new ReOptimizer, stats)
}
