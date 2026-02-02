package io.gbmm.udps.query.execution

import cats.effect.{Clock, IO, Ref}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.query.physical.{ExecutionDAG, ExecutionStage}

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

/** Status of an individual stage within a query execution. */
sealed trait StageStatus extends Product with Serializable

object StageStatus {
  case object Pending extends StageStatus
  final case class Running(startTimeMs: Long, workerId: String) extends StageStatus
  final case class Completed(executionTimeMs: Long) extends StageStatus
  final case class Failed(error: String, attempts: Int) extends StageStatus
}

/** Tracked state of a query execution managed by the coordinator. */
final case class QueryExecution(
  queryId: String,
  dag: ExecutionDAG,
  stageStatuses: Map[Int, StageStatus],
  stageResults: Map[Int, StageResult],
  startTimeMs: Long,
  endTimeMs: Option[Long]
) {

  /** Check whether all stages have completed successfully. */
  def isComplete: Boolean =
    stageStatuses.values.forall {
      case StageStatus.Completed(_) => true
      case _                        => false
    }

  /** Check whether any stage has permanently failed (exhausted retries). */
  def hasFailed: Boolean =
    stageStatuses.values.exists {
      case StageStatus.Failed(_, attempts) => attempts >= QueryCoordinator.MAX_STAGE_RETRIES
      case _                               => false
    }

  /** Return the first permanent failure error message, if any. */
  def failureReason: Option[String] =
    stageStatuses.collectFirst {
      case (stageId, StageStatus.Failed(error, attempts))
        if attempts >= QueryCoordinator.MAX_STAGE_RETRIES =>
        s"Stage '$stageId' failed after $attempts attempts: $error"
    }
}

/** Represents a worker node that can execute stages. */
trait WorkerRef {

  /** Unique identifier for this worker. */
  def workerId: String

  /** Execute a stage on this worker for a specific partition.
    *
    * @param stage       the execution stage
    * @param partitionId the partition to process
    * @param dependencies results from dependency stages keyed by stage ID
    * @return the stage result
    */
  def executeStage(
    stage: ExecutionStage,
    partitionId: Int,
    dependencies: Map[Int, StageResult]
  ): IO[StageResult]
}

/** Configuration for the query coordinator. */
final case class CoordinatorConfig(
  queryTimeout: FiniteDuration,
  stageTimeout: FiniteDuration,
  maxConcurrentStages: Int
)

object CoordinatorConfig {
  private val DEFAULT_QUERY_TIMEOUT_MINUTES = 5
  private val DEFAULT_STAGE_TIMEOUT_MINUTES = 2
  private val DEFAULT_MAX_CONCURRENT_STAGES = 8

  val default: CoordinatorConfig = CoordinatorConfig(
    queryTimeout = DEFAULT_QUERY_TIMEOUT_MINUTES.minutes,
    stageTimeout = DEFAULT_STAGE_TIMEOUT_MINUTES.minutes,
    maxConcurrentStages = DEFAULT_MAX_CONCURRENT_STAGES
  )
}

/** Coordinates distributed query execution across a set of worker nodes.
  *
  * The coordinator takes an [[ExecutionDAG]], schedules stages in topological order,
  * distributes partition-level work to workers, collects results, and handles
  * failures with configurable retry logic.
  *
  * All coordination logic uses Cats Effect IO for composable async operations.
  *
  * @param workers pool of available worker nodes
  * @param config  coordinator configuration
  */
final class QueryCoordinator(
  workers: Seq[WorkerRef],
  config: CoordinatorConfig
) extends LazyLogging {

  /** Execute a full query represented by an ExecutionDAG.
    *
    * Stages are scheduled in topological order, respecting data dependencies.
    * Each stage is partitioned and its partitions executed across available workers.
    * Results are combined per-stage before downstream stages consume them.
    *
    * @param dag the execution DAG to execute
    * @return the final query execution state
    */
  def executeQuery(dag: ExecutionDAG): IO[QueryExecution] = {
    val queryId = UUID.randomUUID().toString

    for {
      startNanos <- Clock[IO].monotonic.map(_.toNanos)
      startMs     = TimeUnit.NANOSECONDS.toMillis(startNanos)

      initialStatuses = dag.stages.map { case (id, _) => id -> (StageStatus.Pending: StageStatus) }
      stateRef <- Ref.of[IO, QueryExecution](QueryExecution(
        queryId = queryId,
        dag = dag,
        stageStatuses = initialStatuses,
        stageResults = Map.empty,
        startTimeMs = startMs,
        endTimeMs = None
      ))

      _ <- IO(logger.info("Starting query {} with {} stages", queryId, dag.size.toString))

      topOrder = dag.topologicalOrder
      _ <- executeStagesInOrder(topOrder, stateRef)

      endNanos <- Clock[IO].monotonic.map(_.toNanos)
      endMs     = TimeUnit.NANOSECONDS.toMillis(endNanos)

      finalState <- stateRef.updateAndGet(_.copy(endTimeMs = Some(endMs)))
      totalMs     = endMs - startMs

      _ <- IO(logger.info(
        "Query {} completed in {}ms, status: {}",
        queryId,
        totalMs.toString,
        if (finalState.isComplete) "COMPLETED"
        else if (finalState.hasFailed) "FAILED"
        else "PARTIAL"
      ))
    } yield finalState
  }

  private def executeStagesInOrder(
    topOrder: Seq[Int],
    stateRef: Ref[IO, QueryExecution]
  ): IO[Unit] =
    topOrder.foldLeft(IO.unit) { (prevIO, stageId) =>
      prevIO.flatMap { _ =>
        stateRef.get.flatMap { state =>
          if (state.hasFailed) {
            IO(logger.warn("Skipping stage {} due to prior failure", stageId.toString))
          } else {
            state.dag.stages.get(stageId) match {
              case Some(stage) => executeStageWithRetry(stage, stateRef, retryCount = 0)
              case None =>
                IO.raiseError(new IllegalStateException(
                  s"Stage '$stageId' not found in execution DAG"
                ))
            }
          }
        }
      }
    }

  private def executeStageWithRetry(
    stage: ExecutionStage,
    stateRef: Ref[IO, QueryExecution],
    retryCount: Int
  ): IO[Unit] = {
    val workerIdx = retryCount % workers.size
    val worker = workers(workerIdx)

    for {
      nowNanos <- Clock[IO].monotonic.map(_.toNanos)
      nowMs     = TimeUnit.NANOSECONDS.toMillis(nowNanos)

      _ <- stateRef.update(s => s.copy(
        stageStatuses = s.stageStatuses.updated(
          stage.id,
          StageStatus.Running(nowMs, worker.workerId)
        )
      ))

      _ <- IO(logger.debug(
        "Executing stage {} on worker {} (attempt {})",
        stage.id.toString, worker.workerId, (retryCount + 1).toString
      ))

      currentState <- stateRef.get
      dependencies  = stage.dependencies.flatMap(depId =>
                        currentState.stageResults.get(depId).map(depId -> _)
                      ).toMap

      result <- executeStagePartitions(stage, worker, dependencies)
                  .timeout(config.stageTimeout)
                  .handleErrorWith { error =>
                    handleStageFailure(stage, stateRef, retryCount, error)
                  }

      _ <- stateRef.update(s => s.copy(
        stageStatuses = s.stageStatuses.updated(
          stage.id,
          StageStatus.Completed(result.executionTimeMs)
        ),
        stageResults = s.stageResults.updated(stage.id, result)
      ))

      _ <- IO(logger.debug(
        "Stage {} completed: {} rows in {}ms",
        stage.id.toString,
        result.rowCount.toString,
        result.executionTimeMs.toString
      ))
    } yield ()
  }

  private def executeStagePartitions(
    stage: ExecutionStage,
    worker: WorkerRef,
    dependencies: Map[Int, StageResult]
  ): IO[StageResult] = {
    val partitionIds = (0 until stage.partitionCount).toList

    for {
      startNanos <- Clock[IO].monotonic.map(_.toNanos)

      partitionResults <- partitionIds.parTraverse { partitionId =>
        worker.executeStage(stage, partitionId, dependencies)
      }

      endNanos <- Clock[IO].monotonic.map(_.toNanos)
      elapsedMs = TimeUnit.NANOSECONDS.toMillis(endNanos - startNanos)

      combinedRows = partitionResults.flatMap(_.rows)
      combinedResult = StageResult(
        stageId = stage.id,
        partitionId = 0,
        rows = combinedRows,
        rowCount = combinedRows.size.toLong,
        executionTimeMs = elapsedMs
      )
    } yield combinedResult
  }

  private def handleStageFailure(
    stage: ExecutionStage,
    stateRef: Ref[IO, QueryExecution],
    retryCount: Int,
    error: Throwable
  ): IO[StageResult] = {
    val nextRetry = retryCount + 1

    if (nextRetry >= QueryCoordinator.MAX_STAGE_RETRIES) {
      val errorMsg = s"${error.getClass.getSimpleName}: ${error.getMessage}"
      for {
        _ <- stateRef.update(s => s.copy(
          stageStatuses = s.stageStatuses.updated(
            stage.id,
            StageStatus.Failed(errorMsg, nextRetry)
          )
        ))
        _ <- IO(logger.error(
          "Stage {} permanently failed after {} attempts: {}",
          stage.id.toString,
          nextRetry.toString,
          errorMsg
        ))
        result <- IO.raiseError[StageResult](
          new StageExecutionException(stage.id, errorMsg, nextRetry)
        )
      } yield result
    } else {
      IO(logger.warn(
        "Stage {} failed on attempt {}, retrying: {}",
        stage.id.toString,
        nextRetry.toString,
        error.getMessage
      )).flatMap { _ =>
        executeStageWithRetry(stage, stateRef, nextRetry).flatMap { _ =>
          stateRef.get.map(_.stageResults.getOrElse(
            stage.id,
            StageResult(stage.id, 0, Seq.empty, 0L, 0L)
          ))
        }
      }
    }
  }
}

/** Thrown when a stage exhausts its retry budget. */
final class StageExecutionException(
  val stageId: Int,
  val reason: String,
  val attempts: Int
) extends RuntimeException(
  s"Stage '$stageId' failed after $attempts attempts: $reason"
)

object QueryCoordinator {

  val MAX_STAGE_RETRIES: Int = 3

  /** Create a QueryCoordinator with the given workers and default configuration. */
  def apply(workers: Seq[WorkerRef]): QueryCoordinator =
    new QueryCoordinator(workers, CoordinatorConfig.default)

  /** Create a QueryCoordinator with the given workers and explicit configuration. */
  def apply(workers: Seq[WorkerRef], config: CoordinatorConfig): QueryCoordinator =
    new QueryCoordinator(workers, config)
}
