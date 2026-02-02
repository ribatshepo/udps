package io.gbmm.udps.query.execution

import cats.effect.{Clock, IO, Ref}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.query.physical._

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

/** Execution mode for the distributed executor. */
sealed trait ExecutionMode extends Product with Serializable

object ExecutionMode {

  /** Execute on the local JVM using a single StageExecutor. */
  case object Local extends ExecutionMode

  /** Distribute execution across a cluster of workers. */
  case object Distributed extends ExecutionMode
}

/** Result of a query execution. */
final case class QueryResult(
  queryId: String,
  columns: Seq[ColumnDescriptor],
  rows: Seq[Map[String, Any]],
  rowCount: Long,
  executionTimeMs: Long,
  stagesExecuted: Int
)

/** Describes a result column with name and type information. */
final case class ColumnDescriptor(
  name: String,
  typeName: String
)

/** Provides access to worker nodes for distributed execution.
  *
  * Implementations wire into the cluster membership layer (e.g. Akka Cluster)
  * to discover and communicate with remote workers.
  */
trait WorkerPool {

  /** Return all currently available workers. */
  def availableWorkers: IO[Seq[WorkerRef]]

  /** Return the number of currently available workers. */
  def workerCount: IO[Int]
}

/** Configuration for the distributed executor. */
final case class ExecutorConfig(
  mode: ExecutionMode,
  defaultPartitionCount: Int,
  queryTimeout: FiniteDuration,
  coordinatorConfig: CoordinatorConfig
)

object ExecutorConfig {
  private val DEFAULT_PARTITION_COUNT = 4
  private val DEFAULT_QUERY_TIMEOUT_MINUTES = 5

  val local: ExecutorConfig = ExecutorConfig(
    mode = ExecutionMode.Local,
    defaultPartitionCount = DEFAULT_PARTITION_COUNT,
    queryTimeout = DEFAULT_QUERY_TIMEOUT_MINUTES.minutes,
    coordinatorConfig = CoordinatorConfig.default
  )

  val distributed: ExecutorConfig = ExecutorConfig(
    mode = ExecutionMode.Distributed,
    defaultPartitionCount = DEFAULT_PARTITION_COUNT,
    queryTimeout = DEFAULT_QUERY_TIMEOUT_MINUTES.minutes,
    coordinatorConfig = CoordinatorConfig.default
  )
}

/** Top-level entry point for query execution.
  *
  * Supports both local (single-JVM) and distributed (cluster) execution modes.
  * In local mode, stages are executed directly via [[StageExecutor]]. In distributed
  * mode, an [[ExecutionDAG]] is constructed and execution is coordinated via
  * [[QueryCoordinator]].
  *
  * @param dataReader  storage-backed data reader for table scans
  * @param workerPool  pool of remote workers (used in distributed mode)
  * @param config      execution configuration
  */
final class DistributedExecutor(
  dataReader: DataReader,
  workerPool: Option[WorkerPool],
  config: ExecutorConfig
) extends LazyLogging {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private val shuffleExchange: ShuffleExchange = ShuffleExchange()

  /** Execute a physical operator tree and return the query result.
    *
    * For single-node mode, the operator is executed locally. For distributed mode,
    * the operator is decomposed into an ExecutionDAG and distributed to workers.
    *
    * @param operator the root physical operator to execute
    * @return the final query result
    */
  def execute(operator: PhysicalOperator): IO[QueryResult] =
    config.mode match {
      case ExecutionMode.Local       => executeLocal(operator)
      case ExecutionMode.Distributed => executeDistributed(operator)
    }

  /** Execute an already-constructed ExecutionDAG.
    *
    * @param dag the execution DAG
    * @return the final query result
    */
  def executeDAG(dag: ExecutionDAG): IO[QueryResult] =
    config.mode match {
      case ExecutionMode.Local       => executeDAGLocal(dag)
      case ExecutionMode.Distributed => executeDAGDistributed(dag)
    }

  /** Access the shuffle exchange for external coordination. */
  def shuffle: ShuffleExchange = shuffleExchange

  private def executeLocal(operator: PhysicalOperator): IO[QueryResult] = {
    val queryId = UUID.randomUUID().toString
    val stageId = 0
    val stage = ExecutionStage(
      id = stageId,
      operator = operator,
      dependencies = Set.empty,
      partitionCount = 1
    )
    val executor = StageExecutor(dataReader, Map.empty)

    for {
      startNanos <- Clock[IO].monotonic.map(_.toNanos)
      result     <- executor.execute(stage, partitionId = 0)
                      .timeout(config.queryTimeout)
      endNanos   <- Clock[IO].monotonic.map(_.toNanos)
      elapsedMs   = TimeUnit.NANOSECONDS.toMillis(endNanos - startNanos)
      columns     = inferColumns(result.rows, operator.outputSchema)
    } yield QueryResult(
      queryId = queryId,
      columns = columns,
      rows = result.rows,
      rowCount = result.rowCount,
      executionTimeMs = elapsedMs,
      stagesExecuted = 1
    )
  }

  private def executeDistributed(operator: PhysicalOperator): IO[QueryResult] = {
    val dag = ExecutionDAG.fromOperator(operator)
    executeDAGDistributed(dag)
  }

  private def executeDAGLocal(dag: ExecutionDAG): IO[QueryResult] = {
    val queryId = UUID.randomUUID().toString
    val topOrder = dag.topologicalOrder

    for {
      startNanos   <- Clock[IO].monotonic.map(_.toNanos)
      resultsRef   <- Ref.of[IO, Map[Int, StageResult]](Map.empty)
      _            <- topOrder.foldLeft(IO.unit) { (prevIO, stageId) =>
                        prevIO.flatMap { _ =>
                          dag.stages.get(stageId) match {
                            case Some(stage) =>
                              for {
                                currentResults <- resultsRef.get
                                depResults      = stage.dependencies.flatMap(d =>
                                                    currentResults.get(d).map(d -> _)
                                                  ).toMap
                                executor        = StageExecutor(dataReader, depResults)
                                partitionResults <- (0 until stage.partitionCount).toList
                                                      .traverse(pid => executor.execute(stage, pid))
                                combined         = combinePartitionResults(stageId, partitionResults)
                                _               <- resultsRef.update(_ + (stageId -> combined))
                              } yield ()
                            case None =>
                              IO.raiseError(new IllegalStateException(
                                s"Stage '$stageId' referenced in topological order but not in DAG"
                              ))
                          }
                        }
                      }
      allResults   <- resultsRef.get
      endNanos     <- Clock[IO].monotonic.map(_.toNanos)
      elapsedMs     = TimeUnit.NANOSECONDS.toMillis(endNanos - startNanos)

      rootStage     = dag.rootStage
      finalRows     = allResults.get(rootStage.id).map(_.rows).getOrElse(Seq.empty)
      columns       = inferColumns(finalRows, rootStage.operator.outputSchema)
    } yield QueryResult(
      queryId = queryId,
      columns = columns,
      rows = finalRows,
      rowCount = finalRows.size.toLong,
      executionTimeMs = elapsedMs,
      stagesExecuted = dag.size
    )
  }

  private def executeDAGDistributed(dag: ExecutionDAG): IO[QueryResult] = {
    val queryId = UUID.randomUUID().toString

    for {
      pool    <- workerPool match {
                   case Some(wp) => wp.availableWorkers
                   case None     =>
                     IO.raiseError[Seq[WorkerRef]](new IllegalStateException(
                       "Distributed execution requires a WorkerPool, but none was configured"
                     ))
                 }
      _       <- IO.whenA(pool.isEmpty)(
                   IO.raiseError(new IllegalStateException(
                     "No workers available in the WorkerPool for distributed execution"
                   ))
                 )

      coordinator = QueryCoordinator(pool, config.coordinatorConfig)

      startNanos <- Clock[IO].monotonic.map(_.toNanos)
      execution  <- coordinator.executeQuery(dag)
                      .timeout(config.queryTimeout)
      endNanos   <- Clock[IO].monotonic.map(_.toNanos)
      elapsedMs   = TimeUnit.NANOSECONDS.toMillis(endNanos - startNanos)

      _ <- IO.whenA(execution.hasFailed)(
             IO.raiseError(new QueryExecutionException(
               queryId,
               execution.failureReason.getOrElse("Unknown failure")
             ))
           )

      rootStage   = dag.rootStage
      finalRows   = execution.stageResults.get(rootStage.id).map(_.rows).getOrElse(Seq.empty)
      columns     = inferColumns(finalRows, rootStage.operator.outputSchema)
    } yield QueryResult(
      queryId = queryId,
      columns = columns,
      rows = finalRows,
      rowCount = finalRows.size.toLong,
      executionTimeMs = elapsedMs,
      stagesExecuted = dag.size
    )
  }

  private def combinePartitionResults(
    stageId: Int,
    partitionResults: List[StageResult]
  ): StageResult = {
    val allRows = partitionResults.flatMap(_.rows)
    val totalTime = partitionResults.map(_.executionTimeMs).maxOption.getOrElse(0L)
    StageResult(
      stageId = stageId,
      partitionId = 0,
      rows = allRows,
      rowCount = allRows.size.toLong,
      executionTimeMs = totalTime
    )
  }

  /** Infer column descriptors from the operator output schema and first result row. */
  private def inferColumns(
    rows: Seq[Map[String, Any]],
    outputSchema: Seq[String]
  ): Seq[ColumnDescriptor] =
    rows.headOption match {
      case Some(row) =>
        outputSchema.map { colName =>
          ColumnDescriptor(colName, inferTypeName(row.getOrElse(colName, null)))
        }
      case None =>
        outputSchema.map(colName => ColumnDescriptor(colName, "NULL"))
    }

  private def inferTypeName(value: Any): String =
    value match {
      case null                        => "NULL"
      case _: Boolean                  => "BOOLEAN"
      case _: Byte                     => "INT8"
      case _: Short                    => "INT16"
      case _: Int                      => "INT32"
      case _: Long                     => "INT64"
      case _: Float                    => "FLOAT32"
      case _: Double                   => "FLOAT64"
      case _: String                   => "UTF8"
      case _: BigDecimal               => "DECIMAL"
      case _: java.math.BigDecimal     => "DECIMAL"
      case _: java.time.LocalDate      => "DATE32"
      case _: java.time.Instant        => "TIMESTAMP"
      case _: Array[Byte]              => "BINARY"
      case _                           => "ANY"
    }
}

/** Thrown when a distributed query execution fails. */
final class QueryExecutionException(
  val queryId: String,
  val reason: String
) extends RuntimeException(
  s"Query '$queryId' failed: $reason"
)

object DistributedExecutor {

  /** Create a local-mode executor with no worker pool. */
  def local(dataReader: DataReader): DistributedExecutor =
    new DistributedExecutor(dataReader, None, ExecutorConfig.local)

  /** Create a local-mode executor with custom configuration. */
  def local(dataReader: DataReader, config: ExecutorConfig): DistributedExecutor =
    new DistributedExecutor(dataReader, None, config.copy(mode = ExecutionMode.Local))

  /** Create a distributed-mode executor with a worker pool. */
  def distributed(
    dataReader: DataReader,
    workerPool: WorkerPool
  ): DistributedExecutor =
    new DistributedExecutor(dataReader, Some(workerPool), ExecutorConfig.distributed)

  /** Create a distributed-mode executor with a worker pool and custom configuration. */
  def distributed(
    dataReader: DataReader,
    workerPool: WorkerPool,
    config: ExecutorConfig
  ): DistributedExecutor =
    new DistributedExecutor(dataReader, Some(workerPool), config.copy(mode = ExecutionMode.Distributed))
}
