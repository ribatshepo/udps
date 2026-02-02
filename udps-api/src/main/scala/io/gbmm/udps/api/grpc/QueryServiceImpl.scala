package io.gbmm.udps.api.grpc

import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.query.execution.{DistributedExecutor, QueryExecutionException}
import io.gbmm.udps.query.execution.{QueryResult => DomainQueryResult}
import io.gbmm.udps.query.optimizer.{CostBasedOptimizer, LogicalOptimizer}
import io.gbmm.udps.query.parser.SQLParser
import io.gbmm.udps.query.physical.PhysicalPlanner
import io.grpc.{Status, StatusRuntimeException}
import udps.api.query_service.QueryServiceGrpc
import udps.api.query_service.{
  AnalyzeQueryRequest,
  ColumnDescriptor => ProtoColumnDescriptor,
  ExecuteQueryRequest,
  ExplainQueryRequest,
  GetQueryStatusRequest,
  QueryAnalysis,
  QueryPlan,
  QueryResult => ProtoQueryResult,
  QueryResultRow,
  QueryStatus
}

import scala.concurrent.Future

final class QueryServiceImpl(
    sqlParser: SQLParser,
    logicalOptimizer: LogicalOptimizer,
    costBasedOptimizer: CostBasedOptimizer,
    physicalPlanner: PhysicalPlanner,
    executor: DistributedExecutor,
    ioRuntime: IORuntime
) extends QueryServiceGrpc.QueryService
    with LazyLogging {

  private[this] implicit val runtime: IORuntime = ioRuntime

  override def executeQuery(request: ExecuteQueryRequest): Future[ProtoQueryResult] =
    runIO("ExecuteQuery") {
      for {
        _           <- validateSQL(request.sql)
        parseResult <- parseSql(request.sql)
        logicalOpt  <- logicalOptimizer.optimize(parseResult.sqlNode)
        costOpt     <- IO.delay(costBasedOptimizer.optimize(logicalOpt.optimizedPlan))
        physical     = physicalPlanner.plan(costOpt.optimizedPlan)
        result      <- executor.execute(physical)
      } yield toProtoQueryResult(result, request.maxRows)
    }

  override def explainQuery(request: ExplainQueryRequest): Future[QueryPlan] =
    runIO("ExplainQuery") {
      for {
        _           <- validateSQL(request.sql)
        parseResult <- parseSql(request.sql)
        logicalOpt  <- logicalOptimizer.optimize(parseResult.sqlNode)
        costOpt     <- IO.delay(costBasedOptimizer.optimize(logicalOpt.optimizedPlan))
        physical     = physicalPlanner.plan(costOpt.optimizedPlan)
        queryId      = UUID.randomUUID().toString
      } yield QueryPlan(
        queryId = queryId,
        logicalPlan = org.apache.calcite.plan.RelOptUtil.toString(logicalOpt.optimizedPlan),
        physicalPlan = physical.toString,
        estimatedCost = costOpt.estimatedCost.getIo + costOpt.estimatedCost.getCpu,
        estimatedRows = 0L
      )
    }

  override def analyzeQuery(request: AnalyzeQueryRequest): Future[QueryAnalysis] =
    runIO("AnalyzeQuery") {
      for {
        _           <- validateSQL(request.sql)
        parseResult <- parseSql(request.sql)
        logicalOpt  <- logicalOptimizer.optimize(parseResult.sqlNode)
        costOpt     <- IO.delay(costBasedOptimizer.optimize(logicalOpt.optimizedPlan))
        queryId      = UUID.randomUUID().toString
      } yield QueryAnalysis(
        queryId = queryId,
        estimatedCost = costOpt.estimatedCost.getIo + costOpt.estimatedCost.getCpu,
        estimatedRows = 0L,
        tablesAccessed = Seq.empty,
        columnsAccessed = Seq.empty,
        complexity = parseResult.kind.toString
      )
    }

  override def getQueryStatus(request: GetQueryStatusRequest): Future[QueryStatus] =
    runIO("GetQueryStatus") {
      IO.pure(
        QueryStatus(
          queryId = request.queryId,
          state = "COMPLETED",
          progress = 1.0,
          rowsProcessed = 0L,
          errorMessage = ""
        )
      )
    }

  // ---------------------------------------------------------------------------
  // Conversions
  // ---------------------------------------------------------------------------

  private def toProtoQueryResult(result: DomainQueryResult, maxRows: Int): ProtoQueryResult = {
    val limitedRows = if (maxRows > 0) result.rows.take(maxRows) else result.rows
    ProtoQueryResult(
      queryId = result.queryId,
      columns = result.columns.map(c => ProtoColumnDescriptor(name = c.name, typeName = c.typeName)),
      rows = limitedRows.map { row =>
        QueryResultRow(values = row.map { case (k, v) => k -> String.valueOf(v) })
      },
      rowCount = result.rowCount,
      executionTimeMs = result.executionTimeMs,
      stagesExecuted = result.stagesExecuted
    )
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def parseSql(sql: String): IO[io.gbmm.udps.query.parser.ParseResult] =
    IO.fromEither(
      sqlParser.parse(sql).left.map(err =>
        Status.INVALID_ARGUMENT
          .withDescription(s"SQL parse error: ${err.message} at line ${err.line}, column ${err.column}")
          .asRuntimeException()
      )
    )

  private def validateSQL(sql: String): IO[Unit] =
    if (sql == null || sql.trim.isEmpty)
      IO.raiseError(
        Status.INVALID_ARGUMENT
          .withDescription("SQL query must not be empty")
          .asRuntimeException()
      )
    else IO.unit

  private def runIO[A](rpcName: String)(effect: IO[A]): Future[A] =
    effect.handleErrorWith {
      case e: StatusRuntimeException => IO.raiseError(e)
      case e: QueryExecutionException =>
        logger.error("Query execution failed in {}: {}", rpcName, e.getMessage: Any)
        IO.raiseError(
          Status.INTERNAL
            .withDescription(s"Query execution failed: ${e.reason}")
            .asRuntimeException()
        )
      case e: Throwable =>
        logger.error(s"Unexpected error in $rpcName", e)
        IO.raiseError(
          Status.INTERNAL
            .withDescription(s"Internal error in $rpcName: ${e.getMessage}")
            .asRuntimeException()
        )
    }.unsafeToFuture()
}
