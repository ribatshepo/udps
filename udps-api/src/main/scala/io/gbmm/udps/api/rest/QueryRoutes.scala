package io.gbmm.udps.api.rest

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.circe.{Decoder, Encoder, Json}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax._
import io.gbmm.udps.query.execution.QueryExecutionException
import io.gbmm.udps.query.physical._
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.io._

import JsonCodecs._

/** Request body for query execute and explain endpoints. */
final case class QueryRequest(
  sql: String,
  maxRows: Option[Long]
)

object QueryRequest {
  implicit val decoder: Decoder[QueryRequest] = deriveDecoder[QueryRequest]
  implicit val encoder: Encoder[QueryRequest] = deriveEncoder[QueryRequest]

  /** Default row limit when none is specified by the client. */
  val DefaultMaxRows: Long = 10000L
}

/** Response body for query explain endpoint. */
final case class ExplainResult(
  plan: String,
  operatorTree: Json
)

object ExplainResult {
  implicit val encoder: Encoder[ExplainResult] = Encoder.instance { er =>
    Json.obj(
      "plan" -> Json.fromString(er.plan),
      "operatorTree" -> er.operatorTree
    )
  }
}

/** HTTP routes for SQL query execution and plan explanation.
  *
  * @param executor        the distributed query executor
  * @param sqlToOperator   abstracted pipeline converting a SQL string to a physical operator
  */
class QueryRoutes(
  executor: io.gbmm.udps.query.execution.DistributedExecutor,
  sqlToOperator: String => IO[PhysicalOperator]
) extends LazyLogging {

  val routes: HttpRoutes[IO] = ErrorHandler.middleware(HttpRoutes.of[IO] {
    case req @ POST -> Root / "api" / "v1" / "query" / "execute" =>
      handleExecute(req)

    case req @ POST -> Root / "api" / "v1" / "query" / "explain" =>
      handleExplain(req)
  })

  private def handleExecute(req: org.http4s.Request[IO]): IO[org.http4s.Response[IO]] =
    for {
      body     <- req.attemptAs[QueryRequest](jsonOf[IO, QueryRequest]).foldF(
                    failure => IO.raiseError(new IllegalArgumentException(
                      s"Invalid request body: ${failure.getMessage}"
                    )),
                    IO.pure
                  )
      _        <- validateSql(body.sql)
      maxRows   = body.maxRows.getOrElse(QueryRequest.DefaultMaxRows)
      operator <- sqlToOperator(body.sql)
      limited   = applyRowLimit(operator, maxRows)
      result   <- executor.execute(limited).handleErrorWith { case e: Throwable =>
                    e match {
                      case qe: QueryExecutionException =>
                        logger.error("Query execution failed: queryId={}, reason={}", qe.queryId, qe.reason)
                      case _ =>
                        logger.error("Unexpected query execution error", e)
                    }
                    IO.raiseError(e)
                  }
      response <- Ok(
                    ApiResponse(
                      data = result,
                      meta = Some(ResponseMeta(
                        totalCount = Some(result.rowCount),
                        executionTimeMs = Some(result.executionTimeMs)
                      ))
                    ).asJson
                  )
    } yield response

  private def handleExplain(req: org.http4s.Request[IO]): IO[org.http4s.Response[IO]] =
    for {
      body     <- req.attemptAs[QueryRequest](jsonOf[IO, QueryRequest]).foldF(
                    failure => IO.raiseError(new IllegalArgumentException(
                      s"Invalid request body: ${failure.getMessage}"
                    )),
                    IO.pure
                  )
      _        <- validateSql(body.sql)
      operator <- sqlToOperator(body.sql)
      plan      = renderPlan(operator, depth = 0)
      tree      = operatorToJson(operator)
      response <- Ok(
                    ApiResponse(
                      data = ExplainResult(plan, tree),
                      meta = None
                    ).asJson
                  )
    } yield response

  /** Validate that the SQL string is non-empty and not excessively long. */
  private def validateSql(sql: String): IO[Unit] = {
    val MaxSqlLength: Int = 65536
    val trimmed = sql.trim
    if (trimmed.isEmpty)
      IO.raiseError(new IllegalArgumentException("SQL statement must not be empty"))
    else if (trimmed.length > MaxSqlLength)
      IO.raiseError(new IllegalArgumentException(
        s"SQL statement exceeds maximum length of $MaxSqlLength characters"
      ))
    else
      IO.unit
  }

  /** Wrap the operator with a LimitOp when the client specifies maxRows. */
  private def applyRowLimit(operator: PhysicalOperator, maxRows: Long): PhysicalOperator =
    operator match {
      case _: LimitOp => operator
      case _ =>
        LimitOp(
          child = operator,
          offset = 0L,
          fetch = maxRows,
          estimatedRows = Math.min(operator.estimatedRows, maxRows)
        )
    }

  /** Render a human-readable indented plan from the physical operator tree. */
  private def renderPlan(op: PhysicalOperator, depth: Int): String = {
    val indent = "  " * depth
    val header = op match {
      case t: TableScanOp =>
        val pred = t.predicate.fold("")(_ => " [filtered]")
        s"${indent}TableScan: ${t.namespace}.${t.tableName} cols=[${t.columns.mkString(", ")}]$pred est=${t.estimatedRows}"
      case _: FilterOp =>
        s"${indent}Filter: est=${op.estimatedRows}"
      case p: ProjectOp =>
        s"${indent}Project: [${p.outputColumns.mkString(", ")}] est=${p.estimatedRows}"
      case h: HashJoinOp =>
        s"${indent}HashJoin: ${h.joinType} keys=(${h.leftKeys.mkString(",")})=(${h.rightKeys.mkString(",")}) est=${h.estimatedRows}"
      case n: NestedLoopJoinOp =>
        s"${indent}NestedLoopJoin: ${n.joinType} est=${n.estimatedRows}"
      case a: HashAggregateOp =>
        s"${indent}HashAggregate: groupBy=[${a.groupByKeys.mkString(",")}] aggs=${a.aggregations.size} est=${a.estimatedRows}"
      case s: SortOp =>
        s"${indent}Sort: keys=${s.sortKeys.size} est=${s.estimatedRows}"
      case l: LimitOp =>
        s"${indent}Limit: offset=${l.offset} fetch=${l.fetch} est=${l.estimatedRows}"
      case u: UnionOp =>
        val kind = if (u.all) "ALL" else "DISTINCT"
        s"${indent}Union($kind): inputs=${u.inputs.size} est=${u.estimatedRows}"
    }
    val childPlans = op.children.map(c => renderPlan(c, depth + 1))
    (header +: childPlans).mkString("\n")
  }

  /** Convert the physical operator tree to a JSON representation. */
  private def operatorToJson(op: PhysicalOperator): Json = {
    val base: Seq[(String, Json)] = op match {
      case t: TableScanOp =>
        Seq(
          "type" -> Json.fromString("TableScan"),
          "table" -> Json.fromString(s"${t.namespace}.${t.tableName}"),
          "columns" -> Json.arr(t.columns.map(Json.fromString): _*),
          "hasFilter" -> Json.fromBoolean(t.predicate.isDefined),
          "estimatedRows" -> Json.fromLong(t.estimatedRows)
        )
      case _: FilterOp =>
        Seq(
          "type" -> Json.fromString("Filter"),
          "estimatedRows" -> Json.fromLong(op.estimatedRows)
        )
      case p: ProjectOp =>
        Seq(
          "type" -> Json.fromString("Project"),
          "outputColumns" -> Json.arr(p.outputColumns.map(Json.fromString): _*),
          "estimatedRows" -> Json.fromLong(p.estimatedRows)
        )
      case h: HashJoinOp =>
        Seq(
          "type" -> Json.fromString("HashJoin"),
          "joinType" -> Json.fromString(h.joinType.toString),
          "leftKeys" -> Json.arr(h.leftKeys.map(Json.fromInt): _*),
          "rightKeys" -> Json.arr(h.rightKeys.map(Json.fromInt): _*),
          "estimatedRows" -> Json.fromLong(h.estimatedRows)
        )
      case n: NestedLoopJoinOp =>
        Seq(
          "type" -> Json.fromString("NestedLoopJoin"),
          "joinType" -> Json.fromString(n.joinType.toString),
          "estimatedRows" -> Json.fromLong(n.estimatedRows)
        )
      case a: HashAggregateOp =>
        Seq(
          "type" -> Json.fromString("HashAggregate"),
          "groupByKeys" -> Json.arr(a.groupByKeys.map(Json.fromInt): _*),
          "aggregationCount" -> Json.fromInt(a.aggregations.size),
          "estimatedRows" -> Json.fromLong(a.estimatedRows)
        )
      case s: SortOp =>
        Seq(
          "type" -> Json.fromString("Sort"),
          "keyCount" -> Json.fromInt(s.sortKeys.size),
          "estimatedRows" -> Json.fromLong(s.estimatedRows)
        )
      case l: LimitOp =>
        Seq(
          "type" -> Json.fromString("Limit"),
          "offset" -> Json.fromLong(l.offset),
          "fetch" -> Json.fromLong(l.fetch),
          "estimatedRows" -> Json.fromLong(l.estimatedRows)
        )
      case u: UnionOp =>
        Seq(
          "type" -> Json.fromString("Union"),
          "all" -> Json.fromBoolean(u.all),
          "estimatedRows" -> Json.fromLong(u.estimatedRows)
        )
    }

    val childrenJson =
      if (op.children.isEmpty) Seq.empty
      else Seq("children" -> Json.arr(op.children.map(operatorToJson): _*))

    Json.obj((base ++ childrenJson): _*)
  }
}
