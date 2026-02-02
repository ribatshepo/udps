package io.gbmm.udps.catalog.querybuilder

import io.circe._
import io.circe.generic.semiauto._

final case class TableRef(
    name: String,
    alias: Option[String]
)

object TableRef {
  implicit val encoder: Encoder[TableRef] = deriveEncoder[TableRef]
  implicit val decoder: Decoder[TableRef] = deriveDecoder[TableRef]
}

sealed trait AggregationType

object AggregationType {
  case object Count extends AggregationType
  case object Sum extends AggregationType
  case object Avg extends AggregationType
  case object Min extends AggregationType
  case object Max extends AggregationType

  implicit val encoder: Encoder[AggregationType] = Encoder.encodeString.contramap {
    case Count => "COUNT"
    case Sum   => "SUM"
    case Avg   => "AVG"
    case Min   => "MIN"
    case Max   => "MAX"
  }

  implicit val decoder: Decoder[AggregationType] = Decoder.decodeString.emap {
    case "COUNT" => Right(Count)
    case "SUM"   => Right(Sum)
    case "AVG"   => Right(Avg)
    case "MIN"   => Right(Min)
    case "MAX"   => Right(Max)
    case other   => Left(s"Unknown aggregation type: $other")
  }
}

final case class ColumnRef(
    table: Option[String],
    column: String,
    aggregation: Option[AggregationType],
    alias: Option[String]
)

object ColumnRef {
  implicit val encoder: Encoder[ColumnRef] = deriveEncoder[ColumnRef]
  implicit val decoder: Decoder[ColumnRef] = deriveDecoder[ColumnRef]
}

sealed trait FilterOp

object FilterOp {
  case object Eq extends FilterOp
  case object NotEq extends FilterOp
  case object Lt extends FilterOp
  case object Gt extends FilterOp
  case object Lte extends FilterOp
  case object Gte extends FilterOp
  case object In extends FilterOp
  case object Like extends FilterOp
  case object IsNull extends FilterOp
  case object IsNotNull extends FilterOp

  implicit val encoder: Encoder[FilterOp] = Encoder.encodeString.contramap {
    case Eq        => "EQ"
    case NotEq     => "NOT_EQ"
    case Lt        => "LT"
    case Gt        => "GT"
    case Lte       => "LTE"
    case Gte       => "GTE"
    case In        => "IN"
    case Like      => "LIKE"
    case IsNull    => "IS_NULL"
    case IsNotNull => "IS_NOT_NULL"
  }

  implicit val decoder: Decoder[FilterOp] = Decoder.decodeString.emap {
    case "EQ"          => Right(Eq)
    case "NOT_EQ"      => Right(NotEq)
    case "LT"          => Right(Lt)
    case "GT"          => Right(Gt)
    case "LTE"         => Right(Lte)
    case "GTE"         => Right(Gte)
    case "IN"          => Right(In)
    case "LIKE"        => Right(Like)
    case "IS_NULL"     => Right(IsNull)
    case "IS_NOT_NULL" => Right(IsNotNull)
    case other         => Left(s"Unknown filter op: $other")
  }
}

final case class FilterExpr(
    column: String,
    operator: FilterOp,
    value: String
)

object FilterExpr {
  implicit val encoder: Encoder[FilterExpr] = deriveEncoder[FilterExpr]
  implicit val decoder: Decoder[FilterExpr] = deriveDecoder[FilterExpr]
}

sealed trait JoinType

object JoinType {
  case object Inner extends JoinType
  case object Left extends JoinType
  case object Right extends JoinType
  case object Full extends JoinType
  case object Cross extends JoinType

  implicit val encoder: Encoder[JoinType] = Encoder.encodeString.contramap {
    case Inner => "INNER"
    case Left  => "LEFT"
    case Right => "RIGHT"
    case Full  => "FULL"
    case Cross => "CROSS"
  }

  implicit val decoder: Decoder[JoinType] = Decoder.decodeString.emap {
    case "INNER" => scala.Right(Inner)
    case "LEFT"  => scala.Right(Left)
    case "RIGHT" => scala.Right(Right)
    case "FULL"  => scala.Right(Full)
    case "CROSS" => scala.Right(Cross)
    case other   => scala.Left(s"Unknown join type: $other")
  }
}

final case class JoinSpec(
    rightTable: TableRef,
    joinType: JoinType,
    leftColumn: String,
    rightColumn: String
)

object JoinSpec {
  implicit val encoder: Encoder[JoinSpec] = deriveEncoder[JoinSpec]
  implicit val decoder: Decoder[JoinSpec] = deriveDecoder[JoinSpec]
}

final case class OrderBySpec(
    column: String,
    ascending: Boolean
)

object OrderBySpec {
  implicit val encoder: Encoder[OrderBySpec] = deriveEncoder[OrderBySpec]
  implicit val decoder: Decoder[OrderBySpec] = deriveDecoder[OrderBySpec]
}

final case class QuerySpec(
    tables: Seq[TableRef],
    columns: Seq[ColumnRef],
    filters: Seq[FilterExpr],
    joins: Seq[JoinSpec],
    groupBy: Seq[String],
    orderBy: Seq[OrderBySpec],
    limit: Option[Int]
)

object QuerySpec {
  implicit val encoder: Encoder[QuerySpec] = deriveEncoder[QuerySpec]
  implicit val decoder: Decoder[QuerySpec] = deriveDecoder[QuerySpec]
}

final class QueryBuilder private (private val spec: QuerySpec) {

  def from(table: String): QueryBuilder =
    new QueryBuilder(spec.copy(tables = spec.tables :+ TableRef(table, None)))

  def fromWithAlias(table: String, alias: String): QueryBuilder =
    new QueryBuilder(spec.copy(tables = spec.tables :+ TableRef(table, Some(alias))))

  def select(columns: String*): QueryBuilder = {
    val refs = columns.map(c => ColumnRef(table = None, column = c, aggregation = None, alias = None))
    new QueryBuilder(spec.copy(columns = spec.columns ++ refs))
  }

  def selectAgg(
      column: String,
      agg: AggregationType,
      alias: Option[String] = None
  ): QueryBuilder = {
    val ref = ColumnRef(table = None, column = column, aggregation = Some(agg), alias = alias)
    new QueryBuilder(spec.copy(columns = spec.columns :+ ref))
  }

  def where(column: String, op: FilterOp, value: String): QueryBuilder =
    new QueryBuilder(spec.copy(filters = spec.filters :+ FilterExpr(column, op, value)))

  def join(
      table: String,
      leftCol: String,
      rightCol: String,
      joinType: JoinType
  ): QueryBuilder = {
    val js = JoinSpec(TableRef(table, None), joinType, leftCol, rightCol)
    new QueryBuilder(spec.copy(joins = spec.joins :+ js))
  }

  def groupBy(columns: String*): QueryBuilder =
    new QueryBuilder(spec.copy(groupBy = spec.groupBy ++ columns))

  def orderBy(column: String, asc: Boolean): QueryBuilder =
    new QueryBuilder(spec.copy(orderBy = spec.orderBy :+ OrderBySpec(column, asc)))

  def limit(n: Int): QueryBuilder =
    new QueryBuilder(spec.copy(limit = Some(n)))

  def build: QuerySpec = spec
}

object QueryBuilder {

  def apply(): QueryBuilder =
    new QueryBuilder(
      QuerySpec(
        tables = Seq.empty,
        columns = Seq.empty,
        filters = Seq.empty,
        joins = Seq.empty,
        groupBy = Seq.empty,
        orderBy = Seq.empty,
        limit = None
      )
    )
}
