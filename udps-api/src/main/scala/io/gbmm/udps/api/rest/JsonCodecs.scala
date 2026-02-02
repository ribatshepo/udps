package io.gbmm.udps.api.rest

import io.circe._
import io.circe.generic.semiauto._
import io.gbmm.udps.catalog.repository._
import io.gbmm.udps.query.execution.{ColumnDescriptor, QueryResult}

import java.time.Instant
import java.util.UUID

/** Request body for creating a new database in the catalog. */
final case class CreateDatabaseRequest(
  name: String,
  description: Option[String]
)

/** Request body for creating a new schema in a database. */
final case class CreateSchemaRequest(
  databaseId: UUID,
  name: String
)

/** Request body for updating an existing database. */
final case class UpdateDatabaseRequest(
  name: String,
  description: Option[String]
)

/** Request body for updating an existing schema. */
final case class UpdateSchemaRequest(
  name: String
)

/** Request body for executing a SQL query. */
final case class ExecuteQueryRequest(
  sql: String,
  parameters: Option[Map[String, String]]
)

/** Request body for creating a new table entry. */
final case class CreateTableRequest(
  schemaId: UUID,
  name: String,
  tier: String
)

/** Request body for updating an existing table entry. */
final case class UpdateTableRequest(
  name: String,
  rowCount: Long,
  sizeBytes: Long,
  tier: String
)

/** Request body for creating a new column entry. */
final case class CreateColumnRequest(
  tableId: UUID,
  name: String,
  dataType: String,
  nullable: Boolean,
  indexed: Boolean,
  ftsEnabled: Boolean,
  piiClassified: Boolean,
  ordinalPosition: Int
)

/** Request body for updating an existing column entry. */
final case class UpdateColumnRequest(
  name: String,
  dataType: String,
  nullable: Boolean,
  indexed: Boolean,
  ftsEnabled: Boolean,
  piiClassified: Boolean,
  ordinalPosition: Int
)

/** Wraps API responses in a uniform envelope. */
final case class ApiResponse[A](
  data: A,
  meta: Option[ResponseMeta]
)

/** Optional metadata in the response envelope. */
final case class ResponseMeta(
  totalCount: Option[Long],
  executionTimeMs: Option[Long]
)

/** Circe codecs for catalog entities, query results, and API request/response types. */
object JsonCodecs {

  // -------------------------------------------------------------------------
  // Primitives
  // -------------------------------------------------------------------------

  implicit val instantEncoder: Encoder[Instant] =
    Encoder.encodeString.contramap[Instant](_.toString)

  implicit val instantDecoder: Decoder[Instant] =
    Decoder.decodeString.emap { str =>
      try Right(Instant.parse(str))
      catch { case e: java.time.format.DateTimeParseException => Left(e.getMessage) }
    }

  // -------------------------------------------------------------------------
  // Catalog entity codecs
  // -------------------------------------------------------------------------

  implicit val catalogDatabaseEncoder: Encoder[CatalogDatabase] = deriveEncoder[CatalogDatabase]
  implicit val catalogDatabaseDecoder: Decoder[CatalogDatabase] = deriveDecoder[CatalogDatabase]

  implicit val catalogSchemaEncoder: Encoder[CatalogSchema] = deriveEncoder[CatalogSchema]
  implicit val catalogSchemaDecoder: Decoder[CatalogSchema] = deriveDecoder[CatalogSchema]

  implicit val catalogTableEncoder: Encoder[CatalogTable] = deriveEncoder[CatalogTable]
  implicit val catalogTableDecoder: Decoder[CatalogTable] = deriveDecoder[CatalogTable]

  implicit val catalogColumnEncoder: Encoder[CatalogColumn] = deriveEncoder[CatalogColumn]
  implicit val catalogColumnDecoder: Decoder[CatalogColumn] = deriveDecoder[CatalogColumn]

  // -------------------------------------------------------------------------
  // Query result codecs
  // -------------------------------------------------------------------------

  implicit val columnDescriptorEncoder: Encoder[ColumnDescriptor] = deriveEncoder[ColumnDescriptor]
  implicit val columnDescriptorDecoder: Decoder[ColumnDescriptor] = deriveDecoder[ColumnDescriptor]

  /** Encoder for Map[String, Any] rows.  Encodes common JVM types that appear
    * in query result rows to their natural JSON representations.
    */
  implicit val anyMapEncoder: Encoder[Map[String, Any]] = Encoder.instance { m =>
    Json.obj(m.map { case (k, v) => k -> encodeAny(v) }.toSeq: _*)
  }

  implicit val anyMapDecoder: Decoder[Map[String, Any]] = Decoder.instance { c =>
    c.as[Map[String, Json]].map(_.map { case (k, v) => k -> decodeJsonValue(v) })
  }

  implicit val queryResultEncoder: Encoder[QueryResult] = Encoder.instance { qr =>
    Json.obj(
      "queryId" -> Json.fromString(qr.queryId),
      "columns" -> Encoder[Seq[ColumnDescriptor]].apply(qr.columns),
      "rows" -> Encoder[Seq[Map[String, Any]]].apply(qr.rows),
      "rowCount" -> Json.fromLong(qr.rowCount),
      "executionTimeMs" -> Json.fromLong(qr.executionTimeMs),
      "stagesExecuted" -> Json.fromInt(qr.stagesExecuted)
    )
  }

  // -------------------------------------------------------------------------
  // API request codecs
  // -------------------------------------------------------------------------

  implicit val createDatabaseRequestDecoder: Decoder[CreateDatabaseRequest] =
    deriveDecoder[CreateDatabaseRequest]
  implicit val createDatabaseRequestEncoder: Encoder[CreateDatabaseRequest] =
    deriveEncoder[CreateDatabaseRequest]

  implicit val createSchemaRequestDecoder: Decoder[CreateSchemaRequest] =
    deriveDecoder[CreateSchemaRequest]
  implicit val createSchemaRequestEncoder: Encoder[CreateSchemaRequest] =
    deriveEncoder[CreateSchemaRequest]

  implicit val executeQueryRequestDecoder: Decoder[ExecuteQueryRequest] =
    deriveDecoder[ExecuteQueryRequest]
  implicit val executeQueryRequestEncoder: Encoder[ExecuteQueryRequest] =
    deriveEncoder[ExecuteQueryRequest]

  implicit val updateDatabaseRequestDecoder: Decoder[UpdateDatabaseRequest] =
    deriveDecoder[UpdateDatabaseRequest]
  implicit val updateDatabaseRequestEncoder: Encoder[UpdateDatabaseRequest] =
    deriveEncoder[UpdateDatabaseRequest]

  implicit val updateSchemaRequestDecoder: Decoder[UpdateSchemaRequest] =
    deriveDecoder[UpdateSchemaRequest]
  implicit val updateSchemaRequestEncoder: Encoder[UpdateSchemaRequest] =
    deriveEncoder[UpdateSchemaRequest]

  implicit val createTableRequestDecoder: Decoder[CreateTableRequest] =
    deriveDecoder[CreateTableRequest]
  implicit val createTableRequestEncoder: Encoder[CreateTableRequest] =
    deriveEncoder[CreateTableRequest]

  implicit val updateTableRequestDecoder: Decoder[UpdateTableRequest] =
    deriveDecoder[UpdateTableRequest]
  implicit val updateTableRequestEncoder: Encoder[UpdateTableRequest] =
    deriveEncoder[UpdateTableRequest]

  implicit val createColumnRequestDecoder: Decoder[CreateColumnRequest] =
    deriveDecoder[CreateColumnRequest]
  implicit val createColumnRequestEncoder: Encoder[CreateColumnRequest] =
    deriveEncoder[CreateColumnRequest]

  implicit val updateColumnRequestDecoder: Decoder[UpdateColumnRequest] =
    deriveDecoder[UpdateColumnRequest]
  implicit val updateColumnRequestEncoder: Encoder[UpdateColumnRequest] =
    deriveEncoder[UpdateColumnRequest]

  // -------------------------------------------------------------------------
  // API response envelope codecs
  // -------------------------------------------------------------------------

  implicit val responseMetaEncoder: Encoder[ResponseMeta] = deriveEncoder[ResponseMeta]
  implicit val responseMetaDecoder: Decoder[ResponseMeta] = deriveDecoder[ResponseMeta]

  implicit def apiResponseEncoder[A: Encoder]: Encoder[ApiResponse[A]] =
    deriveEncoder[ApiResponse[A]]
  implicit def apiResponseDecoder[A: Decoder]: Decoder[ApiResponse[A]] =
    deriveDecoder[ApiResponse[A]]

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private def encodeAny(v: Any): Json = v match {
    case null          => Json.Null
    case s: String     => Json.fromString(s)
    case n: Int        => Json.fromInt(n)
    case n: Long       => Json.fromLong(n)
    case n: Double     => Json.fromDoubleOrNull(n)
    case n: Float      => Json.fromFloatOrNull(n)
    case b: Boolean    => Json.fromBoolean(b)
    case bd: BigDecimal => Json.fromBigDecimal(bd)
    case i: Instant    => Json.fromString(i.toString)
    case u: UUID       => Json.fromString(u.toString)
    case seq: Seq[_]   => Json.arr(seq.map(encodeAny): _*)
    case m: Map[_, _]  =>
      Json.obj(m.map { case (k, mv) => k.toString -> encodeAny(mv) }.toSeq: _*)
    case other         => Json.fromString(other.toString)
  }

  private def decodeJsonValue(j: Json): Any = j.fold(
    jsonNull = null,
    jsonBoolean = identity,
    jsonNumber = n => n.toLong.getOrElse(n.toDouble),
    jsonString = identity,
    jsonArray = arr => arr.map(decodeJsonValue),
    jsonObject = obj => obj.toMap.map { case (k, v) => k -> decodeJsonValue(v) }
  )
}
