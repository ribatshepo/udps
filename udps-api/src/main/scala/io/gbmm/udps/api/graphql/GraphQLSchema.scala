package io.gbmm.udps.api.graphql

import java.time.Instant
import java.util.UUID

import io.gbmm.udps.catalog.repository._
import io.gbmm.udps.query.execution.{ColumnDescriptor, QueryResult}
import sangria.schema._

/** Sangria GraphQL type definitions for UDPS catalog entities and query results. */
object GraphQLSchema {

  // ---------------------------------------------------------------------------
  // Scalar types
  // ---------------------------------------------------------------------------

  implicit val UUIDType: ScalarType[UUID] = ScalarType[UUID](
    name = "UUID",
    description = Some("A universally unique identifier compliant with RFC 4122"),
    coerceOutput = (value, _) => value.toString,
    coerceUserInput = {
      case s: String =>
        try Right(UUID.fromString(s))
        catch { case _: IllegalArgumentException => Left(UUIDViolation(s)) }
      case other =>
        Left(UUIDViolation(other.toString))
    },
    coerceInput = {
      case sangria.ast.StringValue(s, _, _, _, _) =>
        try Right(UUID.fromString(s))
        catch { case _: IllegalArgumentException => Left(UUIDViolation(s)) }
      case other =>
        Left(UUIDViolation(other.toString))
    }
  )

  implicit val InstantType: ScalarType[Instant] = ScalarType[Instant](
    name = "DateTime",
    description = Some("An ISO-8601 date-time string"),
    coerceOutput = (value, _) => value.toString,
    coerceUserInput = {
      case s: String =>
        try Right(Instant.parse(s))
        catch { case _: java.time.format.DateTimeParseException => Left(DateTimeViolation(s)) }
      case other =>
        Left(DateTimeViolation(other.toString))
    },
    coerceInput = {
      case sangria.ast.StringValue(s, _, _, _, _) =>
        try Right(Instant.parse(s))
        catch { case _: java.time.format.DateTimeParseException => Left(DateTimeViolation(s)) }
      case other =>
        Left(DateTimeViolation(other.toString))
    }
  )

  // ---------------------------------------------------------------------------
  // Violations
  // ---------------------------------------------------------------------------

  final case class UUIDViolation(value: String)
      extends sangria.validation.ValueCoercionViolation(s"Invalid UUID: $value")

  final case class DateTimeViolation(value: String)
      extends sangria.validation.ValueCoercionViolation(s"Invalid DateTime: $value")

  // ---------------------------------------------------------------------------
  // Output types
  // ---------------------------------------------------------------------------

  val DatabaseType: ObjectType[GraphQLContext, CatalogDatabase] = ObjectType(
    "Database",
    "A registered database in the catalog",
    fields[GraphQLContext, CatalogDatabase](
      Field("id", UUIDType, resolve = _.value.id),
      Field("name", StringType, resolve = _.value.name),
      Field("description", OptionType(StringType), resolve = _.value.description),
      Field("createdAt", InstantType, resolve = _.value.createdAt),
      Field("updatedAt", InstantType, resolve = _.value.updatedAt),
      Field(
        "schemas",
        ListType(SchemaType),
        description = Some("Schemas belonging to this database"),
        resolve = ctx =>
          ctx.ctx.runIO(ctx.ctx.metadataRepository.listSchemasByDatabase(ctx.value.id))
      )
    )
  )

  lazy val SchemaType: ObjectType[GraphQLContext, CatalogSchema] = ObjectType(
    "CatalogSchema",
    "A schema within a database",
    () =>
      fields[GraphQLContext, CatalogSchema](
        Field("id", UUIDType, resolve = _.value.id),
        Field("databaseId", UUIDType, resolve = _.value.databaseId),
        Field("name", StringType, resolve = _.value.name),
        Field("createdAt", InstantType, resolve = _.value.createdAt),
        Field("updatedAt", InstantType, resolve = _.value.updatedAt),
        Field(
          "tables",
          ListType(TableType),
          description = Some("Tables belonging to this schema"),
          resolve = ctx =>
            ctx.ctx.runIO(ctx.ctx.metadataRepository.listTablesBySchema(ctx.value.id))
        )
      )
  )

  lazy val TableType: ObjectType[GraphQLContext, CatalogTable] = ObjectType(
    "Table",
    "A table within a schema",
    () =>
      fields[GraphQLContext, CatalogTable](
        Field("id", UUIDType, resolve = _.value.id),
        Field("schemaId", UUIDType, resolve = _.value.schemaId),
        Field("name", StringType, resolve = _.value.name),
        Field("rowCount", LongType, resolve = _.value.rowCount),
        Field("sizeBytes", LongType, resolve = _.value.sizeBytes),
        Field("tier", StringType, resolve = _.value.tier),
        Field("createdAt", InstantType, resolve = _.value.createdAt),
        Field("updatedAt", InstantType, resolve = _.value.updatedAt),
        Field(
          "columns",
          ListType(ColumnType),
          description = Some("Columns belonging to this table"),
          resolve = ctx =>
            ctx.ctx.runIO(ctx.ctx.metadataRepository.listColumnsByTable(ctx.value.id))
        )
      )
  )

  val ColumnType: ObjectType[GraphQLContext, CatalogColumn] = ObjectType(
    "Column",
    "A column within a table",
    fields[GraphQLContext, CatalogColumn](
      Field("id", UUIDType, resolve = _.value.id),
      Field("tableId", UUIDType, resolve = _.value.tableId),
      Field("name", StringType, resolve = _.value.name),
      Field("dataType", StringType, resolve = _.value.dataType),
      Field("nullable", BooleanType, resolve = _.value.nullable),
      Field("indexed", BooleanType, resolve = _.value.indexed),
      Field("ftsEnabled", BooleanType, resolve = _.value.ftsEnabled),
      Field("piiClassified", BooleanType, resolve = _.value.piiClassified),
      Field("ordinalPosition", IntType, resolve = _.value.ordinalPosition)
    )
  )

  val ColumnDescriptorType: ObjectType[GraphQLContext, ColumnDescriptor] = ObjectType(
    "ColumnDescriptor",
    "Describes a result column with name and type",
    fields[GraphQLContext, ColumnDescriptor](
      Field("name", StringType, resolve = _.value.name),
      Field("typeName", StringType, resolve = _.value.typeName)
    )
  )

  /** Represents a single row as a list of key-value string pairs.
    * Sangria does not support Map[String, Any] natively, so we flatten
    * each row into a list of RowField entries.
    */
  final case class RowField(key: String, value: String)

  val RowFieldType: ObjectType[GraphQLContext, RowField] = ObjectType(
    "RowField",
    "A single key-value pair within a result row",
    fields[GraphQLContext, RowField](
      Field("key", StringType, resolve = _.value.key),
      Field("value", StringType, resolve = _.value.value)
    )
  )

  /** Wrapper around a query result row for GraphQL serialisation. */
  final case class ResultRow(fields: List[RowField])

  val ResultRowType: ObjectType[GraphQLContext, ResultRow] = ObjectType(
    "ResultRow",
    "A single row in a query result",
    fields[GraphQLContext, ResultRow](
      Field("fields", ListType(RowFieldType), resolve = _.value.fields)
    )
  )

  /** GraphQL-friendly projection of [[QueryResult]]. */
  final case class GraphQLQueryResult(
      queryId: String,
      columns: Seq[ColumnDescriptor],
      rows: Seq[ResultRow],
      rowCount: Long,
      executionTimeMs: Long,
      stagesExecuted: Int
  )

  object GraphQLQueryResult {
    def fromQueryResult(qr: QueryResult): GraphQLQueryResult =
      GraphQLQueryResult(
        queryId = qr.queryId,
        columns = qr.columns,
        rows = qr.rows.map { row =>
          ResultRow(row.map { case (k, v) =>
            RowField(k, if (v == null) "" else v.toString) // scalastyle:ignore null
          }.toList)
        },
        rowCount = qr.rowCount,
        executionTimeMs = qr.executionTimeMs,
        stagesExecuted = qr.stagesExecuted
      )
  }

  val QueryResultType: ObjectType[GraphQLContext, GraphQLQueryResult] = ObjectType(
    "QueryResult",
    "The result of a SQL query execution",
    fields[GraphQLContext, GraphQLQueryResult](
      Field("queryId", StringType, resolve = _.value.queryId),
      Field("columns", ListType(ColumnDescriptorType), resolve = _.value.columns.toList),
      Field("rows", ListType(ResultRowType), resolve = _.value.rows.toList),
      Field("rowCount", LongType, resolve = _.value.rowCount),
      Field("executionTimeMs", LongType, resolve = _.value.executionTimeMs),
      Field("stagesExecuted", IntType, resolve = _.value.stagesExecuted)
    )
  )

  // ---------------------------------------------------------------------------
  // Input types -- Sangria infers Map[String, Any] for InputObjectType
  // ---------------------------------------------------------------------------

  val CreateDatabaseInput = InputObjectType(
    "CreateDatabaseInput",
    List(
      InputField("name", StringType),
      InputField("description", OptionInputType(StringType))
    )
  )

  val UpdateDatabaseInput = InputObjectType(
    "UpdateDatabaseInput",
    List(
      InputField("id", UUIDType),
      InputField("name", OptionInputType(StringType)),
      InputField("description", OptionInputType(StringType))
    )
  )

  val CreateSchemaInput = InputObjectType(
    "CreateSchemaInput",
    List(
      InputField("databaseId", UUIDType),
      InputField("name", StringType)
    )
  )

  val UpdateSchemaInput = InputObjectType(
    "UpdateSchemaInput",
    List(
      InputField("id", UUIDType),
      InputField("name", OptionInputType(StringType))
    )
  )

  val CreateTableInput = InputObjectType(
    "CreateTableInput",
    List(
      InputField("schemaId", UUIDType),
      InputField("name", StringType),
      InputField("tier", StringType)
    )
  )

  val UpdateTableInput = InputObjectType(
    "UpdateTableInput",
    List(
      InputField("id", UUIDType),
      InputField("name", OptionInputType(StringType)),
      InputField("rowCount", OptionInputType(LongType)),
      InputField("sizeBytes", OptionInputType(LongType)),
      InputField("tier", OptionInputType(StringType))
    )
  )

  val CreateColumnInput = InputObjectType(
    "CreateColumnInput",
    List(
      InputField("tableId", UUIDType),
      InputField("name", StringType),
      InputField("dataType", StringType),
      InputField("nullable", BooleanType),
      InputField("indexed", BooleanType),
      InputField("ftsEnabled", BooleanType),
      InputField("piiClassified", BooleanType),
      InputField("ordinalPosition", IntType)
    )
  )

  val UpdateColumnInput = InputObjectType(
    "UpdateColumnInput",
    List(
      InputField("id", UUIDType),
      InputField("name", OptionInputType(StringType)),
      InputField("dataType", OptionInputType(StringType)),
      InputField("nullable", OptionInputType(BooleanType)),
      InputField("indexed", OptionInputType(BooleanType)),
      InputField("ftsEnabled", OptionInputType(BooleanType)),
      InputField("piiClassified", OptionInputType(BooleanType)),
      InputField("ordinalPosition", OptionInputType(IntType))
    )
  )
}
