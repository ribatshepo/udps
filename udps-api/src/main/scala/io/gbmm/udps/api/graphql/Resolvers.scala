package io.gbmm.udps.api.graphql

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import io.gbmm.udps.catalog.repository.MetadataRepository
import io.gbmm.udps.query.execution.QueryResult
import sangria.schema._

import scala.concurrent.Future

/** Holds all dependencies needed by GraphQL resolvers during execution.
  *
  * @param metadataRepository catalog metadata CRUD operations
  * @param executeSql         end-to-end SQL execution function (parse -> optimise -> execute)
  * @param runtime            Cats Effect IO runtime used to bridge IO to Future
  */
final case class GraphQLContext(
    metadataRepository: MetadataRepository,
    executeSql: String => IO[QueryResult]
)(implicit val runtime: IORuntime) {

  /** Convert an IO action to a Future for Sangria's execution model. */
  def runIO[A](io: IO[A]): Future[A] =
    io.unsafeToFuture()
}

/** Query resolvers for the UDPS GraphQL API. */
object Resolvers {

  import GraphQLSchema._

  // ---------------------------------------------------------------------------
  // Query arguments
  // ---------------------------------------------------------------------------

  private val IdArg = Argument("id", UUIDType, description = "Entity identifier")
  private val DatabaseIdArg =
    Argument("databaseId", UUIDType, description = "Parent database identifier")
  private val SchemaIdArg =
    Argument("schemaId", UUIDType, description = "Parent schema identifier")
  private val TableIdArg =
    Argument("tableId", UUIDType, description = "Parent table identifier")
  private val SqlArg = Argument("sql", StringType, description = "SQL query to execute")

  // ---------------------------------------------------------------------------
  // Query type
  // ---------------------------------------------------------------------------

  val QueryType: ObjectType[GraphQLContext, Unit] = ObjectType(
    "Query",
    "UDPS catalog and query operations",
    fields[GraphQLContext, Unit](
      Field(
        "databases",
        ListType(DatabaseType),
        description = Some("List all registered databases"),
        resolve = ctx => ctx.ctx.runIO(ctx.ctx.metadataRepository.listDatabases)
      ),
      Field(
        "database",
        OptionType(DatabaseType),
        description = Some("Retrieve a database by its identifier"),
        arguments = IdArg :: Nil,
        resolve = ctx =>
          ctx.ctx.runIO(ctx.ctx.metadataRepository.getDatabase(ctx.arg(IdArg)))
      ),
      Field(
        "schemas",
        ListType(SchemaType),
        description = Some("List schemas within a database"),
        arguments = DatabaseIdArg :: Nil,
        resolve = ctx =>
          ctx.ctx.runIO(
            ctx.ctx.metadataRepository.listSchemasByDatabase(ctx.arg(DatabaseIdArg))
          )
      ),
      Field(
        "schema",
        OptionType(SchemaType),
        description = Some("Retrieve a schema by its identifier"),
        arguments = IdArg :: Nil,
        resolve = ctx =>
          ctx.ctx.runIO(ctx.ctx.metadataRepository.getSchema(ctx.arg(IdArg)))
      ),
      Field(
        "tables",
        ListType(TableType),
        description = Some("List tables within a schema"),
        arguments = SchemaIdArg :: Nil,
        resolve = ctx =>
          ctx.ctx.runIO(
            ctx.ctx.metadataRepository.listTablesBySchema(ctx.arg(SchemaIdArg))
          )
      ),
      Field(
        "table",
        OptionType(TableType),
        description = Some("Retrieve a table by its identifier"),
        arguments = IdArg :: Nil,
        resolve = ctx =>
          ctx.ctx.runIO(ctx.ctx.metadataRepository.getTable(ctx.arg(IdArg)))
      ),
      Field(
        "columns",
        ListType(ColumnType),
        description = Some("List columns within a table"),
        arguments = TableIdArg :: Nil,
        resolve = ctx =>
          ctx.ctx.runIO(
            ctx.ctx.metadataRepository.listColumnsByTable(ctx.arg(TableIdArg))
          )
      ),
      Field(
        "column",
        OptionType(ColumnType),
        description = Some("Retrieve a column by its identifier"),
        arguments = IdArg :: Nil,
        resolve = ctx =>
          ctx.ctx.runIO(ctx.ctx.metadataRepository.getColumn(ctx.arg(IdArg)))
      ),
      Field(
        "executeQuery",
        QueryResultType,
        description = Some("Execute a SQL query and return results"),
        arguments = SqlArg :: Nil,
        resolve = ctx => {
          val sql = ctx.arg(SqlArg)
          ctx.ctx.runIO(
            ctx.ctx
              .executeSql(sql)
              .map(GraphQLQueryResult.fromQueryResult)
          )
        }
      )
    )
  )

  // ---------------------------------------------------------------------------
  // Full schema construction
  // ---------------------------------------------------------------------------

  /** Build the complete Sangria Schema combining queries and mutations. */
  def schema: Schema[GraphQLContext, Unit] =
    Schema(
      query = QueryType,
      mutation = Some(Mutations.MutationType),
      subscription = None,
      description = Some("UDPS - Universal Data Platform Service GraphQL API")
    )
}
