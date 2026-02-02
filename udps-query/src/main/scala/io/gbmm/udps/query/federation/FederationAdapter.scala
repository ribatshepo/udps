package io.gbmm.udps.query.federation

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import io.gbmm.udps.core.domain.DataType
import org.apache.calcite.jdbc.CalciteConnection
import org.apache.calcite.schema.{Schema, SchemaPlus}

import java.sql.DriverManager
import java.util.{Properties => JProperties}
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/** Sealed trait enumerating all supported federated data source types. */
sealed trait DataSourceType extends Product with Serializable

object DataSourceType {
  case object PostgreSQL extends DataSourceType
  case object MySQL extends DataSourceType
  case object MongoDB extends DataSourceType
  case object Elasticsearch extends DataSourceType
  case object S3 extends DataSourceType
  case object Kafka extends DataSourceType
}

/** Configuration for connecting to a federated data source.
  *
  * @param sourceType   the type of data source
  * @param host         hostname or IP address
  * @param port         port number (use 0 to indicate default for the source type)
  * @param database     database name, bucket name, index pattern, or topic prefix depending on source type
  * @param username     optional authentication username
  * @param password     optional authentication password
  * @param properties   additional key-value properties for driver-specific configuration
  */
final case class DataSourceConfig(
    sourceType: DataSourceType,
    host: String,
    port: Int,
    database: String,
    username: Option[String],
    password: Option[String],
    properties: Map[String, String]
)

/** Information about a column in a federated table. */
final case class FederatedColumnInfo(
    name: String,
    dataType: DataType
)

/** Metadata about a discovered table in a federated data source.
  *
  * @param name            fully qualified or simple table name
  * @param columns         ordered sequence of column definitions
  * @param estimatedRows   estimated number of rows (use -1 if unknown)
  * @param sourceType      the data source type this table originates from
  */
final case class FederatedTableInfo(
    name: String,
    columns: Seq[FederatedColumnInfo],
    estimatedRows: Long,
    sourceType: DataSourceType
)

/** Describes which query operations a federated source can handle natively. */
final case class PushdownCapabilities(
    predicates: Boolean,
    projections: Boolean,
    limits: Boolean,
    aggregations: Boolean,
    joins: Boolean
)

/** A live connection to a federated data source. */
trait FederatedConnection {

  /** Returns true if the underlying connection is open and usable. */
  def isConnected: IO[Boolean]

  /** Close the connection and release all associated resources. */
  def close: IO[Unit]

  /** The type of data source this connection is for. */
  def sourceType: DataSourceType

  /** Arbitrary metadata about this connection (driver version, server info, etc.). */
  def metadata: Map[String, String]
}

/** Adapter interface for integrating a federated data source into the UDPS query engine.
  *
  * Each data source type provides an implementation that knows how to connect,
  * discover schema information, and create a Calcite-compatible schema for
  * cross-source query planning and execution.
  */
trait FederationAdapter {

  /** The data source type handled by this adapter. */
  def sourceType: DataSourceType

  /** Establish a connection to the federated data source. */
  def connect(config: DataSourceConfig): IO[FederatedConnection]

  /** Discover all available tables via the given connection. */
  def discoverTables(connection: FederatedConnection): IO[Seq[FederatedTableInfo]]

  /** Create a Calcite Schema that can participate in federated query planning. */
  def createCalciteSchema(connection: FederatedConnection): IO[Schema]

  /** Return the pushdown capabilities of this data source. */
  def pushdownCapabilities: PushdownCapabilities
}

/** Thread-safe registry of federation adapters and factory for federated Calcite schemas.
  *
  * @param runtime the Cats Effect IORuntime used at Calcite's synchronous boundary
  */
final class FederationRegistry(runtime: IORuntime) {

  private val adapters: ConcurrentHashMap[DataSourceType, FederationAdapter] =
    new ConcurrentHashMap[DataSourceType, FederationAdapter]()

  /** Register an adapter for the given data source type, replacing any previous registration. */
  def register(adapter: FederationAdapter): Unit = {
    val _ = adapters.put(adapter.sourceType, adapter)
  }

  /** Remove the adapter for the given data source type. */
  def unregister(sourceType: DataSourceType): Unit = {
    val _ = adapters.remove(sourceType)
  }

  /** Retrieve the adapter registered for the given data source type. */
  def getAdapter(sourceType: DataSourceType): Option[FederationAdapter] =
    Option(adapters.get(sourceType))

  /** Return all currently registered adapters. */
  def registeredAdapters: Seq[FederationAdapter] =
    adapters.values().asScala.toSeq

  /** Build a Calcite SchemaPlus containing sub-schemas for each federated source.
    *
    * Each `DataSourceConfig` in `sources` is used to connect via the appropriate adapter
    * and create a named sub-schema. The sub-schema name is derived from the source config's
    * database field combined with the source type.
    *
    * @param sources sequence of data source configurations to federate
    * @return a Calcite SchemaPlus with sub-schemas for each source
    */
  def createFederatedSchema(sources: Seq[DataSourceConfig]): IO[SchemaPlus] = IO {
    val info = new JProperties()
    info.setProperty("lex", "JAVA")
    val calciteConnection = DriverManager
      .getConnection("jdbc:calcite:", info)
      .unwrap(classOf[CalciteConnection])
    calciteConnection.getRootSchema
  }.flatMap { rootSchema =>
    val effects = sources.map { config =>
      getAdapter(config.sourceType) match {
        case Some(adapter) =>
          for {
            conn   <- adapter.connect(config)
            schema <- adapter.createCalciteSchema(conn)
          } yield {
            val subSchemaName = buildSubSchemaName(config)
            rootSchema.add(subSchemaName, schema)
          }
        case None =>
          IO.raiseError[Unit](
            new IllegalArgumentException(
              s"No adapter registered for source type: ${config.sourceType}"
            )
          )
      }
    }
    import cats.implicits._
    effects.sequence_.as(rootSchema)
  }

  private def buildSubSchemaName(config: DataSourceConfig): String = {
    val typeName = config.sourceType match {
      case DataSourceType.PostgreSQL     => "pg"
      case DataSourceType.MySQL          => "mysql"
      case DataSourceType.MongoDB        => "mongo"
      case DataSourceType.Elasticsearch  => "es"
      case DataSourceType.S3             => "s3"
      case DataSourceType.Kafka          => "kafka"
    }
    s"${typeName}_${config.database}"
  }
}

object FederationRegistry {

  /** Create a new empty registry. */
  def apply(runtime: IORuntime): FederationRegistry =
    new FederationRegistry(runtime)
}
