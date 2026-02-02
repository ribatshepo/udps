package io.gbmm.udps.api

import cats.effect.{IO, IOApp, Resource}
import cats.effect.unsafe.IORuntime
import cats.syntax.semigroupk._
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.api.auth.AuthMiddleware
import io.gbmm.udps.api.config._
import io.gbmm.udps.api.graphql.{GraphQLContext, GraphQLRoutes}
import io.gbmm.udps.api.grpc._
import io.gbmm.udps.api.rest._
import io.gbmm.udps.catalog.repository.{CatalogDbConfig => RepoCatalogDbConfig, DoobieMetadataRepository, MetadataRepository}
import io.gbmm.udps.core.config.{MinioConfig, StorageConfig}
import io.gbmm.udps.integration.circuitbreaker.{CircuitBreakerConfig => IntegrationCBConfig, IntegrationCircuitBreaker}
import io.gbmm.udps.integration.uccp.{MetricsPusher, MetricsSource, MetricSnapshot, ObservabilityConfig, TracingPropagator, ServiceDiscoveryClient, ServiceDiscoveryConfig, HealthReporter, HealthReportingConfig}
import io.gbmm.udps.integration.uccp.{HealthCheck => UccpHealthCheck, HealthDetails, ComponentHealth => UccpComponentHealth, HealthReportStatus}
import io.gbmm.udps.integration.usp.{AuthenticationClient, AuthenticationConfig}
import io.gbmm.udps.query.execution.{DataReader, DistributedExecutor}
import io.gbmm.udps.query.optimizer.{ColumnStats, CostBasedOptimizer, LogicalOptimizer, StatisticsProvider, TableStatistics}
import io.gbmm.udps.query.parser.{SQLParser, SQLParserConfig}
import io.gbmm.udps.query.physical.{FilterExpression, PhysicalOperator, PhysicalPlanner}
import io.gbmm.udps.storage.tiering.TierManager
import io.grpc.{ManagedChannel, ManagedChannelBuilder, Server}
import org.http4s.HttpRoutes
import org.http4s.server.{Server => Http4sServer}
import pureconfig.ConfigSource
import pureconfig.module.catseffect.syntax._

/** Main entry point for the UDPS API service.
  *
  * Wires all dependencies together: metadata repository, query execution pipeline,
  * storage tier management, authentication, REST routes, GraphQL routes, and gRPC
  * services. Both an HTTP server and a gRPC server are started concurrently and
  * run until a shutdown signal is received.
  */
object ApiMain extends IOApp.Simple with LazyLogging {

  private val configNamespace = "udps.api"

  override def run: IO[Unit] =
    buildResources.use { case (httpServer, grpcServer) =>
      IO(logger.info(
        "UDPS API started -- HTTP on {}, gRPC on port {}",
        httpServer.address.toString,
        grpcServer.getPort.toString
      )) *> IO.never
    }

  private def buildResources: Resource[IO, (Http4sServer, Server)] =
    for {
      apiConfig     <- Resource.eval(loadConfig)
      ioRuntime      = IORuntime.global

      // -- Database --
      metadataRepo  <- DoobieMetadataRepository.resource(toRepoCatalogDbConfig(apiConfig.catalogDb))

      // -- Storage tier management --
      storageConfig  = toStorageConfig(apiConfig.storage)
      minioConfig    = toMinioConfig(apiConfig.minio)
      tierManager   <- TierManager.resource(storageConfig, minioConfig)

      // -- Authentication --
      uspChannel    <- managedChannelResource(apiConfig.uspGrpc)
      cbConfig       = toIntegrationCBConfig(apiConfig.circuitBreaker)
      circuitBreaker <- IntegrationCircuitBreaker.create(cbConfig, "usp-auth")
      authConfig     = AuthenticationConfig(
                         cacheTtl = apiConfig.auth.cacheTtl,
                         maxCacheSize = apiConfig.auth.maxCacheSize
                       )
      authClient    <- Resource.eval(AuthenticationClient.create(uspChannel, circuitBreaker, authConfig))

      // -- UCCP Observability --
      obsConfig      = ObservabilityConfig.validate(apiConfig.observability)
      uccpMonChannel <- managedChannelResource(obsConfig.uccpMonitoringHost, obsConfig.uccpMonitoringPort, obsConfig.tracing)
      uccpObsCB      <- IntegrationCircuitBreaker.create(cbConfig, "uccp-observability")
      metricsSource   = noopMetricsSource
      metricsPusher  <- MetricsPusher.resource(uccpMonChannel, uccpObsCB, obsConfig.metrics, metricsSource, "udps")

      // -- Query execution pipeline --
      dataReader     = emptyDataReader
      executor       = DistributedExecutor.local(dataReader)
      sqlParser      = SQLParser(SQLParserConfig.default)
      physicalPlanner = PhysicalPlanner()

      sqlToOperator: (String => IO[PhysicalOperator]) = buildSqlToOperator(
                       sqlParser, physicalPlanner, metadataRepo
                     )

      executeSql: (String => IO[io.gbmm.udps.query.execution.QueryResult]) = { (sql: String) =>
                       sqlToOperator(sql).flatMap(executor.execute)
                     }

      // -- Health checks --
      dbHealthCheck  = new HealthCheck {
                         val name: String = "catalog-db"
                         def check: IO[ComponentHealth] =
                           metadataRepo.listDatabases
                             .map(_ => ComponentHealth(name, HealthStatus.Up, None))
                             .handleError { err =>
                               ComponentHealth(name, HealthStatus.Down, Some(err.getMessage))
                             }
                       }

      healthChecks   = List(dbHealthCheck)

      // -- REST routes --
      catalogRoutes  = new CatalogRoutes(metadataRepo)
      storageRoutes  = new StorageRoutes(metadataRepo, tierManager)
      queryRoutes    = new QueryRoutes(executor, sqlToOperator)
      healthRoutes   = new HealthRoutes(healthChecks, apiConfig.version)

      graphqlContext = GraphQLContext(metadataRepo, executeSql)(ioRuntime)
      graphqlRoutes  = GraphQLRoutes(graphqlContext)

      docsRoutes     = new DocsRoutes()

      // -- Compose routes --
      protectedRoutes: HttpRoutes[IO] = catalogRoutes.routes <+>
                         storageRoutes.routes <+>
                         queryRoutes.routes <+>
                         graphqlRoutes.routes

      authedRoutes   = AuthMiddleware(authClient)(protectedRoutes)

      publicRoutes: HttpRoutes[IO] = healthRoutes.routes <+> docsRoutes.routes

      composedRoutes = publicRoutes <+> authedRoutes

      // -- gRPC services --
      catalogGrpc    = new CatalogServiceImpl(metadataRepo, ioRuntime)
      storageGrpc    = new StorageServiceImpl(metadataRepo, tierManager, ioRuntime)
      queryGrpc      = new QueryServiceImpl(
                         sqlParser,
                         buildLogicalOptimizer(metadataRepo),
                         buildCostBasedOptimizer,
                         physicalPlanner,
                         executor,
                         ioRuntime
                       )
      healthGrpc     = HealthServiceImpl(
                         Map("catalog-db" -> metadataRepo.listDatabases.map(_ => true).handleError(_ => false)),
                         ioRuntime
                       )

      // -- Start servers --
      httpServer    <- RestServer.build(apiConfig, composedRoutes)
      grpcServer    <- GrpcServer.build(apiConfig.grpc, catalogGrpc, storageGrpc, queryGrpc, healthGrpc)
    } yield (httpServer, grpcServer)

  /** Load API configuration from the application config under "udps.api". */
  private def loadConfig: IO[ApiConfig] = {
    import ApiConfig._
    ConfigSource.default.at(configNamespace).loadF[IO, ApiConfig]()
  }

  /** Convert the API-layer CatalogDbConfig to the repository-layer CatalogDbConfig. */
  private def toRepoCatalogDbConfig(c: CatalogDbConfig): RepoCatalogDbConfig =
    RepoCatalogDbConfig(
      jdbcUrl = c.jdbcUrl,
      username = c.username,
      password = c.password,
      maximumPoolSize = c.maximumPoolSize,
      minimumIdle = c.minimumIdle,
      connectionTimeoutMs = c.connectionTimeoutMs
    )

  /** Convert storage path config to core StorageConfig. */
  private def toStorageConfig(c: StoragePathConfig): StorageConfig =
    StorageConfig(
      hotPath = c.hotPath,
      warmPath = c.warmPath,
      coldPath = c.coldPath,
      defaultCompression = c.defaultCompression,
      defaultRowGroupSize = c.defaultRowGroupSize,
      workerThreads = c.workerThreads
    )

  /** Convert API minio config to core MinioConfig. */
  private def toMinioConfig(c: MinioApiConfig): MinioConfig =
    MinioConfig(
      endpoint = c.endpoint,
      accessKey = c.accessKey,
      secretKey = c.secretKey,
      bucket = c.bucket
    )

  /** Convert API circuit breaker config to integration layer config. */
  private def toIntegrationCBConfig(c: CircuitBreakerConfig): IntegrationCBConfig =
    IntegrationCBConfig(
      maxFailures = c.maxFailures,
      callTimeout = c.callTimeout,
      resetTimeout = c.resetTimeout,
      exponentialBackoffFactor = c.exponentialBackoffFactor,
      maxResetTimeout = c.maxResetTimeout
    )

  /** Create a managed gRPC channel resource for communicating with UCCP monitoring (with tracing interceptor). */
  private def managedChannelResource(host: String, port: Int, tracingConfig: io.gbmm.udps.integration.uccp.TracingConfig): Resource[IO, ManagedChannel] =
    Resource.make(
      IO {
        val interceptor = TracingPropagator.interceptor(tracingConfig)
        ManagedChannelBuilder
          .forAddress(host, port)
          .usePlaintext()
          .intercept(interceptor)
          .build()
      }
    )(channel =>
      IO {
        channel.shutdown()
        logger.info("UCCP Monitoring gRPC channel shut down")
      }.void
    )

  /** Create a managed gRPC channel resource for communicating with USP. */
  private def managedChannelResource(config: UspGrpcConfig): Resource[IO, ManagedChannel] =
    Resource.make(
      IO {
        ManagedChannelBuilder
          .forAddress(config.host, config.port)
          .usePlaintext()
          .build()
      }
    )(channel =>
      IO {
        channel.shutdown()
        logger.info("USP gRPC channel shut down")
      }.void
    )

  /** Build the SQL-to-PhysicalOperator pipeline function.
    *
    * Parses SQL, runs logical optimisation, cost-based optimisation, and
    * physical planning to produce a PhysicalOperator ready for execution.
    */
  private def buildSqlToOperator(
      sqlParser: SQLParser,
      physicalPlanner: PhysicalPlanner,
      metadataRepo: MetadataRepository
  ): String => IO[PhysicalOperator] = { (sql: String) =>
    val logicalOptimizer = buildLogicalOptimizer(metadataRepo)
    val costOptimizer = buildCostBasedOptimizer
    for {
      parseResult <- IO.fromEither(
                       sqlParser.parse(sql).left.map(err =>
                         new IllegalArgumentException(
                           s"SQL parse error: ${err.message} at line ${err.line}, column ${err.column}"
                         )
                       )
                     )
      logicalOpt  <- logicalOptimizer.optimize(parseResult.sqlNode)
      costOpt     <- IO.delay(costOptimizer.optimize(logicalOpt.optimizedPlan))
      physical     = physicalPlanner.plan(costOpt.optimizedPlan)
    } yield physical
  }

  /** Build a LogicalOptimizer backed by an empty Calcite schema.
    *
    * In a full deployment the schema would be populated from the metadata
    * repository; here we use a root schema as the baseline for the optimiser
    * framework.
    */
  private def buildLogicalOptimizer(metadataRepo: MetadataRepository): LogicalOptimizer = {
    val rootSchema = org.apache.calcite.jdbc.CalciteSchema
      .createRootSchema(true)
      .plus()
    LogicalOptimizer.create(rootSchema)
  }

  /** Build a CostBasedOptimizer with a no-statistics provider.
    *
    * Real deployments should wire a statistics provider backed by the catalog.
    */
  private def buildCostBasedOptimizer: CostBasedOptimizer = {
    val emptyStats = new StatisticsProvider {
      def getTableStatistics(tableName: String, namespace: String): Option[TableStatistics] = None
      def getColumnStatistics(tableName: String, namespace: String, columnName: String): Option[ColumnStats] = None
    }
    CostBasedOptimizer(emptyStats)
  }

  /** A DataReader that returns no rows.
    *
    * For an API gateway that proxies query execution to workers this is
    * sufficient. Direct data access should be wired via the storage layer.
    */
  private val noopMetricsSource: MetricsSource = new MetricsSource {
    def gatherMetrics: IO[List[MetricSnapshot]] = IO.pure(Nil)
  }

  private val emptyDataReader: DataReader = new DataReader {
    def read(
      tableName: String,
      namespace: String,
      columns: Seq[String],
      predicate: Option[FilterExpression],
      partitionId: Int,
      partitionCount: Int
    ): IO[Seq[Map[String, Any]]] = IO.pure(Seq.empty)
  }
}
