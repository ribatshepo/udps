package io.gbmm.udps.core.config

import cats.effect.IO
import pureconfig._
import pureconfig.error.ConfigReaderFailures
import pureconfig.generic.semiauto._
import pureconfig.module.catseffect.syntax._

import scala.concurrent.duration.FiniteDuration

final case class ServiceConfig(
  name: String,
  version: String
)

final case class GrpcConfig(
  host: String,
  port: Int,
  maxMessageSize: Int
)

final case class HttpsConfig(
  host: String,
  port: Int
)

final case class MetricsConfig(
  host: String,
  port: Int
)

final case class HealthConfig(
  host: String,
  port: Int
)

final case class DatabaseConfig(
  host: String,
  port: Int,
  name: String,
  user: String,
  password: String,
  maxPoolSize: Int
) {

  def jdbcUrl: String =
    s"jdbc:postgresql://$host:$port/$name"
}

final case class MinioConfig(
  endpoint: String,
  accessKey: String,
  secretKey: String,
  bucket: String
)

final case class KafkaConfig(
  bootstrapServers: String,
  groupId: String,
  autoOffsetReset: String
)

final case class RedisConfig(
  host: String,
  port: Int
) {

  def uri: String =
    s"redis://$host:$port"
}

final case class TlsConfig(
  enabled: Boolean,
  certPath: String,
  keyPath: String,
  caPath: String
)

final case class StorageConfig(
  hotPath: String,
  warmPath: String,
  coldPath: String,
  defaultCompression: String,
  defaultRowGroupSize: Long,
  workerThreads: Int
)

final case class QueryConfig(
  maxConcurrentQueries: Int,
  defaultTimeout: FiniteDuration,
  queryCacheSize: Int
)

final case class TracingConfig(
  endpoint: String,
  serviceName: String
)

final case class UdpsConfig(
  service: ServiceConfig,
  grpc: GrpcConfig,
  https: HttpsConfig,
  metrics: MetricsConfig,
  health: HealthConfig,
  database: DatabaseConfig,
  minio: MinioConfig,
  kafka: KafkaConfig,
  redis: RedisConfig,
  tls: TlsConfig,
  storage: StorageConfig,
  query: QueryConfig,
  tracing: TracingConfig
)

object UdpsConfig {

  private val configNamespace = "udps"
  private val seriNamespace = "seri"

  implicit val serviceReader: ConfigReader[ServiceConfig] = deriveReader[ServiceConfig]
  implicit val grpcReader: ConfigReader[GrpcConfig] = deriveReader[GrpcConfig]
  implicit val httpsReader: ConfigReader[HttpsConfig] = deriveReader[HttpsConfig]
  implicit val metricsReader: ConfigReader[MetricsConfig] = deriveReader[MetricsConfig]
  implicit val healthReader: ConfigReader[HealthConfig] = deriveReader[HealthConfig]
  implicit val databaseReader: ConfigReader[DatabaseConfig] = deriveReader[DatabaseConfig]
  implicit val minioReader: ConfigReader[MinioConfig] = deriveReader[MinioConfig]
  implicit val kafkaReader: ConfigReader[KafkaConfig] = deriveReader[KafkaConfig]
  implicit val redisReader: ConfigReader[RedisConfig] = deriveReader[RedisConfig]
  implicit val tlsReader: ConfigReader[TlsConfig] = deriveReader[TlsConfig]
  implicit val storageReader: ConfigReader[StorageConfig] = deriveReader[StorageConfig]
  implicit val queryReader: ConfigReader[QueryConfig] = deriveReader[QueryConfig]
  implicit val tracingReader: ConfigReader[TracingConfig] = deriveReader[TracingConfig]
  implicit val udpsReader: ConfigReader[UdpsConfig] = deriveReader[UdpsConfig]

  /** Load configuration from application.conf as an IO effect.
    * Raises a ConfigReaderException on failure.
    */
  def load: IO[UdpsConfig] =
    ConfigSource.default.at(configNamespace).loadF[IO, UdpsConfig]()

  /** Load configuration purely, returning Either for contexts
    * where IO is not desired.
    */
  def loadEither: Either[ConfigReaderFailures, UdpsConfig] =
    ConfigSource.default.at(configNamespace).load[UdpsConfig]

  /** Load the seri platform configuration from the "seri" namespace.
    * Raises a ConfigReaderException on failure.
    */
  def loadSeri: IO[SeriConfig] =
    ConfigSource.default.at(seriNamespace).loadF[IO, SeriConfig]()(
      SeriConfig.reader,
      implicitly
    ).map(SeriConfig.validate)

  /** Load the seri platform configuration purely, returning Either. */
  def loadSeriEither: Either[ConfigReaderFailures, SeriConfig] =
    ConfigSource.default.at(seriNamespace).load[SeriConfig](SeriConfig.reader).map(SeriConfig.validate)
}
