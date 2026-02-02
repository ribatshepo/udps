package io.gbmm.udps.api.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

final case class HttpConfig(
  host: String,
  port: Int,
  idleTimeout: FiniteDuration,
  responseHeaderTimeout: FiniteDuration
)

final case class GrpcConfig(
  port: Int,
  maxInboundMessageSize: Int
)

final case class CorsConfig(
  allowedOrigins: List[String],
  allowedMethods: List[String],
  allowedHeaders: List[String],
  maxAge: FiniteDuration
)

final case class CatalogDbConfig(
  jdbcUrl: String,
  username: String,
  password: String,
  maximumPoolSize: Int,
  minimumIdle: Int,
  connectionTimeoutMs: Long
)

final case class AuthConfig(
  cacheTtl: FiniteDuration,
  maxCacheSize: Int
)

final case class UspGrpcConfig(
  host: String,
  port: Int
)

final case class CircuitBreakerConfig(
  maxFailures: Int,
  callTimeout: FiniteDuration,
  resetTimeout: FiniteDuration,
  exponentialBackoffFactor: Double,
  maxResetTimeout: FiniteDuration
)

final case class StoragePathConfig(
  hotPath: String,
  warmPath: String,
  coldPath: String,
  defaultCompression: String,
  defaultRowGroupSize: Long,
  workerThreads: Int
)

final case class MinioApiConfig(
  endpoint: String,
  accessKey: String,
  secretKey: String,
  bucket: String
)

final case class ApiConfig(
  http: HttpConfig,
  grpc: GrpcConfig,
  cors: CorsConfig,
  catalogDb: CatalogDbConfig,
  auth: AuthConfig,
  uspGrpc: UspGrpcConfig,
  circuitBreaker: CircuitBreakerConfig,
  storage: StoragePathConfig,
  minio: MinioApiConfig,
  version: String
)

object ApiConfig {
  implicit val httpReader: ConfigReader[HttpConfig] = deriveReader[HttpConfig]
  implicit val grpcReader: ConfigReader[GrpcConfig] = deriveReader[GrpcConfig]
  implicit val corsReader: ConfigReader[CorsConfig] = deriveReader[CorsConfig]
  implicit val catalogDbReader: ConfigReader[CatalogDbConfig] = deriveReader[CatalogDbConfig]
  implicit val authReader: ConfigReader[AuthConfig] = deriveReader[AuthConfig]
  implicit val uspGrpcReader: ConfigReader[UspGrpcConfig] = deriveReader[UspGrpcConfig]
  implicit val cbReader: ConfigReader[CircuitBreakerConfig] = deriveReader[CircuitBreakerConfig]
  implicit val storageReader: ConfigReader[StoragePathConfig] = deriveReader[StoragePathConfig]
  implicit val minioReader: ConfigReader[MinioApiConfig] = deriveReader[MinioApiConfig]
  implicit val reader: ConfigReader[ApiConfig] = deriveReader[ApiConfig]
}
