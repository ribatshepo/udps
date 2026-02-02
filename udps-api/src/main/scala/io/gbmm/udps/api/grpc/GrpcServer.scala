package io.gbmm.udps.api.grpc

import cats.effect.{IO, Resource}
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.api.config.GrpcConfig
import io.grpc.{Server, ServerBuilder}
import udps.api.catalog_service.CatalogServiceGrpc
import udps.api.health_service.HealthServiceGrpc
import udps.api.query_service.QueryServiceGrpc
import udps.api.storage_service.StorageServiceGrpc

import scala.concurrent.ExecutionContext

object GrpcServer extends LazyLogging {

  def build(
      config: GrpcConfig,
      catalogService: CatalogServiceImpl,
      storageService: StorageServiceImpl,
      queryService: QueryServiceImpl,
      healthService: HealthServiceImpl
  ): Resource[IO, Server] = {
    val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

    Resource.make(
      IO {
        val server = ServerBuilder
          .forPort(config.port)
          .maxInboundMessageSize(config.maxInboundMessageSize)
          .addService(CatalogServiceGrpc.bindService(catalogService, ec))
          .addService(StorageServiceGrpc.bindService(storageService, ec))
          .addService(QueryServiceGrpc.bindService(queryService, ec))
          .addService(HealthServiceGrpc.bindService(healthService, ec))
          .build()
          .start()
        logger.info("gRPC server started on port {}", config.port.toString)
        server
      }
    )(server =>
      IO {
        logger.info("Shutting down gRPC server")
        server.shutdown()
      }.void
    )
  }
}
