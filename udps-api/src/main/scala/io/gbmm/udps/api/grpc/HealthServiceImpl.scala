package io.gbmm.udps.api.grpc

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.typesafe.scalalogging.LazyLogging
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.stub.StreamObserver
import udps.api.health_service.HealthServiceGrpc
import udps.api.health_service.{HealthCheckRequest, HealthCheckResponse}
import udps.api.health_service.HealthCheckResponse.ServingStatus

import scala.concurrent.Future

final class HealthServiceImpl(
    componentChecks: Map[String, IO[Boolean]],
    ioRuntime: IORuntime
) extends HealthServiceGrpc.HealthService
    with LazyLogging {

  private[this] implicit val runtime: IORuntime = ioRuntime

  override def check(request: HealthCheckRequest): Future[HealthCheckResponse] =
    runIO("Check") {
      if (request.service.isEmpty) {
        aggregateHealthCheck()
      } else {
        componentChecks.get(request.service) match {
          case Some(chk) =>
            chk.map { healthy =>
              HealthCheckResponse(
                status = if (healthy) ServingStatus.SERVING else ServingStatus.NOT_SERVING,
                details = Map("service" -> request.service)
              )
            }
          case None =>
            IO.pure(
              HealthCheckResponse(
                status = ServingStatus.SERVICE_UNKNOWN,
                details = Map("service" -> request.service)
              )
            )
        }
      }
    }

  override def watch(
      request: HealthCheckRequest,
      responseObserver: StreamObserver[HealthCheckResponse]
  ): Unit = {
    val effect: IO[HealthCheckResponse] = if (request.service.isEmpty) {
      aggregateHealthCheck()
    } else {
      componentChecks.get(request.service) match {
        case Some(chk) =>
          chk.map { healthy =>
            HealthCheckResponse(
              status = if (healthy) ServingStatus.SERVING else ServingStatus.NOT_SERVING,
              details = Map("service" -> request.service)
            )
          }
        case None =>
          IO.pure(
            HealthCheckResponse(
              status = ServingStatus.SERVICE_UNKNOWN,
              details = Map("service" -> request.service)
            )
          )
      }
    }

    effect.flatMap { response =>
      IO.delay {
        responseObserver.onNext(response)
        responseObserver.onCompleted()
      }
    }.handleErrorWith { err =>
      IO.delay {
        logger.error("Health watch failed", err)
        responseObserver.onError(
          Status.INTERNAL
            .withDescription(s"Health check failed: ${err.getMessage}")
            .asRuntimeException()
        )
      }
    }.unsafeRunAndForget()
  }

  // ---------------------------------------------------------------------------
  // Internal
  // ---------------------------------------------------------------------------

  private def aggregateHealthCheck(): IO[HealthCheckResponse] =
    if (componentChecks.isEmpty) {
      IO.pure(
        HealthCheckResponse(
          status = ServingStatus.SERVING,
          details = Map("message" -> "No components registered")
        )
      )
    } else {
      import cats.implicits._
      componentChecks.toList.traverse { case (name, chk) =>
        chk.map(healthy => name -> healthy).handleError { err =>
          logger.warn("Health check for component '{}' failed: {}", name, err.getMessage: Any)
          name -> false
        }
      }.map { results =>
        val details = results.map { case (name, healthy) =>
          name -> (if (healthy) "SERVING" else "NOT_SERVING")
        }.toMap
        val allHealthy = results.forall(_._2)
        HealthCheckResponse(
          status = if (allHealthy) ServingStatus.SERVING else ServingStatus.NOT_SERVING,
          details = details
        )
      }
    }

  private def runIO[A](rpcName: String)(effect: IO[A]): Future[A] =
    effect.handleErrorWith {
      case e: StatusRuntimeException => IO.raiseError(e)
      case e: Throwable =>
        logger.error(s"Unexpected error in $rpcName", e)
        IO.raiseError(
          Status.INTERNAL
            .withDescription(s"Internal error in $rpcName: ${e.getMessage}")
            .asRuntimeException()
        )
    }.unsafeToFuture()
}

object HealthServiceImpl {

  def apply(
      componentChecks: Map[String, IO[Boolean]],
      ioRuntime: IORuntime
  ): HealthServiceImpl =
    new HealthServiceImpl(componentChecks, ioRuntime)

  def noChecks(ioRuntime: IORuntime): HealthServiceImpl =
    new HealthServiceImpl(Map.empty, ioRuntime)
}
