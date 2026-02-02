package io.gbmm.udps.integration.uccp

import cats.effect.{IO, Resource}
import cats.effect.std.Queue
import com.typesafe.scalalogging.LazyLogging
import fs2.Stream
import io.gbmm.udps.integration.circuitbreaker.IntegrationCircuitBreaker
import io.grpc.ManagedChannel
import io.grpc.stub.StreamObserver
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import uccp.coordination.coordination._
import uccp.common.common.HealthStatus

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

final case class ServiceDiscoveryConfig(
    uccpHost: String,
    uccpPort: Int,
    serviceName: String,
    serviceType: String,
    version: String,
    address: String,
    port: Int,
    protocol: String,
    heartbeatInterval: FiniteDuration
)

object ServiceDiscoveryConfig {
  implicit val reader: ConfigReader[ServiceDiscoveryConfig] = deriveReader[ServiceDiscoveryConfig]
}

// ---------------------------------------------------------------------------
// Domain model
// ---------------------------------------------------------------------------

final case class ServiceInstance(
    serviceName: String,
    address: String,
    port: Int,
    metadata: Map[String, String],
    healthy: Boolean
)

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

final class ServiceDiscoveryClient private (
    channel: ManagedChannel,
    circuitBreaker: IntegrationCircuitBreaker,
    config: ServiceDiscoveryConfig
) extends LazyLogging {

  private[uccp] val stub: ServiceDiscoveryGrpc.ServiceDiscoveryStub =
    new ServiceDiscoveryGrpc.ServiceDiscoveryStub(channel)

  private[uccp] val registeredServiceId: AtomicReference[String] =
    new AtomicReference[String]("")

  /** Register this UDPS service with UCCP. */
  def registerService: IO[Unit] =
    circuitBreaker.protect {
      val request = RegisterRequest(
        serviceName = config.serviceName,
        serviceType = config.serviceType,
        version = config.version,
        address = config.address,
        port = config.port,
        protocol = config.protocol
      )
      IO.fromFuture(IO(stub.register(request))).flatMap { response =>
        IO {
          registeredServiceId.set(response.serviceId)
          logger.info(
            s"Registered service '${config.serviceName}' with UCCP, serviceId=${response.serviceId}"
          )
        }
      }
    }

  /** Deregister this UDPS service from UCCP. */
  def deregisterService: IO[Unit] =
    circuitBreaker.protect {
      val serviceId = registeredServiceId.get()
      if (serviceId.isEmpty) {
        IO(logger.warn("Attempted to deregister but no serviceId is registered"))
      } else {
        val request = DeregisterRequest(serviceId = serviceId)
        IO.fromFuture(IO(stub.deregister(request))).flatMap { response =>
          IO {
            logger.info(
              s"Deregistered service '$serviceId' from UCCP, success=${response.success}"
            )
            registeredServiceId.set("")
          }
        }
      }
    }

  /** Discover instances of a named service via UCCP. */
  def discoverService(name: String): IO[List[ServiceInstance]] =
    circuitBreaker.protect {
      val request = DiscoverRequest(serviceName = name)
      IO.fromFuture(IO(stub.discover(request))).map { response =>
        response.services.map(toServiceInstance).toList
      }
    }

  /** Subscribe to service change events as an fs2 Stream. */
  def watchStream: Stream[IO, ServiceEvent] =
    Stream.eval(Queue.unbounded[IO, Option[ServiceEvent]]).flatMap { queue =>
      val startObserver: IO[Unit] = IO {
        val request = WatchServicesRequest(serviceName = config.serviceName)
        val observer = new StreamObserver[ServiceEvent] {
          override def onNext(value: ServiceEvent): Unit = {
            val _ = queue.tryOffer(Some(value))
            logger.debug(s"Received service event: type=${value.`type`}")
          }

          override def onError(t: Throwable): Unit = {
            logger.error(s"Watch stream error: ${t.getMessage}", t)
            val _ = queue.tryOffer(None)
          }

          override def onCompleted(): Unit = {
            logger.info("Watch stream completed by server")
            val _ = queue.tryOffer(None)
          }
        }
        stub.watchServices(request, observer)
      }

      Stream.eval(startObserver) >>
        Stream
          .fromQueueNoneTerminated(queue)
    }

  /** Periodic heartbeat stream that signals liveness to UCCP. */
  def heartbeatStream: Stream[IO, Unit] =
    Stream
      .awakeEvery[IO](config.heartbeatInterval)
      .evalMap { _ =>
        sendHeartbeat
      }

  private def sendHeartbeat: IO[Unit] = {
    val serviceId = registeredServiceId.get()
    if (serviceId.isEmpty) {
      IO(logger.debug("Skipping heartbeat -- no registered serviceId"))
    } else {
      circuitBreaker.protect {
        val request = HeartbeatRequest(
          serviceId = serviceId,
          health = HealthStatus.HEALTH_STATUS_HEALTHY
        )
        IO.fromFuture(IO(stub.heartbeat(request))).flatMap { response =>
          IO(logger.debug(s"Heartbeat sent, success=${response.success}"))
        }
      }
    }
  }

  private def toServiceInstance(
      proto: uccp.coordination.coordination.ServiceInstance
  ): ServiceInstance = {
    val isHealthy = proto.health match {
      case HealthStatus.HEALTH_STATUS_HEALTHY => true
      case _                                  => false
    }
    ServiceInstance(
      serviceName = proto.serviceName,
      address = proto.address,
      port = proto.port,
      metadata = proto.metadata,
      healthy = isHealthy
    )
  }
}

// ---------------------------------------------------------------------------
// Companion -- Resource factory
// ---------------------------------------------------------------------------

object ServiceDiscoveryClient extends LazyLogging {

  def resource(
      channel: ManagedChannel,
      circuitBreaker: IntegrationCircuitBreaker,
      config: ServiceDiscoveryConfig
  ): Resource[IO, ServiceDiscoveryClient] =
    Resource.make(
      IO {
        logger.info(
          s"Creating ServiceDiscoveryClient for '${config.serviceName}' " +
            s"targeting ${config.uccpHost}:${config.uccpPort}"
        )
        new ServiceDiscoveryClient(channel, circuitBreaker, config)
      }
    ) { client =>
      client.deregisterService.handleErrorWith { err =>
        IO(logger.error(s"Failed to deregister on shutdown: ${err.getMessage}", err))
      }
    }
}
