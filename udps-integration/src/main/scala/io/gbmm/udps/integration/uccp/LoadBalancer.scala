package io.gbmm.udps.integration.uccp

import cats.effect.{IO, Ref}
import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent.atomic.AtomicLong

// ---------------------------------------------------------------------------
// Load Balancer -- round-robin across healthy ServiceInstances
// ---------------------------------------------------------------------------

final class LoadBalancer private (
    instancesRef: Ref[IO, Vector[ServiceInstance]],
    counter: AtomicLong
) extends LazyLogging {

  /** Select the next healthy instance using round-robin. Returns None if no
    * healthy instances are available.
    */
  def nextInstance: IO[Option[ServiceInstance]] =
    instancesRef.get.map { instances =>
      val healthy = instances.filter(_.healthy)
      if (healthy.isEmpty) {
        logger.warn("No healthy instances available for load balancing")
        None
      } else {
        val idx = math.abs(counter.getAndIncrement() % healthy.size).toInt
        Some(healthy(idx))
      }
    }

  /** Replace the current instance list with a fresh snapshot from discovery.
    */
  def updateInstances(instances: List[ServiceInstance]): IO[Unit] =
    instancesRef.set(instances.toVector) *>
      IO(
        logger.info(
          s"Updated load balancer instances: total=${instances.size}, " +
            s"healthy=${instances.count(_.healthy)}"
        )
      )

  /** Mark a specific instance as unhealthy by address and port. */
  def markUnhealthy(address: String, port: Int): IO[Unit] =
    instancesRef
      .update { instances =>
        instances.map { inst =>
          if (inst.address == address && inst.port == port) inst.copy(healthy = false)
          else inst
        }
      }
      .flatTap(_ => IO(logger.warn(s"Marked instance $address:$port as unhealthy")))

  /** Mark a specific instance as healthy by address and port. */
  def markHealthy(address: String, port: Int): IO[Unit] =
    instancesRef
      .update { instances =>
        instances.map { inst =>
          if (inst.address == address && inst.port == port) inst.copy(healthy = true)
          else inst
        }
      }
      .flatTap(_ => IO(logger.info(s"Marked instance $address:$port as healthy")))
}

// ---------------------------------------------------------------------------
// Companion -- factory
// ---------------------------------------------------------------------------

object LoadBalancer extends LazyLogging {

  def create: IO[LoadBalancer] =
    for {
      ref <- Ref.of[IO, Vector[ServiceInstance]](Vector.empty)
      lb   = new LoadBalancer(ref, new AtomicLong(0L))
      _   <- IO(logger.info("LoadBalancer created"))
    } yield lb
}
