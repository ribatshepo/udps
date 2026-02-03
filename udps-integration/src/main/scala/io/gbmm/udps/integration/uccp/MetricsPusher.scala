package io.gbmm.udps.integration.uccp

import cats.effect.{IO, Resource}
import com.typesafe.scalalogging.LazyLogging
import fs2.Stream
import io.gbmm.udps.integration.circuitbreaker.IntegrationCircuitBreaker
import io.grpc.ManagedChannel
import uccp.monitoring.monitoring.{MonitoringServiceGrpc, PushMetricsRequest, MetricSample => ProtoMetricSample}
import com.google.protobuf.timestamp.Timestamp

// ---------------------------------------------------------------------------
// Domain model
// ---------------------------------------------------------------------------

final case class MetricSnapshot(
    name: String,
    value: Double,
    labels: Map[String, String]
)

// ---------------------------------------------------------------------------
// Metrics source abstraction
// ---------------------------------------------------------------------------

trait MetricsSource {
  def gatherMetrics: IO[List[MetricSnapshot]]
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

final class MetricsPusher private (
    channel: ManagedChannel,
    circuitBreaker: IntegrationCircuitBreaker,
    config: MetricsPushConfig,
    metricsSource: MetricsSource,
    sourceId: String
) extends LazyLogging {

  private val stub: MonitoringServiceGrpc.MonitoringServiceStub =
    new MonitoringServiceGrpc.MonitoringServiceStub(channel)

  /** Continuous stream that pushes metrics at the configured interval. */
  def pushStream: Stream[IO, Unit] =
    Stream
      .fixedRate[IO](config.pushInterval)
      .evalMap(_ => pushOnce)

  private[uccp] def pushOnce: IO[Unit] =
    metricsSource.gatherMetrics.flatMap { snapshots =>
      val filtered = snapshots.filter { snapshot =>
        config.enabledPrefixes.exists(prefix => snapshot.name.startsWith(prefix))
      }

      if (filtered.isEmpty) {
        IO(logger.debug("No metrics matched enabled prefixes, skipping push"))
      } else {
        val now = java.time.Instant.now()
        val ts = Timestamp(seconds = now.getEpochSecond, nanos = now.getNano)

        val protoSamples = filtered.map { snapshot =>
          ProtoMetricSample(
            metricName = snapshot.name,
            value = snapshot.value,
            labels = snapshot.labels,
            timestamp = Some(ts)
          )
        }

        val request = PushMetricsRequest(
          sourceId = sourceId,
          metricNamespace = "udps",
          samples = protoSamples
        )

        circuitBreaker.protect {
          IO.fromFuture(IO(stub.pushMetrics(request))).flatMap { response =>
            IO(logger.debug(s"Pushed ${filtered.size} metrics, success=${response.success}"))
          }
        }
      }
    }.handleErrorWith { err =>
      IO(logger.error(s"Failed to push metrics: ${err.getMessage}", err))
    }
}

// ---------------------------------------------------------------------------
// Companion -- Resource factory
// ---------------------------------------------------------------------------

object MetricsPusher extends LazyLogging {

  def resource(
      channel: ManagedChannel,
      circuitBreaker: IntegrationCircuitBreaker,
      config: MetricsPushConfig,
      metricsSource: MetricsSource,
      sourceId: String
  ): Resource[IO, MetricsPusher] =
    Resource.make(
      IO {
        logger.info(
          s"Creating MetricsPusher for sourceId='$sourceId' " +
            s"with pushInterval=${config.pushInterval}, " +
            s"enabledPrefixes=${config.enabledPrefixes.mkString("[", ", ", "]")}"
        )
        new MetricsPusher(channel, circuitBreaker, config, metricsSource, sourceId)
      }.flatMap { pusher =>
        pusher.pushStream.compile.drain.start.map(fiber => (pusher, fiber))
      }
    ) { case (_, fiber) =>
      fiber.cancel *> IO(logger.info("MetricsPusher shut down, background fiber cancelled"))
    }.map { case (pusher, _) => pusher }
}
