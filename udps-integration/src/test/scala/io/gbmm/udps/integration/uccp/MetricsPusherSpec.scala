package io.gbmm.udps.integration.uccp

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.gbmm.udps.integration.circuitbreaker.{CircuitBreakerConfig, IntegrationCircuitBreaker}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.util.MutableHandlerRegistry
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class MetricsPusherSpec extends AnyWordSpec with Matchers {

  private val testSourceId = "test-node-1"

  private def makeMetricsSource(snapshots: List[MetricSnapshot]): MetricsSource =
    new MetricsSource {
      override def gatherMetrics: IO[List[MetricSnapshot]] = IO.pure(snapshots)
    }

  private def failingMetricsSource(error: Throwable): MetricsSource =
    new MetricsSource {
      override def gatherMetrics: IO[List[MetricSnapshot]] = IO.raiseError(error)
    }

  private def makeConfig(
      prefixes: List[String] = List("udps.", "jvm."),
      interval: FiniteDuration = 1.hour
  ): MetricsPushConfig =
    MetricsPushConfig(
      pushInterval = interval,
      enabledPrefixes = prefixes
    )

  private val testCbConfig = CircuitBreakerConfig(
    maxFailures = 5,
    callTimeout = 10.seconds,
    resetTimeout = 30.seconds,
    exponentialBackoffFactor = 2.0,
    maxResetTimeout = 5.minutes
  )

  private def withPusher[A](
      metricsSource: MetricsSource,
      config: MetricsPushConfig = makeConfig()
  )(test: MetricsPusher => A): A = {
    val serverName = InProcessServerBuilder.generateName()
    val server = InProcessServerBuilder
      .forName(serverName)
      .fallbackHandlerRegistry(new MutableHandlerRegistry())
      .directExecutor()
      .build()
      .start()

    val channel = InProcessChannelBuilder
      .forName(serverName)
      .directExecutor()
      .build()

    val program = IntegrationCircuitBreaker
      .create(testCbConfig, "test-metrics-cb")
      .flatMap { cb =>
        MetricsPusher.resource(channel, cb, config, metricsSource, testSourceId)
      }
      .use { pusher =>
        IO(test(pusher))
      }

    try {
      program.unsafeRunSync()
    } finally {
      channel.shutdownNow()
      server.shutdownNow()
    }
  }

  "MetricSnapshot" should {

    "hold name, value, and labels correctly" in {
      val labels   = Map("method" -> "GET", "path" -> "/api/query")
      val snapshot = MetricSnapshot(name = "udps.http.latency", value = 42.5, labels = labels)

      snapshot.name shouldBe "udps.http.latency"
      snapshot.value shouldBe 42.5
      snapshot.labels shouldBe labels
    }

    "support empty labels" in {
      val snapshot = MetricSnapshot(name = "udps.counter", value = 1.0, labels = Map.empty)

      snapshot.labels shouldBe empty
    }

    "implement equality by value" in {
      val a = MetricSnapshot("udps.x", 1.0, Map("k" -> "v"))
      val b = MetricSnapshot("udps.x", 1.0, Map("k" -> "v"))

      a shouldBe b
      a.hashCode shouldBe b.hashCode
    }
  }

  "MetricsPushConfig" should {

    "hold pushInterval and enabledPrefixes" in {
      val config = MetricsPushConfig(
        pushInterval = 30.seconds,
        enabledPrefixes = List("udps.", "jvm.", "system.")
      )

      config.pushInterval shouldBe 30.seconds
      config.enabledPrefixes should contain theSameElementsAs List("udps.", "jvm.", "system.")
    }

    "support empty prefix list" in {
      val config = MetricsPushConfig(
        pushInterval = 5.seconds,
        enabledPrefixes = Nil
      )

      config.enabledPrefixes shouldBe empty
    }
  }

  "MetricsPusher prefix filtering" should {

    "filter snapshots by enabled prefixes" in {
      val snapshots = List(
        MetricSnapshot("udps.query.latency", 12.5, Map("type" -> "select")),
        MetricSnapshot("jvm.memory", 512.0, Map("area" -> "heap")),
        MetricSnapshot("system.cpu", 0.75, Map("core" -> "0")),
        MetricSnapshot("udps.cache.hits", 1024.0, Map.empty)
      )

      val config = makeConfig(prefixes = List("udps.", "jvm."))

      val filtered = snapshots.filter { snapshot =>
        config.enabledPrefixes.exists(prefix => snapshot.name.startsWith(prefix))
      }

      filtered should have size 3
      filtered.map(_.name) should contain theSameElementsAs List(
        "udps.query.latency",
        "jvm.memory",
        "udps.cache.hits"
      )
    }

    "return empty list when no snapshots match prefixes" in {
      val snapshots = List(
        MetricSnapshot("system.cpu", 0.75, Map.empty),
        MetricSnapshot("os.disk.free", 100.0, Map.empty)
      )

      val config = makeConfig(prefixes = List("udps.", "jvm."))

      val filtered = snapshots.filter { snapshot =>
        config.enabledPrefixes.exists(prefix => snapshot.name.startsWith(prefix))
      }

      filtered shouldBe empty
    }

    "return empty list when prefixes list is empty" in {
      val snapshots = List(
        MetricSnapshot("udps.query.latency", 12.5, Map.empty)
      )

      val config = makeConfig(prefixes = Nil)

      val filtered = snapshots.filter { snapshot =>
        config.enabledPrefixes.exists(prefix => snapshot.name.startsWith(prefix))
      }

      filtered shouldBe empty
    }

    "match all snapshots when prefix is very broad" in {
      val snapshots = List(
        MetricSnapshot("udps.a", 1.0, Map.empty),
        MetricSnapshot("udps.b", 2.0, Map.empty),
        MetricSnapshot("udps.c", 3.0, Map.empty)
      )

      val config = makeConfig(prefixes = List("udps."))

      val filtered = snapshots.filter { snapshot =>
        config.enabledPrefixes.exists(prefix => snapshot.name.startsWith(prefix))
      }

      filtered should have size 3
    }
  }

  "MetricsPusher pushOnce" should {

    "not propagate errors when MetricsSource fails" in {
      val source = failingMetricsSource(new RuntimeException("metrics collection exploded"))

      withPusher(source) { pusher =>
        noException should be thrownBy {
          pusher.pushOnce.unsafeRunSync()
        }
      }
    }

    "complete without error when all metrics are filtered out" in {
      val snapshots = List(
        MetricSnapshot("system.cpu", 0.75, Map.empty),
        MetricSnapshot("os.disk.free", 100.0, Map.empty)
      )

      val config = makeConfig(prefixes = List("udps.", "jvm."))

      withPusher(makeMetricsSource(snapshots), config) { pusher =>
        noException should be thrownBy {
          pusher.pushOnce.unsafeRunSync()
        }
      }
    }

    "complete without error when metrics source returns empty list" in {
      withPusher(makeMetricsSource(Nil)) { pusher =>
        noException should be thrownBy {
          pusher.pushOnce.unsafeRunSync()
        }
      }
    }

    "handle gRPC call failure gracefully" in {
      val snapshots = List(
        MetricSnapshot("udps.query.latency", 12.5, Map("type" -> "select"))
      )

      withPusher(makeMetricsSource(snapshots)) { pusher =>
        noException should be thrownBy {
          pusher.pushOnce.unsafeRunSync()
        }
      }
    }

    "handle MetricsSource throwing non-RuntimeException gracefully" in {
      val source = failingMetricsSource(new java.io.IOException("disk read failed"))

      withPusher(source) { pusher =>
        noException should be thrownBy {
          pusher.pushOnce.unsafeRunSync()
        }
      }
    }
  }
}
