package io.gbmm.udps.integration.uccp

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import uccp.coordination.coordination.HeartbeatRequest

import scala.concurrent.duration._

class HealthReporterComponentSpec extends AnyWordSpec with Matchers {

  private val testConfig = HealthReportingConfig(
    reportInterval = 1.second,
    unhealthyDiskThreshold = 90.0,
    unhealthyErrorRateThreshold = 5.0
  )

  private val healthyDetails = HealthDetails(
    serviceVersion = "1.0.0",
    uptimeSeconds = 3600L,
    activeQueries = 5,
    storageUsagePercent = 45.0,
    errorRate = 0.1,
    databaseReachable = true
  )

  private def makeReporter(
      sender: HeartbeatSender = noOpSender,
      healthCheck: HealthCheck = defaultHealthCheck
  ): HealthReporter =
    HealthReporter.create(sender, testConfig, healthCheck, IO.pure("test-service-id"))

  private val noOpSender: HeartbeatSender = new HeartbeatSender {
    override def sendHeartbeat(request: HeartbeatRequest): IO[Unit] = IO.unit
  }

  private val defaultHealthCheck: HealthCheck = new HealthCheck {
    override def check: IO[HealthDetails] = IO.pure(healthyDetails)
  }

  private def componentHealthCheck(components: List[ComponentHealth]): HealthCheck =
    new HealthCheck {
      override def check: IO[HealthDetails] = IO.pure(healthyDetails)
      override def checkComponents: IO[List[ComponentHealth]] = IO.pure(components)
    }

  "determineComponentStatus" should {

    "return Healthy when all components are Healthy" in {
      val reporter = makeReporter()
      val components = List(
        ComponentHealth("db", HealthReportStatus.Healthy, Map.empty),
        ComponentHealth("cache", HealthReportStatus.Healthy, Map.empty),
        ComponentHealth("storage", HealthReportStatus.Healthy, Map.empty)
      )

      reporter.determineComponentStatus(components) shouldBe HealthReportStatus.Healthy
    }

    "return Unhealthy when any component is Unhealthy" in {
      val reporter = makeReporter()
      val components = List(
        ComponentHealth("db", HealthReportStatus.Unhealthy(List("connection refused")), Map.empty),
        ComponentHealth("cache", HealthReportStatus.Healthy, Map.empty)
      )

      val result = reporter.determineComponentStatus(components)
      result shouldBe a[HealthReportStatus.Unhealthy]
      result.asInstanceOf[HealthReportStatus.Unhealthy].reasons should contain("db: connection refused")
    }

    "return Degraded when any component is Degraded and none Unhealthy" in {
      val reporter = makeReporter()
      val components = List(
        ComponentHealth("cache", HealthReportStatus.Degraded(List("high latency")), Map.empty),
        ComponentHealth("db", HealthReportStatus.Healthy, Map.empty)
      )

      val result = reporter.determineComponentStatus(components)
      result shouldBe a[HealthReportStatus.Degraded]
      result.asInstanceOf[HealthReportStatus.Degraded].reasons should contain("cache: high latency")
    }

    "return Healthy for an empty component list" in {
      val reporter = makeReporter()

      reporter.determineComponentStatus(Nil) shouldBe HealthReportStatus.Healthy
    }

    "aggregate reasons from multiple Unhealthy components" in {
      val reporter = makeReporter()
      val components = List(
        ComponentHealth("db", HealthReportStatus.Unhealthy(List("connection refused")), Map.empty),
        ComponentHealth("storage", HealthReportStatus.Unhealthy(List("disk full")), Map.empty),
        ComponentHealth("cache", HealthReportStatus.Healthy, Map.empty)
      )

      val result = reporter.determineComponentStatus(components)
      result shouldBe a[HealthReportStatus.Unhealthy]
      val reasons = result.asInstanceOf[HealthReportStatus.Unhealthy].reasons
      reasons should have size 2
      reasons should contain("db: connection refused")
      reasons should contain("storage: disk full")
    }

    "aggregate reasons from multiple Degraded components" in {
      val reporter = makeReporter()
      val components = List(
        ComponentHealth("cache", HealthReportStatus.Degraded(List("high latency")), Map.empty),
        ComponentHealth("search", HealthReportStatus.Degraded(List("index stale")), Map.empty),
        ComponentHealth("db", HealthReportStatus.Healthy, Map.empty)
      )

      val result = reporter.determineComponentStatus(components)
      result shouldBe a[HealthReportStatus.Degraded]
      val reasons = result.asInstanceOf[HealthReportStatus.Degraded].reasons
      reasons should have size 2
      reasons should contain("cache: high latency")
      reasons should contain("search: index stale")
    }

    "prefer Unhealthy over Degraded when both present" in {
      val reporter = makeReporter()
      val components = List(
        ComponentHealth("db", HealthReportStatus.Unhealthy(List("connection refused")), Map.empty),
        ComponentHealth("cache", HealthReportStatus.Degraded(List("high latency")), Map.empty),
        ComponentHealth("storage", HealthReportStatus.Healthy, Map.empty)
      )

      val result = reporter.determineComponentStatus(components)
      result shouldBe a[HealthReportStatus.Unhealthy]
      val reasons = result.asInstanceOf[HealthReportStatus.Unhealthy].reasons
      reasons should contain("db: connection refused")
      reasons should not contain "cache: high latency"
    }
  }

  "formatComponentHealth via gatherAndSend" should {

    "include componentHealth key with JSON array in heartbeat metadata" in {
      var capturedRequest: Option[HeartbeatRequest] = None

      val capturingSender: HeartbeatSender = new HeartbeatSender {
        override def sendHeartbeat(request: HeartbeatRequest): IO[Unit] = IO {
          capturedRequest = Some(request)
        }
      }

      val components = List(
        ComponentHealth("db", HealthReportStatus.Healthy, Map.empty),
        ComponentHealth("cache", HealthReportStatus.Degraded(List("slow")), Map.empty)
      )

      val reporter = makeReporter(
        sender = capturingSender,
        healthCheck = componentHealthCheck(components)
      )

      reporter.reportStream.take(1).compile.drain.unsafeRunSync()

      capturedRequest shouldBe defined
      val metadata = capturedRequest.get.metadataUpdates
      metadata should contain key "componentHealth"

      val componentHealthJson = metadata("componentHealth")
      componentHealthJson should startWith("[")
      componentHealthJson should endWith("]")
      componentHealthJson should include("\"name\":\"db\"")
      componentHealthJson should include("\"status\":\"HEALTHY\"")
      componentHealthJson should include("\"name\":\"cache\"")
      componentHealthJson should include("\"status\":\"DEGRADED\"")
    }

    "not include componentHealth key when component list is empty" in {
      var capturedRequest: Option[HeartbeatRequest] = None

      val capturingSender: HeartbeatSender = new HeartbeatSender {
        override def sendHeartbeat(request: HeartbeatRequest): IO[Unit] = IO {
          capturedRequest = Some(request)
        }
      }

      val reporter = makeReporter(
        sender = capturingSender,
        healthCheck = defaultHealthCheck
      )

      reporter.reportStream.take(1).compile.drain.unsafeRunSync()

      capturedRequest shouldBe defined
      val metadata = capturedRequest.get.metadataUpdates
      metadata should not contain key("componentHealth")
    }
  }

  "backward compatibility" should {

    "use determineStatus when checkComponents returns empty list" in {
      var capturedRequest: Option[HeartbeatRequest] = None

      val capturingSender: HeartbeatSender = new HeartbeatSender {
        override def sendHeartbeat(request: HeartbeatRequest): IO[Unit] = IO {
          capturedRequest = Some(request)
        }
      }

      val oldStyleHealthCheck: HealthCheck = new HealthCheck {
        override def check: IO[HealthDetails] = IO.pure(healthyDetails)
      }

      val reporter = makeReporter(
        sender = capturingSender,
        healthCheck = oldStyleHealthCheck
      )

      reporter.reportStream.take(1).compile.drain.unsafeRunSync()

      capturedRequest shouldBe defined
      val metadata = capturedRequest.get.metadataUpdates
      metadata should not contain key("componentHealth")
      metadata should not contain key("unhealthyReasons")
      metadata should not contain key("degradedReasons")
    }

    "use determineStatus for old-style HealthCheck and report degraded correctly" in {
      var capturedRequest: Option[HeartbeatRequest] = None

      val capturingSender: HeartbeatSender = new HeartbeatSender {
        override def sendHeartbeat(request: HeartbeatRequest): IO[Unit] = IO {
          capturedRequest = Some(request)
        }
      }

      val degradedDetails = HealthDetails(
        serviceVersion = "1.0.0",
        uptimeSeconds = 3600L,
        activeQueries = 5,
        storageUsagePercent = 95.0,
        errorRate = 0.1,
        databaseReachable = true
      )

      val oldStyleHealthCheck: HealthCheck = new HealthCheck {
        override def check: IO[HealthDetails] = IO.pure(degradedDetails)
      }

      val reporter = makeReporter(
        sender = capturingSender,
        healthCheck = oldStyleHealthCheck
      )

      reporter.reportStream.take(1).compile.drain.unsafeRunSync()

      capturedRequest shouldBe defined
      val metadata = capturedRequest.get.metadataUpdates
      metadata should contain key "degradedReasons"
      metadata("degradedReasons") should include("Disk usage")
    }
  }
}
