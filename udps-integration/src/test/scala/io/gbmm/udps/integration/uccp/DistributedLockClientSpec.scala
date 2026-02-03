package io.gbmm.udps.integration.uccp

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.protobuf.timestamp.Timestamp
import io.gbmm.udps.integration.circuitbreaker.{CircuitBreakerConfig, IntegrationCircuitBreaker}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.util.MutableHandlerRegistry
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uccp.common.common.Response
import uccp.coordination.coordination._

import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.{global => ec}
import scala.concurrent.Future
import scala.concurrent.duration._

class DistributedLockClientSpec extends AnyWordSpec with Matchers {

  private val testCbConfig = CircuitBreakerConfig(
    maxFailures = 5,
    callTimeout = 10.seconds,
    resetTimeout = 30.seconds,
    exponentialBackoffFactor = 2.0,
    maxResetTimeout = 5.minutes
  )

  private val testLockConfig = LockConfig.withDefaults("localhost", 50000, "test-namespace")

  private val testLockName = "test-lock"
  private val testOwner    = "test-owner"
  private val testTtl      = 30
  private val testToken    = "lock-token-abc-123"

  private val futureExpirySeconds = Instant.now().plusSeconds(60).getEpochSecond

  private def withLockClient[A](serviceImpl: LockServiceGrpc.LockService)(test: DistributedLockClient => A): A = {
    val serverName = InProcessServerBuilder.generateName()
    val serviceDefinition = LockServiceGrpc.bindService(serviceImpl, ec)

    val registry = new MutableHandlerRegistry()
    registry.addService(serviceDefinition)

    val server = InProcessServerBuilder
      .forName(serverName)
      .fallbackHandlerRegistry(registry)
      .directExecutor()
      .build()
      .start()

    val channel = InProcessChannelBuilder
      .forName(serverName)
      .directExecutor()
      .build()

    val program = IntegrationCircuitBreaker
      .create(testCbConfig, "test-lock-cb")
      .flatMap { cb =>
        DistributedLockClient.resource(channel, cb, testLockConfig)
      }
      .use { client =>
        IO(test(client))
      }

    try {
      program.unsafeRunSync()
    } finally {
      channel.shutdownNow()
      server.shutdownNow()
    }
  }

  private def noOpLockService: LockServiceGrpc.LockService = new LockServiceGrpc.LockService {
    override def acquireLock(request: AcquireLockRequest): Future[AcquireLockResponse] =
      Future.failed(new UnsupportedOperationException("not mocked"))
    override def releaseLock(request: ReleaseLockRequest): Future[Response] =
      Future.failed(new UnsupportedOperationException("not mocked"))
    override def renewLock(request: RenewLockRequest): Future[Response] =
      Future.failed(new UnsupportedOperationException("not mocked"))
    override def tryLock(request: TryLockRequest): Future[TryLockResponse] =
      Future.failed(new UnsupportedOperationException("not mocked"))
    override def acquireReadLock(request: AcquireReadLockRequest): Future[AcquireLockResponse] =
      Future.failed(new UnsupportedOperationException("not mocked"))
    override def acquireSemaphore(request: AcquireSemaphoreRequest): Future[AcquireSemaphoreResponse] =
      Future.failed(new UnsupportedOperationException("not mocked"))
    override def releaseSemaphore(request: ReleaseSemaphoreRequest): Future[Response] =
      Future.failed(new UnsupportedOperationException("not mocked"))
    override def waitBarrier(request: WaitBarrierRequest): Future[WaitBarrierResponse] =
      Future.failed(new UnsupportedOperationException("not mocked"))
    override def listLocks(request: ListLocksRequest): Future[ListLocksResponse] =
      Future.failed(new UnsupportedOperationException("not mocked"))
  }

  "acquireLock" should {

    "return LockHandle when lock is successfully acquired" in {
      val service = new LockServiceGrpc.LockService {
        override def acquireLock(request: AcquireLockRequest): Future[AcquireLockResponse] = {
          request.lockName shouldBe testLockName
          request.namespace shouldBe "test-namespace"
          request.owner shouldBe testOwner
          Future.successful(
            AcquireLockResponse(
              acquired = true,
              lockToken = testToken,
              expiresAt = Some(Timestamp(seconds = futureExpirySeconds))
            )
          )
        }
        override def releaseLock(request: ReleaseLockRequest): Future[Response] =
          noOpLockService.releaseLock(request)
        override def renewLock(request: RenewLockRequest): Future[Response] =
          noOpLockService.renewLock(request)
        override def tryLock(request: TryLockRequest): Future[TryLockResponse] =
          noOpLockService.tryLock(request)
        override def acquireReadLock(request: AcquireReadLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireReadLock(request)
        override def acquireSemaphore(request: AcquireSemaphoreRequest): Future[AcquireSemaphoreResponse] =
          noOpLockService.acquireSemaphore(request)
        override def releaseSemaphore(request: ReleaseSemaphoreRequest): Future[Response] =
          noOpLockService.releaseSemaphore(request)
        override def waitBarrier(request: WaitBarrierRequest): Future[WaitBarrierResponse] =
          noOpLockService.waitBarrier(request)
        override def listLocks(request: ListLocksRequest): Future[ListLocksResponse] =
          noOpLockService.listLocks(request)
      }

      withLockClient(service) { client =>
        val handle = client.acquireLock(testLockName, testOwner, testTtl).unsafeRunSync()

        handle.lockToken shouldBe testToken
        handle.lockName shouldBe testLockName
        handle.owner shouldBe testOwner
        handle.namespace shouldBe "test-namespace"
        handle.expiresAt.getEpochSecond shouldBe futureExpirySeconds
        handle.fenceToken should be > 0L
      }
    }

    "raise LockNotAcquiredException when lock is held by another" in {
      val service = new LockServiceGrpc.LockService {
        override def acquireLock(request: AcquireLockRequest): Future[AcquireLockResponse] =
          Future.successful(AcquireLockResponse(acquired = false))
        override def releaseLock(request: ReleaseLockRequest): Future[Response] =
          noOpLockService.releaseLock(request)
        override def renewLock(request: RenewLockRequest): Future[Response] =
          noOpLockService.renewLock(request)
        override def tryLock(request: TryLockRequest): Future[TryLockResponse] =
          noOpLockService.tryLock(request)
        override def acquireReadLock(request: AcquireReadLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireReadLock(request)
        override def acquireSemaphore(request: AcquireSemaphoreRequest): Future[AcquireSemaphoreResponse] =
          noOpLockService.acquireSemaphore(request)
        override def releaseSemaphore(request: ReleaseSemaphoreRequest): Future[Response] =
          noOpLockService.releaseSemaphore(request)
        override def waitBarrier(request: WaitBarrierRequest): Future[WaitBarrierResponse] =
          noOpLockService.waitBarrier(request)
        override def listLocks(request: ListLocksRequest): Future[ListLocksResponse] =
          noOpLockService.listLocks(request)
      }

      withLockClient(service) { client =>
        val ex = the[LockNotAcquiredException] thrownBy {
          client.acquireLock(testLockName, testOwner, testTtl).unsafeRunSync()
        }
        ex.lockName shouldBe testLockName
      }
    }
  }

  "releaseLock" should {

    "succeed when server confirms release" in {
      val service = new LockServiceGrpc.LockService {
        override def acquireLock(request: AcquireLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireLock(request)
        override def releaseLock(request: ReleaseLockRequest): Future[Response] = {
          request.lockToken shouldBe testToken
          request.lockName shouldBe testLockName
          request.namespace shouldBe "test-namespace"
          Future.successful(Response(success = true, message = "released"))
        }
        override def renewLock(request: RenewLockRequest): Future[Response] =
          noOpLockService.renewLock(request)
        override def tryLock(request: TryLockRequest): Future[TryLockResponse] =
          noOpLockService.tryLock(request)
        override def acquireReadLock(request: AcquireReadLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireReadLock(request)
        override def acquireSemaphore(request: AcquireSemaphoreRequest): Future[AcquireSemaphoreResponse] =
          noOpLockService.acquireSemaphore(request)
        override def releaseSemaphore(request: ReleaseSemaphoreRequest): Future[Response] =
          noOpLockService.releaseSemaphore(request)
        override def waitBarrier(request: WaitBarrierRequest): Future[WaitBarrierResponse] =
          noOpLockService.waitBarrier(request)
        override def listLocks(request: ListLocksRequest): Future[ListLocksResponse] =
          noOpLockService.listLocks(request)
      }

      withLockClient(service) { client =>
        val handle = LockHandle(
          lockToken = testToken,
          lockName = testLockName,
          owner = testOwner,
          namespace = "test-namespace",
          acquiredAt = Instant.now(),
          expiresAt = Instant.now().plusSeconds(60),
          fenceToken = 1L
        )

        noException should be thrownBy {
          client.releaseLock(handle).unsafeRunSync()
        }
      }
    }

    "raise LockReleaseException on server rejection" in {
      val service = new LockServiceGrpc.LockService {
        override def acquireLock(request: AcquireLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireLock(request)
        override def releaseLock(request: ReleaseLockRequest): Future[Response] =
          Future.successful(Response(success = false, message = "lock expired"))
        override def renewLock(request: RenewLockRequest): Future[Response] =
          noOpLockService.renewLock(request)
        override def tryLock(request: TryLockRequest): Future[TryLockResponse] =
          noOpLockService.tryLock(request)
        override def acquireReadLock(request: AcquireReadLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireReadLock(request)
        override def acquireSemaphore(request: AcquireSemaphoreRequest): Future[AcquireSemaphoreResponse] =
          noOpLockService.acquireSemaphore(request)
        override def releaseSemaphore(request: ReleaseSemaphoreRequest): Future[Response] =
          noOpLockService.releaseSemaphore(request)
        override def waitBarrier(request: WaitBarrierRequest): Future[WaitBarrierResponse] =
          noOpLockService.waitBarrier(request)
        override def listLocks(request: ListLocksRequest): Future[ListLocksResponse] =
          noOpLockService.listLocks(request)
      }

      withLockClient(service) { client =>
        val handle = LockHandle(
          lockToken = testToken,
          lockName = testLockName,
          owner = testOwner,
          namespace = "test-namespace",
          acquiredAt = Instant.now(),
          expiresAt = Instant.now().plusSeconds(60),
          fenceToken = 1L
        )

        val ex = the[LockReleaseException] thrownBy {
          client.releaseLock(handle).unsafeRunSync()
        }
        ex.lockName shouldBe testLockName
        ex.reason shouldBe "lock expired"
      }
    }
  }

  "renewLock" should {

    "return updated LockHandle with new expiry" in {
      val service = new LockServiceGrpc.LockService {
        override def acquireLock(request: AcquireLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireLock(request)
        override def releaseLock(request: ReleaseLockRequest): Future[Response] =
          noOpLockService.releaseLock(request)
        override def renewLock(request: RenewLockRequest): Future[Response] = {
          request.lockToken shouldBe testToken
          request.lockName shouldBe testLockName
          request.namespace shouldBe "test-namespace"
          Future.successful(Response(success = true, message = "renewed"))
        }
        override def tryLock(request: TryLockRequest): Future[TryLockResponse] =
          noOpLockService.tryLock(request)
        override def acquireReadLock(request: AcquireReadLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireReadLock(request)
        override def acquireSemaphore(request: AcquireSemaphoreRequest): Future[AcquireSemaphoreResponse] =
          noOpLockService.acquireSemaphore(request)
        override def releaseSemaphore(request: ReleaseSemaphoreRequest): Future[Response] =
          noOpLockService.releaseSemaphore(request)
        override def waitBarrier(request: WaitBarrierRequest): Future[WaitBarrierResponse] =
          noOpLockService.waitBarrier(request)
        override def listLocks(request: ListLocksRequest): Future[ListLocksResponse] =
          noOpLockService.listLocks(request)
      }

      withLockClient(service) { client =>
        val originalExpiry = Instant.now().plusSeconds(10)
        val handle = LockHandle(
          lockToken = testToken,
          lockName = testLockName,
          owner = testOwner,
          namespace = "test-namespace",
          acquiredAt = Instant.now(),
          expiresAt = originalExpiry,
          fenceToken = 1L
        )

        val renewed = client.renewLock(handle, testTtl).unsafeRunSync()

        renewed.lockToken shouldBe testToken
        renewed.lockName shouldBe testLockName
        renewed.owner shouldBe testOwner
        renewed.expiresAt.isAfter(originalExpiry) shouldBe true
      }
    }

    "raise LockRenewalException on failure" in {
      val service = new LockServiceGrpc.LockService {
        override def acquireLock(request: AcquireLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireLock(request)
        override def releaseLock(request: ReleaseLockRequest): Future[Response] =
          noOpLockService.releaseLock(request)
        override def renewLock(request: RenewLockRequest): Future[Response] =
          Future.successful(Response(success = false, message = "lock not held"))
        override def tryLock(request: TryLockRequest): Future[TryLockResponse] =
          noOpLockService.tryLock(request)
        override def acquireReadLock(request: AcquireReadLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireReadLock(request)
        override def acquireSemaphore(request: AcquireSemaphoreRequest): Future[AcquireSemaphoreResponse] =
          noOpLockService.acquireSemaphore(request)
        override def releaseSemaphore(request: ReleaseSemaphoreRequest): Future[Response] =
          noOpLockService.releaseSemaphore(request)
        override def waitBarrier(request: WaitBarrierRequest): Future[WaitBarrierResponse] =
          noOpLockService.waitBarrier(request)
        override def listLocks(request: ListLocksRequest): Future[ListLocksResponse] =
          noOpLockService.listLocks(request)
      }

      withLockClient(service) { client =>
        val handle = LockHandle(
          lockToken = testToken,
          lockName = testLockName,
          owner = testOwner,
          namespace = "test-namespace",
          acquiredAt = Instant.now(),
          expiresAt = Instant.now().plusSeconds(60),
          fenceToken = 1L
        )

        val ex = the[LockRenewalException] thrownBy {
          client.renewLock(handle, testTtl).unsafeRunSync()
        }
        ex.lockName shouldBe testLockName
        ex.reason shouldBe "lock not held"
      }
    }
  }

  "tryLock" should {

    "return Some(LockHandle) when lock is available" in {
      val service = new LockServiceGrpc.LockService {
        override def acquireLock(request: AcquireLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireLock(request)
        override def releaseLock(request: ReleaseLockRequest): Future[Response] =
          noOpLockService.releaseLock(request)
        override def renewLock(request: RenewLockRequest): Future[Response] =
          noOpLockService.renewLock(request)
        override def tryLock(request: TryLockRequest): Future[TryLockResponse] = {
          request.lockName shouldBe testLockName
          request.namespace shouldBe "test-namespace"
          request.owner shouldBe testOwner
          Future.successful(
            TryLockResponse(
              acquired = true,
              lockToken = testToken,
              expiresAt = Some(Timestamp(seconds = futureExpirySeconds))
            )
          )
        }
        override def acquireReadLock(request: AcquireReadLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireReadLock(request)
        override def acquireSemaphore(request: AcquireSemaphoreRequest): Future[AcquireSemaphoreResponse] =
          noOpLockService.acquireSemaphore(request)
        override def releaseSemaphore(request: ReleaseSemaphoreRequest): Future[Response] =
          noOpLockService.releaseSemaphore(request)
        override def waitBarrier(request: WaitBarrierRequest): Future[WaitBarrierResponse] =
          noOpLockService.waitBarrier(request)
        override def listLocks(request: ListLocksRequest): Future[ListLocksResponse] =
          noOpLockService.listLocks(request)
      }

      withLockClient(service) { client =>
        val result = client.tryLock(testLockName, testOwner, testTtl, 5000L).unsafeRunSync()

        result shouldBe defined
        val handle = result.get
        handle.lockToken shouldBe testToken
        handle.lockName shouldBe testLockName
        handle.owner shouldBe testOwner
        handle.namespace shouldBe "test-namespace"
        handle.fenceToken should be > 0L
      }
    }

    "return None when lock is not available" in {
      val service = new LockServiceGrpc.LockService {
        override def acquireLock(request: AcquireLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireLock(request)
        override def releaseLock(request: ReleaseLockRequest): Future[Response] =
          noOpLockService.releaseLock(request)
        override def renewLock(request: RenewLockRequest): Future[Response] =
          noOpLockService.renewLock(request)
        override def tryLock(request: TryLockRequest): Future[TryLockResponse] =
          Future.successful(TryLockResponse(acquired = false))
        override def acquireReadLock(request: AcquireReadLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireReadLock(request)
        override def acquireSemaphore(request: AcquireSemaphoreRequest): Future[AcquireSemaphoreResponse] =
          noOpLockService.acquireSemaphore(request)
        override def releaseSemaphore(request: ReleaseSemaphoreRequest): Future[Response] =
          noOpLockService.releaseSemaphore(request)
        override def waitBarrier(request: WaitBarrierRequest): Future[WaitBarrierResponse] =
          noOpLockService.waitBarrier(request)
        override def listLocks(request: ListLocksRequest): Future[ListLocksResponse] =
          noOpLockService.listLocks(request)
      }

      withLockClient(service) { client =>
        val result = client.tryLock(testLockName, testOwner, testTtl, 5000L).unsafeRunSync()

        result shouldBe None
      }
    }
  }

  "lockResource" should {

    "acquire and release lock via Resource lifecycle" in {
      val acquireCalled  = new java.util.concurrent.atomic.AtomicBoolean(false)
      val releaseCalled  = new java.util.concurrent.atomic.AtomicBoolean(false)

      val service = new LockServiceGrpc.LockService {
        override def acquireLock(request: AcquireLockRequest): Future[AcquireLockResponse] = {
          acquireCalled.set(true)
          Future.successful(
            AcquireLockResponse(
              acquired = true,
              lockToken = testToken,
              expiresAt = Some(Timestamp(seconds = futureExpirySeconds))
            )
          )
        }
        override def releaseLock(request: ReleaseLockRequest): Future[Response] = {
          releaseCalled.set(true)
          Future.successful(Response(success = true, message = "released"))
        }
        override def renewLock(request: RenewLockRequest): Future[Response] =
          noOpLockService.renewLock(request)
        override def tryLock(request: TryLockRequest): Future[TryLockResponse] =
          noOpLockService.tryLock(request)
        override def acquireReadLock(request: AcquireReadLockRequest): Future[AcquireLockResponse] =
          noOpLockService.acquireReadLock(request)
        override def acquireSemaphore(request: AcquireSemaphoreRequest): Future[AcquireSemaphoreResponse] =
          noOpLockService.acquireSemaphore(request)
        override def releaseSemaphore(request: ReleaseSemaphoreRequest): Future[Response] =
          noOpLockService.releaseSemaphore(request)
        override def waitBarrier(request: WaitBarrierRequest): Future[WaitBarrierResponse] =
          noOpLockService.waitBarrier(request)
        override def listLocks(request: ListLocksRequest): Future[ListLocksResponse] =
          noOpLockService.listLocks(request)
      }

      withLockClient(service) { client =>
        val result = client
          .lockResource(testLockName, testOwner, testTtl)
          .use { handle =>
            IO {
              handle.lockToken shouldBe testToken
              handle.lockName shouldBe testLockName
              acquireCalled.get() shouldBe true
              "action-completed"
            }
          }
          .unsafeRunSync()

        result shouldBe "action-completed"
        releaseCalled.get() shouldBe true
      }
    }
  }

  "LockHandle" should {

    "hold lock metadata correctly" in {
      val now       = Instant.now()
      val expiresAt = now.plusSeconds(60)

      val handle = LockHandle(
        lockToken = "token-xyz",
        lockName = "my-lock",
        owner = "owner-1",
        namespace = "prod",
        acquiredAt = now,
        expiresAt = expiresAt,
        fenceToken = 42L
      )

      handle.lockToken shouldBe "token-xyz"
      handle.lockName shouldBe "my-lock"
      handle.owner shouldBe "owner-1"
      handle.namespace shouldBe "prod"
      handle.acquiredAt shouldBe now
      handle.expiresAt shouldBe expiresAt
      handle.fenceToken shouldBe 42L
    }
  }

  "LockConfig.withDefaults" should {

    "set default TTL and renewal intervals" in {
      val config = LockConfig.withDefaults("lock-host", 9090, "my-namespace")

      config.uccpHost shouldBe "lock-host"
      config.uccpPort shouldBe 9090
      config.namespace shouldBe "my-namespace"
      config.defaultTtlSeconds shouldBe 30
      config.renewIntervalSeconds shouldBe 10
      config.maxRetries shouldBe 3
      config.retryBackoffMs shouldBe 500L
    }
  }
}
