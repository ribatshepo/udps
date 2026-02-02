package io.gbmm.udps.integration.usp

import cats.effect.{IO, Resource}
import cats.effect.std.AtomicCell
import com.typesafe.scalalogging.LazyLogging
import fs2.Stream
import io.gbmm.udps.integration.circuitbreaker.IntegrationCircuitBreaker
import io.grpc.ManagedChannel
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import usp.outpost.outpost.{
  AuthenticateRequest,
  AuthenticateResponse,
  HeartbeatRequest,
  HeartbeatResponse,
  OutpostCommunicationServiceGrpc
}

import java.net.InetAddress
import scala.concurrent.duration.FiniteDuration

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

final case class OutpostConfig(
    uspHost: String,
    uspPort: Int,
    outpostId: String,
    bootstrapToken: String,
    outpostVersion: String,
    heartbeatInterval: FiniteDuration
)

object OutpostConfig {
  implicit val reader: ConfigReader[OutpostConfig] = deriveReader[OutpostConfig]
}

// ---------------------------------------------------------------------------
// Registration failure
// ---------------------------------------------------------------------------

final class OutpostRegistrationException(
    val errorCode: String,
    val serverMessage: String
) extends RuntimeException(
      s"Outpost registration failed: errorCode=$errorCode, message=$serverMessage"
    )

// ---------------------------------------------------------------------------
// Session state
// ---------------------------------------------------------------------------

private[usp] final case class SessionState(
    sessionId: String,
    configVersion: Int
)

// ---------------------------------------------------------------------------
// OutpostRegistrationClient
// ---------------------------------------------------------------------------

final class OutpostRegistrationClient private[usp] (
    channel: ManagedChannel,
    circuitBreaker: IntegrationCircuitBreaker,
    config: OutpostConfig,
    sessionRef: AtomicCell[IO, Option[SessionState]]
) extends LazyLogging {

  private val stub: OutpostCommunicationServiceGrpc.OutpostCommunicationServiceStub =
    OutpostCommunicationServiceGrpc.stub(channel)

  /** Authenticate this outpost with USP using the bootstrap token. */
  def register: IO[AuthenticateResponse] = {
    val hostname = resolveHostname
    val metadata = Map(
      "outpost_type" -> "OUTPOST_TYPE_COMPUTE",
      "outpost_type_value" -> "6"
    )

    val request = AuthenticateRequest(
      outpostId = config.outpostId,
      token = config.bootstrapToken,
      outpostVersion = config.outpostVersion,
      hostname = hostname,
      metadata = metadata
    )

    val rpc: IO[AuthenticateResponse] =
      IO.fromFuture(IO(stub.authenticate(request)))

    circuitBreaker
      .protect(rpc)
      .flatMap { response =>
        if (response.success) {
          val state = SessionState(
            sessionId = response.sessionId,
            configVersion = response.configVersion
          )
          sessionRef.set(Some(state)).as {
            logger.info(
              "Outpost registered successfully: outpostId={}, sessionId={}, configVersion={}",
              config.outpostId,
              response.sessionId,
              response.configVersion.toString
            )
            response
          }
        } else {
          IO.raiseError(
            new OutpostRegistrationException(
              errorCode = response.errorCode,
              serverMessage = response.message
            )
          )
        }
      }
  }

  /** Emit periodic heartbeats for the given session. */
  def heartbeatStream(
      sessionId: String,
      configVersion: Int
  ): Stream[IO, HeartbeatResponse] =
    Stream
      .fixedRate[IO](config.heartbeatInterval)
      .evalMap { _ =>
        sendHeartbeat(sessionId, configVersion)
      }

  /** Register, then run heartbeat loop. Re-registers on failure. */
  def start: Stream[IO, Unit] = {
    val registrationAndHeartbeat: Stream[IO, Unit] =
      Stream
        .eval(register)
        .flatMap { authResponse =>
          heartbeatStream(authResponse.sessionId, authResponse.configVersion)
            .evalMap { hbResponse =>
              handleHeartbeatResponse(hbResponse)
            }
        }

    registrationAndHeartbeat.handleErrorWith { err =>
      Stream.eval(
        IO(logger.warn("Outpost connection lost, scheduling re-registration: {}", err.getMessage))
      ) ++ start
    }
  }

  /** Current session state, if registered. */
  def currentSession: IO[Option[SessionState]] =
    sessionRef.get

  // ---------------------------------------------------------------------------
  // Internal
  // ---------------------------------------------------------------------------

  private def sendHeartbeat(
      sessionId: String,
      configVersion: Int
  ): IO[HeartbeatResponse] = {
    val request = HeartbeatRequest(
      outpostId = config.outpostId,
      sessionId = sessionId,
      currentConfigVersion = configVersion,
      metrics = None
    )

    val rpc: IO[HeartbeatResponse] =
      IO.fromFuture(IO(stub.heartbeat(request)))

    circuitBreaker.protect(rpc).flatTap { response =>
      if (response.success) {
        IO(
          logger.debug(
            "Heartbeat acknowledged: outpostId={}, latestConfigVersion={}",
            config.outpostId,
            response.latestConfigVersion.toString
          )
        )
      } else {
        IO(
          logger.warn(
            "Heartbeat rejected for outpostId={}",
            config.outpostId
          )
        )
      }
    }
  }

  private def handleHeartbeatResponse(response: HeartbeatResponse): IO[Unit] =
    if (response.configUpdateAvailable) {
      val newVersion = response.latestConfigVersion
      sessionRef.evalModify {
        case Some(state) =>
          val updated = state.copy(configVersion = newVersion)
          IO(
            logger.info(
              "Config update detected: {} -> {}",
              state.configVersion.toString,
              newVersion.toString
            )
          ).as((Some(updated), ()))
        case None =>
          IO(
            logger.warn("Received heartbeat response but no active session")
          ).as((None, ()))
      }
    } else {
      IO.unit
    }

  private def resolveHostname: String =
    try {
      InetAddress.getLocalHost.getHostName
    } catch {
      case e: java.net.UnknownHostException =>
        logger.warn("Unable to resolve local hostname: {}", e.getMessage)
        "unknown"
    }
}

// ---------------------------------------------------------------------------
// Companion
// ---------------------------------------------------------------------------

object OutpostRegistrationClient extends LazyLogging {

  def resource(
      channel: Resource[IO, ManagedChannel],
      circuitBreaker: IntegrationCircuitBreaker,
      config: OutpostConfig
  ): Resource[IO, OutpostRegistrationClient] =
    for {
      ch         <- channel
      sessionRef <- Resource.eval(AtomicCell[IO].of(Option.empty[SessionState]))
    } yield {
      logger.info(
        "Created OutpostRegistrationClient: outpostId={}, usp={}:{}",
        config.outpostId,
        config.uspHost,
        config.uspPort.toString
      )
      new OutpostRegistrationClient(ch, circuitBreaker, config, sessionRef)
    }
}
