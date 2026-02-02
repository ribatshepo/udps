package io.gbmm.udps.integration.tls

import cats.effect.{IO, Resource}
import cats.effect.std.Supervisor
import com.typesafe.scalalogging.LazyLogging
import fs2.io.file.{Files, Path, Watcher}
import io.netty.handler.ssl.SslContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._

final class CertificateReloader private (
  private val contextRef: AtomicReference[SslContext],
  private val config: MTLSClientConfig
) extends LazyLogging {

  def currentSslContext: IO[SslContext] =
    IO.delay(contextRef.get())

  def buildChannel(host: String, port: Int): Resource[IO, io.grpc.ManagedChannel] =
    Resource.eval(currentSslContext).flatMap { ctx =>
      MTLSConfig.createChannel(host, port, ctx)
    }

  private[tls] def reload: IO[Unit] =
    MTLSConfig.loadSslContext(config).attempt.flatMap {
      case Right(newCtx) =>
        IO.delay {
          contextRef.set(newCtx)
          logger.info("Successfully reloaded mTLS certificates")
        }
      case Left(err) =>
        IO.delay(logger.error("Failed to reload mTLS certificates, keeping current context", err))
    }
}

object CertificateReloader extends LazyLogging {

  private val DEBOUNCE_INTERVAL: FiniteDuration = 5.seconds

  def start(config: MTLSClientConfig): Resource[IO, CertificateReloader] =
    for {
      initialCtx <- Resource.eval(MTLSConfig.loadSslContext(config))
      ref = new AtomicReference[SslContext](initialCtx)
      reloader = new CertificateReloader(ref, config)
      supervisor <- Supervisor[IO]
      _ <- Resource.eval(
        supervisor.supervise(watchCertificateFiles(reloader, config))
      )
      _ <- Resource.eval(IO.delay(
        logger.info("CertificateReloader started, watching for file changes")
      ))
    } yield reloader

  private def watchCertificateFiles(
    reloader: CertificateReloader,
    config: MTLSClientConfig
  ): IO[Unit] = {
    val certPath = Path(config.certPath)
    val keyPath = Path(config.keyPath)
    val caPath = Path(config.caPath)

    val certDir = parentDirectory(certPath)
    val keyDir = parentDirectory(keyPath)
    val caDir = parentDirectory(caPath)

    val watchedFiles = Set(
      certPath.fileName.toString,
      keyPath.fileName.toString,
      caPath.fileName.toString
    )

    val directories = Set(certDir, keyDir, caDir)

    val watchStreams = directories.toList.map { dir =>
      Files[IO]
        .watch(dir)
        .filter { event =>
          fileNameFromEvent(event).exists(watchedFiles.contains)
        }
    }

    fs2.Stream
      .emits(watchStreams)
      .covary[IO]
      .flatten
      .debounce(DEBOUNCE_INTERVAL)
      .evalMap(_ => reloader.reload)
      .compile
      .drain
  }

  private def parentDirectory(path: Path): Path =
    path.parent match {
      case Some(p) => p
      case None    => Path(".")
    }

  private def fileNameFromEvent(event: Watcher.Event): Option[String] =
    event match {
      case Watcher.Event.Created(p, _)      => Some(p.fileName.toString)
      case Watcher.Event.Deleted(p, _)      => Some(p.fileName.toString)
      case Watcher.Event.Modified(p, _)     => Some(p.fileName.toString)
      case Watcher.Event.Overflow(_)        => None
      case Watcher.Event.NonStandard(_, _)  => None
    }
}
