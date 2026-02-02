package io.gbmm.udps.integration.tls

import cats.effect.{IO, Resource}
import com.typesafe.scalalogging.LazyLogging
import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import io.netty.handler.ssl.{SslContext, SslContextBuilder, SslProvider}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import java.io.FileInputStream
import java.nio.file.{Files, Path, Paths}
import java.security.cert.{CertificateFactory, X509Certificate}
import java.time.Instant
import java.util.Date

final case class MTLSClientConfig(
  certPath: String,
  keyPath: String,
  caPath: String,
  enabled: Boolean
)

object MTLSClientConfig {
  implicit val reader: ConfigReader[MTLSClientConfig] = deriveReader[MTLSClientConfig]
}

final class MTLSContext(val sslContext: SslContext) {

  def buildChannel(host: String, port: Int): Resource[IO, ManagedChannel] =
    MTLSConfig.createChannel(host, port, sslContext)
}

object MTLSConfig extends LazyLogging {

  private val TLS_PROTOCOL = "TLSv1.3"

  private val STRONG_CIPHERS: java.lang.Iterable[String] = {
    val ciphers = new java.util.ArrayList[String]()
    val _ = ciphers.add("TLS_AES_256_GCM_SHA384")
    val _ = ciphers.add("TLS_AES_128_GCM_SHA256")
    val _ = ciphers.add("TLS_CHACHA20_POLY1305_SHA256")
    ciphers
  }

  def loadSslContext(config: MTLSClientConfig): IO[SslContext] =
    IO.blocking {
      val certPath = Paths.get(config.certPath)
      val keyPath = Paths.get(config.keyPath)
      val caPath = Paths.get(config.caPath)

      validatePathsExist(certPath, keyPath, caPath)
      validateCertificateNotExpired(certPath)
      validateCertificateNotExpired(caPath)

      buildSslContext(certPath, keyPath, caPath)
    }

  def createChannel(host: String, port: Int, sslContext: SslContext): Resource[IO, ManagedChannel] =
    Resource.make(
      IO.blocking {
        logger.info("Creating mTLS gRPC channel to {}:{}", host, port.toString)
        NettyChannelBuilder
          .forAddress(host, port)
          .sslContext(sslContext)
          .build()
      }
    )(channel =>
      IO.blocking {
        logger.info("Shutting down mTLS gRPC channel")
        channel.shutdown()
        ()
      }
    )

  private[tls] def buildSslContext(certPath: Path, keyPath: Path, caPath: Path): SslContext = {
    logger.info("Building mTLS SslContext with cert={}, key={}, ca={}", certPath, keyPath, caPath)

    val certStream = new FileInputStream(certPath.toFile)
    val keyStream = new FileInputStream(keyPath.toFile)
    val caStream = new FileInputStream(caPath.toFile)

    try {
      SslContextBuilder
        .forClient()
        .sslProvider(SslProvider.OPENSSL)
        .keyManager(certStream, keyStream)
        .trustManager(caStream)
        .protocols(TLS_PROTOCOL)
        .ciphers(STRONG_CIPHERS)
        .build()
    } finally {
      certStream.close()
      keyStream.close()
      caStream.close()
    }
  }

  private def validatePathsExist(paths: Path*): Unit =
    paths.foreach { path =>
      if (!Files.exists(path)) {
        val msg = s"Certificate file not found: $path"
        logger.error(msg)
        throw new IllegalArgumentException(msg)
      }
      if (!Files.isReadable(path)) {
        val msg = s"Certificate file not readable: $path"
        logger.error(msg)
        throw new IllegalArgumentException(msg)
      }
    }

  private[tls] def validateCertificateNotExpired(certPath: Path): Unit = {
    val factory = CertificateFactory.getInstance("X.509")
    val stream = new FileInputStream(certPath.toFile)
    try {
      val cert = factory.generateCertificate(stream).asInstanceOf[X509Certificate]
      val now = Date.from(Instant.now())
      if (cert.getNotAfter.before(now)) {
        val msg = s"Certificate is expired: $certPath (expired at ${cert.getNotAfter})"
        logger.error(msg)
        throw new IllegalStateException(msg)
      }
      if (cert.getNotBefore.after(now)) {
        val msg = s"Certificate is not yet valid: $certPath (valid from ${cert.getNotBefore})"
        logger.error(msg)
        throw new IllegalStateException(msg)
      }
      logger.debug("Certificate valid: {} (expires {})", certPath.toString, cert.getNotAfter.toString)
    } finally {
      stream.close()
    }
  }
}
