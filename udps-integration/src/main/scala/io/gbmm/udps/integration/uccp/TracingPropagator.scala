package io.gbmm.udps.integration.uccp

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.grpc.{CallOptions, Channel, ClientCall, ClientInterceptor, ForwardingClientCall, Metadata, MethodDescriptor}

import java.util.concurrent.ThreadLocalRandom

// ---------------------------------------------------------------------------
// Trace context model
// ---------------------------------------------------------------------------

final case class TraceContext(
    traceId: String,
    spanId: String,
    traceFlags: Byte
)

// ---------------------------------------------------------------------------
// TracingPropagator — utility methods for W3C Trace Context propagation
// ---------------------------------------------------------------------------

object TracingPropagator extends LazyLogging {

  private val HexPattern32 = "[0-9a-f]{32}".r
  private val HexPattern16 = "[0-9a-f]{16}".r
  private val HexPattern2  = "[0-9a-f]{2}".r

  private val TraceparentVersion = "00"

  val TraceparentKey: Metadata.Key[String] =
    Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER)

  /** Generate a new trace context with random IDs, applying the sampling decision. */
  def generateTraceContext(config: TracingConfig): IO[TraceContext] =
    IO {
      generateTraceContextSync(config)
    }

  /** Format a TraceContext as a W3C traceparent header value. */
  def formatTraceparent(ctx: TraceContext): String =
    s"$TraceparentVersion-${ctx.traceId}-${ctx.spanId}-${f"${ctx.traceFlags & 0xff}%02x"}"

  /**
   * Parse a W3C traceparent header value.
   * Expected format: "00-{32 hex}-{16 hex}-{2 hex}"
   */
  def parseTraceparent(header: String): Option[TraceContext] = {
    val parts = header.split('-')
    if (parts.length != 4) {
      None
    } else {
      val version  = parts(0)
      val traceId  = parts(1)
      val spanId   = parts(2)
      val flagsHex = parts(3)

      val valid =
        version == TraceparentVersion &&
          HexPattern32.pattern.matcher(traceId).matches() &&
          HexPattern16.pattern.matcher(spanId).matches() &&
          HexPattern2.pattern.matcher(flagsHex).matches()

      if (valid) {
        val flags = Integer.parseInt(flagsHex, 16).toByte
        Some(TraceContext(traceId, spanId, flags))
      } else {
        None
      }
    }
  }

  /** Create a TracingClientInterceptor for the given config. */
  def interceptor(config: TracingConfig): TracingClientInterceptor =
    new TracingClientInterceptor(config)

  /** Synchronous trace context generation for use inside gRPC interceptors. */
  private[uccp] def generateTraceContextSync(config: TracingConfig): TraceContext = {
    val random  = ThreadLocalRandom.current()
    val traceId = f"${random.nextLong()}%016x${random.nextLong()}%016x"
    val spanId  = f"${random.nextLong()}%016x"
    val sampled = random.nextDouble() < config.sampleRate
    TraceContext(traceId, spanId, if (sampled) 0x01.toByte else 0x00.toByte)
  }
}

// ---------------------------------------------------------------------------
// gRPC client interceptor — injects traceparent into outgoing metadata
// ---------------------------------------------------------------------------

final class TracingClientInterceptor(config: TracingConfig)
    extends ClientInterceptor
    with LazyLogging {

  override def interceptCall[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      callOptions: CallOptions,
      next: Channel
  ): ClientCall[ReqT, RespT] = {
    new ForwardingClientCall.SimpleForwardingClientCall[ReqT, RespT](
      next.newCall(method, callOptions)
    ) {
      override def start(
          responseListener: ClientCall.Listener[RespT],
          headers: Metadata
      ): Unit = {
        if (config.enabled) {
          val ctx         = TracingPropagator.generateTraceContextSync(config)
          val traceparent = TracingPropagator.formatTraceparent(ctx)
          headers.put(TracingPropagator.TraceparentKey, traceparent)
          logger.debug(
            s"Injected traceparent for ${method.getFullMethodName}: $traceparent"
          )
        }
        super.start(responseListener, headers)
      }
    }
  }
}
