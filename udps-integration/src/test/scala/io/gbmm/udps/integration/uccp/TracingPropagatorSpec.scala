package io.gbmm.udps.integration.uccp

import cats.effect.unsafe.implicits.global
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class TracingPropagatorSpec extends AnyWordSpec with Matchers {

  private val HexCharPattern = "[0-9a-f]+".r

  "formatTraceparent" should {

    "format a TraceContext as W3C traceparent" in {
      val ctx = TraceContext(
        traceId = "0af7651916cd43dd8448eb211c80319c",
        spanId = "b7ad6b7169203331",
        traceFlags = 0x01.toByte
      )

      val result = TracingPropagator.formatTraceparent(ctx)

      result shouldBe "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
      val parts = result.split('-')
      parts should have length 4
      parts(0) shouldBe "00"
      parts(1) should have length 32
      parts(2) should have length 16
      parts(3) should have length 2
    }

    "format flags 0x01 as sampled" in {
      val ctx = TraceContext(
        traceId = "0af7651916cd43dd8448eb211c80319c",
        spanId = "b7ad6b7169203331",
        traceFlags = 0x01.toByte
      )

      val result = TracingPropagator.formatTraceparent(ctx)

      result should endWith("-01")
    }

    "format flags 0x00 as not sampled" in {
      val ctx = TraceContext(
        traceId = "0af7651916cd43dd8448eb211c80319c",
        spanId = "b7ad6b7169203331",
        traceFlags = 0x00.toByte
      )

      val result = TracingPropagator.formatTraceparent(ctx)

      result should endWith("-00")
    }
  }

  "parseTraceparent" should {

    "parse a valid traceparent header" in {
      val header = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

      val result = TracingPropagator.parseTraceparent(header)

      result shouldBe defined
      val ctx = result.get
      ctx.traceId shouldBe "0af7651916cd43dd8448eb211c80319c"
      ctx.spanId shouldBe "b7ad6b7169203331"
      ctx.traceFlags shouldBe 0x01.toByte
    }

    "return None for invalid version" in {
      val header = "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

      TracingPropagator.parseTraceparent(header) shouldBe None
    }

    "return None for wrong trace ID length" in {
      val header = "00-abc-b7ad6b7169203331-01"

      TracingPropagator.parseTraceparent(header) shouldBe None
    }

    "return None for wrong span ID length" in {
      val header = "00-0af7651916cd43dd8448eb211c80319c-abc-01"

      TracingPropagator.parseTraceparent(header) shouldBe None
    }

    "return None for empty string" in {
      TracingPropagator.parseTraceparent("") shouldBe None
    }

    "return None for wrong number of parts" in {
      TracingPropagator.parseTraceparent("00-abcd") shouldBe None
      TracingPropagator.parseTraceparent("00-a-b-c-d") shouldBe None
    }

    "roundtrip: format then parse" in {
      val original = TraceContext(
        traceId = "abcdef0123456789abcdef0123456789",
        spanId = "0123456789abcdef",
        traceFlags = 0x01.toByte
      )

      val formatted = TracingPropagator.formatTraceparent(original)
      val parsed = TracingPropagator.parseTraceparent(formatted)

      parsed shouldBe defined
      parsed.get shouldBe original
    }
  }

  "generateTraceContext" should {

    "generate a context with 32-char hex traceId" in {
      val config = TracingConfig(enabled = true, sampleRate = 1.0)

      val ctx = TracingPropagator.generateTraceContext(config).unsafeRunSync()

      ctx.traceId should have length 32
      HexCharPattern.pattern.matcher(ctx.traceId).matches() shouldBe true
    }

    "generate a context with 16-char hex spanId" in {
      val config = TracingConfig(enabled = true, sampleRate = 1.0)

      val ctx = TracingPropagator.generateTraceContext(config).unsafeRunSync()

      ctx.spanId should have length 16
      HexCharPattern.pattern.matcher(ctx.spanId).matches() shouldBe true
    }

    "apply sampling at rate 1.0" in {
      val config = TracingConfig(enabled = true, sampleRate = 1.0)

      val ctx = TracingPropagator.generateTraceContext(config).unsafeRunSync()

      ctx.traceFlags shouldBe 0x01.toByte
    }

    "apply sampling at rate 0.0" in {
      val config = TracingConfig(enabled = true, sampleRate = 0.0)

      val ctx = TracingPropagator.generateTraceContext(config).unsafeRunSync()

      ctx.traceFlags shouldBe 0x00.toByte
    }

    "generate unique contexts" in {
      val config = TracingConfig(enabled = true, sampleRate = 1.0)

      val ctx1 = TracingPropagator.generateTraceContext(config).unsafeRunSync()
      val ctx2 = TracingPropagator.generateTraceContext(config).unsafeRunSync()

      ctx1.traceId should not equal ctx2.traceId
    }
  }

  "TracingClientInterceptor" should {

    "be created from TracingConfig" in {
      val config = TracingConfig(enabled = true, sampleRate = 0.5)

      val result = TracingPropagator.interceptor(config)

      result should not be null
      result shouldBe a[TracingClientInterceptor]
    }
  }
}
