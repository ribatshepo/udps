package io.gbmm.udps.core.domain

sealed trait CompressionCodec extends Product with Serializable

object CompressionCodec {
  case object Uncompressed extends CompressionCodec
  case object Snappy extends CompressionCodec
  case object Gzip extends CompressionCodec
  case object Lz4 extends CompressionCodec
  case object Zstd extends CompressionCodec
  case object Brotli extends CompressionCodec
  case object DeltaBinaryPacked extends CompressionCodec
  case object DeltaLengthByteArray extends CompressionCodec
}
