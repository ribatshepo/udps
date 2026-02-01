package io.gbmm.udps.storage.parquet

import io.gbmm.udps.core.domain.CompressionCodec

import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
 * Configuration for Parquet file compression and encoding.
 *
 * Maps UDPS domain [[CompressionCodec]] to Parquet-native
 * [[CompressionCodecName]] values. The encoding-only codecs
 * (DeltaBinaryPacked, DeltaLengthByteArray) are mapped to
 * UNCOMPRESSED with a flag indicating that the caller should
 * prefer delta encoding at the column level.
 *
 * @param compressionCodec  the Parquet compression codec to apply
 * @param rowGroupSizeBytes maximum row group size in bytes
 * @param pageSizeBytes     target page size in bytes
 * @param dictionaryPageSizeBytes target dictionary page size in bytes
 * @param enableDictionary  whether to enable dictionary encoding
 * @param useDeltaEncoding  whether delta encoding should be preferred
 *                          (set automatically for DeltaBinaryPacked and
 *                          DeltaLengthByteArray)
 * @param writerVersion     Parquet writer version
 */
final case class CompressionConfig(
    compressionCodec: CompressionCodecName,
    rowGroupSizeBytes: Long,
    pageSizeBytes: Int,
    dictionaryPageSizeBytes: Int,
    enableDictionary: Boolean,
    useDeltaEncoding: Boolean,
    writerVersion: ParquetProperties.WriterVersion
)

object CompressionConfig {

  /** 128 MiB default row group size. */
  val DefaultRowGroupSizeBytes: Long = 128L * 1024L * 1024L

  /** 1 MiB default page size. */
  val DefaultPageSizeBytes: Int = 1024 * 1024

  /** 1 MiB default dictionary page size. */
  val DefaultDictionaryPageSizeBytes: Int = 1024 * 1024

  /** Default writer version. */
  val DefaultWriterVersion: ParquetProperties.WriterVersion =
    ParquetProperties.WriterVersion.PARQUET_2_0

  /**
   * Maps a UDPS [[CompressionCodec]] to a Parquet [[CompressionCodecName]].
   *
   * DeltaBinaryPacked and DeltaLengthByteArray are Parquet ''encodings'',
   * not compression codecs. They are mapped to UNCOMPRESSED; the
   * `useDeltaEncoding` flag signals to the writer that delta encoding
   * should be preferred at the column level.
   */
  def toParquetCodec(codec: CompressionCodec): CompressionCodecName =
    codec match {
      case CompressionCodec.Uncompressed         => CompressionCodecName.UNCOMPRESSED
      case CompressionCodec.Snappy               => CompressionCodecName.SNAPPY
      case CompressionCodec.Gzip                 => CompressionCodecName.GZIP
      case CompressionCodec.Lz4                  => CompressionCodecName.LZ4
      case CompressionCodec.Zstd                 => CompressionCodecName.ZSTD
      case CompressionCodec.Brotli               => CompressionCodecName.BROTLI
      case CompressionCodec.DeltaBinaryPacked    => CompressionCodecName.UNCOMPRESSED
      case CompressionCodec.DeltaLengthByteArray => CompressionCodecName.UNCOMPRESSED
    }

  /**
   * Returns `true` when the UDPS codec is an encoding hint rather than a
   * compression codec.
   */
  def isDeltaEncoding(codec: CompressionCodec): Boolean =
    codec match {
      case CompressionCodec.DeltaBinaryPacked    => true
      case CompressionCodec.DeltaLengthByteArray => true
      case _                                     => false
    }

  /**
   * Creates a [[CompressionConfig]] from a UDPS [[CompressionCodec]] with
   * all other settings at their defaults.
   */
  def fromCodec(codec: CompressionCodec): CompressionConfig =
    CompressionConfig(
      compressionCodec = toParquetCodec(codec),
      rowGroupSizeBytes = DefaultRowGroupSizeBytes,
      pageSizeBytes = DefaultPageSizeBytes,
      dictionaryPageSizeBytes = DefaultDictionaryPageSizeBytes,
      enableDictionary = true,
      useDeltaEncoding = isDeltaEncoding(codec),
      writerVersion = DefaultWriterVersion
    )

  /**
   * Creates a [[CompressionConfig]] with full control over all settings.
   */
  def apply(
      codec: CompressionCodec,
      rowGroupSizeBytes: Long = DefaultRowGroupSizeBytes,
      pageSizeBytes: Int = DefaultPageSizeBytes,
      dictionaryPageSizeBytes: Int = DefaultDictionaryPageSizeBytes,
      enableDictionary: Boolean = true,
      writerVersion: ParquetProperties.WriterVersion = DefaultWriterVersion
  ): CompressionConfig =
    CompressionConfig(
      compressionCodec = toParquetCodec(codec),
      rowGroupSizeBytes = rowGroupSizeBytes,
      pageSizeBytes = pageSizeBytes,
      dictionaryPageSizeBytes = dictionaryPageSizeBytes,
      enableDictionary = enableDictionary,
      useDeltaEncoding = isDeltaEncoding(codec),
      writerVersion = writerVersion
    )

  /** Convenience: sensible default using Snappy compression. */
  val Default: CompressionConfig = fromCodec(CompressionCodec.Snappy)
}
