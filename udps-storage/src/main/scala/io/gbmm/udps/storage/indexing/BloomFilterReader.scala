package io.gbmm.udps.storage.indexing

import cats.effect.IO

import java.io.{DataInputStream, DataOutputStream, FileInputStream, FileOutputStream}
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.nio.file.Path

/**
 * File-based reader and writer for bloom filter sidecar index files.
 *
 * Naming convention: for a data file `data.parquet`, the bloom filter
 * for column `col` is stored at `data.parquet.bloom.col`.
 */
object BloomFilterReader {

  /**
   * Generate the sidecar file path for a bloom filter index.
   *
   * @param dataFilePath path to the data file (e.g. data.parquet)
   * @param columnName   the column name this bloom filter covers
   * @return the sidecar path (e.g. data.parquet.bloom.columnName)
   */
  def sidecarPath(dataFilePath: Path, columnName: String): Path =
    dataFilePath.resolveSibling(dataFilePath.getFileName.toString + ".bloom." + columnName)

  /**
   * Read a bloom filter from a `.bloom` sidecar file.
   */
  def readFromFile(path: Path): IO[BloomFilter] =
    IO.blocking {
      val fis = new FileInputStream(path.toFile)
      val dis = new DataInputStream(fis)
      try {
        val magic = dis.readInt()
        require(magic == BloomFilterSerializer.MagicNumber,
          f"Invalid bloom filter magic: 0x${magic}%08X")

        val version = dis.readInt()
        require(version == BloomFilterSerializer.FormatVersion,
          s"Unsupported bloom filter version: $version")

        val expectedInsertions = dis.readLong()
        val falsePositiveRate = dis.readDouble()
        val bitCount = dis.readLong()
        val hashCount = dis.readInt()
        val arrayLength = dis.readInt()

        val bitArray = new Array[Long](arrayLength)
        var i = 0
        while (i < arrayLength) {
          bitArray(i) = dis.readLong()
          i += 1
        }

        val config = BloomFilterConfig(expectedInsertions, falsePositiveRate)
        new BloomFilter(bitArray, bitCount, hashCount, config)
      } finally {
        dis.close()
      }
    }

  /**
   * Write a bloom filter to a `.bloom` sidecar file.
   */
  def writeToFile(filter: BloomFilter, path: Path): IO[Unit] =
    IO.blocking {
      val parentDir = path.getParent
      if (parentDir != null) {
        java.nio.file.Files.createDirectories(parentDir)
      }
      val fos = new FileOutputStream(path.toFile)
      val dos = new DataOutputStream(fos)
      try {
        dos.writeInt(BloomFilterSerializer.MagicNumber)
        dos.writeInt(BloomFilterSerializer.FormatVersion)
        dos.writeLong(filter.config.expectedInsertions)
        dos.writeDouble(filter.config.falsePositiveRate)
        dos.writeLong(filter.bitCount)
        dos.writeInt(filter.hashCount)
        dos.writeInt(filter.bitArray.length)
        var i = 0
        while (i < filter.bitArray.length) {
          dos.writeLong(filter.bitArray(i))
          i += 1
        }
        dos.flush()
      } finally {
        dos.close()
      }
    }

  /**
   * Determine whether a row group can be skipped based on bloom filter membership testing.
   *
   * If ALL values in a query's IN clause are definitely NOT in the filter, the row group
   * can be safely skipped. If any single value might be present, the row group must be read.
   *
   * @param filter the bloom filter for this row group/column
   * @param values the set of query predicate values to test
   * @return true if the row group can be safely skipped (no values match)
   */
  def shouldSkipRowGroup(filter: BloomFilter, values: Set[Any]): Boolean = {
    val iterator = values.iterator
    while (iterator.hasNext) {
      val bytes = anyToBytes(iterator.next())
      if (filter.mightContain(bytes)) {
        return false
      }
    }
    true
  }

  /**
   * Convert a query predicate value to its byte representation for bloom filter lookup.
   * Supports String, Long, Int, Short, Byte, and Array[Byte].
   */
  private def anyToBytes(value: Any): Array[Byte] = value match {
    case s: String =>
      s.getBytes(StandardCharsets.UTF_8)
    case l: Long =>
      val buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
      buf.putLong(l)
      buf.array()
    case i: Int =>
      val buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
      buf.putLong(i.toLong)
      buf.array()
    case s: Short =>
      val buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
      buf.putLong(s.toLong)
      buf.array()
    case b: Byte =>
      val buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
      buf.putLong(b.toLong)
      buf.array()
    case bytes: Array[Byte] =>
      bytes
    case other =>
      throw new UnsupportedOperationException(
        s"Cannot convert value of type ${other.getClass.getSimpleName} to bytes for bloom filter lookup")
  }
}

