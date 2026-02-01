package io.gbmm.udps.storage.indexing

import org.apache.arrow.vector._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import scala.util.hashing.MurmurHash3

/**
 * Configuration for a Bloom filter that computes optimal parameters
 * from the expected number of insertions and the desired false positive rate.
 *
 * @param expectedInsertions expected number of elements to insert
 * @param falsePositiveRate  desired false positive probability (default 1%)
 */
final case class BloomFilterConfig(
  expectedInsertions: Long,
  falsePositiveRate: Double = 0.01
) {
  require(expectedInsertions > 0, "expectedInsertions must be positive")
  require(falsePositiveRate > 0.0 && falsePositiveRate < 1.0,
    "falsePositiveRate must be in (0, 1)")

  /** Optimal bit array size: m = ceil(-n * ln(p) / (ln(2))^2) */
  val optimalBitCount: Long = {
    val n = expectedInsertions.toDouble
    val p = falsePositiveRate
    val ln2Sq = math.log(2.0) * math.log(2.0)
    math.ceil(-n * math.log(p) / ln2Sq).toLong
  }

  /** Optimal number of hash functions: k = ceil((m/n) * ln(2)) */
  val optimalHashCount: Int = {
    val ratio = optimalBitCount.toDouble / expectedInsertions.toDouble
    math.max(1, math.ceil(ratio * math.log(2.0)).toInt)
  }
}

/**
 * A space-efficient probabilistic data structure for membership testing.
 *
 * Uses double-hashing with two independent 32-bit MurmurHash3 seeds to
 * simulate k independent hash functions: h_i(x) = h1(x) + i * h2(x).
 *
 * @param bitArray  the underlying bit storage as an array of longs
 * @param bitCount  total number of bits in the filter
 * @param hashCount number of hash functions to apply
 * @param config    the original configuration used to build this filter
 */
final class BloomFilter private[indexing](
  private[indexing] val bitArray: Array[Long],
  val bitCount: Long,
  val hashCount: Int,
  val config: BloomFilterConfig
) {

  private val HashSeed1: Int = 0
  private val HashSeed2: Int = 0x9747b28c

  /**
   * Add an element (as raw bytes) to the filter.
   */
  def add(value: Array[Byte]): Unit = {
    val (h1, h2) = computeHashes(value)
    var i = 0
    while (i < hashCount) {
      val combinedHash = h1 + i.toLong * h2
      val bitIndex = ((combinedHash % bitCount) + bitCount) % bitCount
      setBit(bitIndex)
      i += 1
    }
  }

  /**
   * Test whether an element might be in the set.
   *
   * @return true if the element might be present (with FPR probability of false positive),
   *         false if the element is definitely not present
   */
  def mightContain(value: Array[Byte]): Boolean = {
    val (h1, h2) = computeHashes(value)
    var i = 0
    while (i < hashCount) {
      val combinedHash = h1 + i.toLong * h2
      val bitIndex = ((combinedHash % bitCount) + bitCount) % bitCount
      if (!getBit(bitIndex)) return false
      i += 1
    }
    true
  }

  def addString(value: String): Unit =
    add(value.getBytes(StandardCharsets.UTF_8))

  def addLong(value: Long): Unit = {
    val buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
    buf.putLong(value)
    add(buf.array())
  }

  def mightContainString(value: String): Boolean =
    mightContain(value.getBytes(StandardCharsets.UTF_8))

  def mightContainLong(value: Long): Boolean = {
    val buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
    buf.putLong(value)
    mightContain(buf.array())
  }

  /**
   * Compute two independent 32-bit hashes using MurmurHash3 with different seeds,
   * then widen to Long for the double-hashing combination.
   */
  private def computeHashes(value: Array[Byte]): (Long, Long) = {
    val h1 = murmur3Hash(value, HashSeed1).toLong
    val h2 = murmur3Hash(value, HashSeed2).toLong
    (h1, h2)
  }

  /**
   * MurmurHash3 32-bit hash of a byte array with a given seed.
   * Uses scala.util.hashing.MurmurHash3 internals with byte-level mixing.
   */
  private def murmur3Hash(data: Array[Byte], seed: Int): Int = {
    val len = data.length
    var h = seed
    val nBlocks = len / 4

    var i = 0
    while (i < nBlocks) {
      val offset = i * 4
      val k = (data(offset) & 0xff) |
        ((data(offset + 1) & 0xff) << 8) |
        ((data(offset + 2) & 0xff) << 16) |
        ((data(offset + 3) & 0xff) << 24)
      h = MurmurHash3.mix(h, k)
      i += 1
    }

    val tailStart = nBlocks * 4
    var k1 = 0
    (len - tailStart) match {
      case 3 =>
        k1 ^= (data(tailStart + 2) & 0xff) << 16
        k1 ^= (data(tailStart + 1) & 0xff) << 8
        k1 ^= data(tailStart) & 0xff
        k1 = mixK1(k1)
        h ^= k1
      case 2 =>
        k1 ^= (data(tailStart + 1) & 0xff) << 8
        k1 ^= data(tailStart) & 0xff
        k1 = mixK1(k1)
        h ^= k1
      case 1 =>
        k1 ^= data(tailStart) & 0xff
        k1 = mixK1(k1)
        h ^= k1
      case _ => ()
    }

    MurmurHash3.finalizeHash(h, len)
  }

  /** Tail-block mixing for MurmurHash3. */
  private def mixK1(k: Int): Int = {
    var k1 = k
    k1 *= 0xcc9e2d51
    k1 = Integer.rotateLeft(k1, 15)
    k1 *= 0x1b873593
    k1
  }

  private def setBit(index: Long): Unit = {
    val wordIndex = (index >>> 6).toInt
    val bitOffset = index & 63L
    bitArray(wordIndex) = bitArray(wordIndex) | (1L << bitOffset)
  }

  private def getBit(index: Long): Boolean = {
    val wordIndex = (index >>> 6).toInt
    val bitOffset = index & 63L
    (bitArray(wordIndex) & (1L << bitOffset)) != 0L
  }
}

object BloomFilter {

  /** Create an empty bloom filter from the given configuration. */
  def create(config: BloomFilterConfig): BloomFilter = {
    val bitCount = config.optimalBitCount
    val arraySize = ((bitCount + 63L) / 64L).toInt
    val bitArray = new Array[Long](arraySize)
    new BloomFilter(bitArray, bitCount, config.optimalHashCount, config)
  }
}

/**
 * Serialization utilities for BloomFilter.
 *
 * Wire format:
 *   - 4 bytes: magic number (0x424C4D46 = "BLMF")
 *   - 4 bytes: format version (1)
 *   - 8 bytes: expectedInsertions
 *   - 8 bytes: falsePositiveRate (Double bits)
 *   - 8 bytes: bitCount
 *   - 4 bytes: hashCount
 *   - 4 bytes: number of longs in bit array
 *   - N * 8 bytes: bit array longs
 */
object BloomFilterSerializer {

  private[indexing] val MagicNumber: Int = 0x424C4D46
  private[indexing] val FormatVersion: Int = 1

  def serialize(filter: BloomFilter): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    try {
      dos.writeInt(MagicNumber)
      dos.writeInt(FormatVersion)
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
      baos.toByteArray
    } finally {
      dos.close()
    }
  }

  def deserialize(bytes: Array[Byte]): BloomFilter = {
    val bais = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bais)
    try {
      val magic = dis.readInt()
      require(magic == MagicNumber,
        f"Invalid bloom filter magic number: 0x${magic}%08X (expected 0x${MagicNumber}%08X)")

      val version = dis.readInt()
      require(version == FormatVersion,
        s"Unsupported bloom filter format version: $version (expected $FormatVersion)")

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
}

/**
 * Builds BloomFilter instances from Apache Arrow column vectors.
 */
object BloomFilterIndexer {

  /**
   * Build a bloom filter from an Arrow FieldVector (column).
   * Supports VarCharVector, IntVector, BigIntVector, and VarBinaryVector.
   */
  def buildFromColumn(vector: FieldVector, config: BloomFilterConfig): BloomFilter = {
    val filter = BloomFilter.create(config)
    val rowCount = vector.getValueCount

    vector match {
      case v: VarCharVector =>
        var i = 0
        while (i < rowCount) {
          if (!v.isNull(i)) {
            filter.add(v.get(i))
          }
          i += 1
        }

      case v: IntVector =>
        var i = 0
        while (i < rowCount) {
          if (!v.isNull(i)) {
            filter.addLong(v.get(i).toLong)
          }
          i += 1
        }

      case v: BigIntVector =>
        var i = 0
        while (i < rowCount) {
          if (!v.isNull(i)) {
            filter.addLong(v.get(i))
          }
          i += 1
        }

      case v: VarBinaryVector =>
        var i = 0
        while (i < rowCount) {
          if (!v.isNull(i)) {
            filter.add(v.get(i))
          }
          i += 1
        }

      case other =>
        throw new UnsupportedOperationException(
          s"BloomFilterIndexer does not support vector type: ${other.getClass.getSimpleName}")
    }

    filter
  }

  /**
   * Build a bloom filter from a named column within a VectorSchemaRoot (row group).
   */
  def buildFromRowGroup(
    root: VectorSchemaRoot,
    columnName: String,
    config: BloomFilterConfig
  ): BloomFilter = {
    val vector = root.getVector(columnName)
    if (vector == null) {
      throw new IllegalArgumentException(
        s"Column '$columnName' not found in schema root. " +
        s"Available columns: ${root.getSchema.getFields.toString}")
    }
    buildFromColumn(vector, config)
  }
}
