package io.gbmm.udps.storage.indexing

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector.{BigIntVector, FieldVector, Float8Vector, IntVector, VarCharVector}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}
import scala.collection.Searching._

// ---------------------------------------------------------------------------
// Value type tag constants for serialization
// ---------------------------------------------------------------------------

private[indexing] object ValueTypeTag {
  val LongTag: Byte = 0
  val IntTag: Byte = 1
  val DoubleTag: Byte = 2
  val StringTag: Byte = 3
}

// ---------------------------------------------------------------------------
// SkipListEntry — one boundary marker in the skip list
// ---------------------------------------------------------------------------

final case class SkipListEntry(
  value: Comparable[_],
  rowGroupIndex: Int,
  startRow: Long,
  endRow: Long
)

// ---------------------------------------------------------------------------
// SkipListIndex — sorted collection of entries with O(log n) range queries
// ---------------------------------------------------------------------------

final case class SkipListIndex(entries: IndexedSeq[SkipListEntry]) {

  /**
   * Find row group indices whose entries overlap with [lowerBound, upperBound].
   * Either bound may be None to indicate an open range.
   */
  def findRowGroups(
    lowerBound: Option[Comparable[_]],
    upperBound: Option[Comparable[_]]
  ): Seq[Int] = {
    if (entries.isEmpty) return Seq.empty

    val startIdx = lowerBound match {
      case Some(lb) => lowerBoundIndex(lb)
      case None     => 0
    }

    val endIdx = upperBound match {
      case Some(ub) => upperBoundIndex(ub)
      case None     => entries.length - 1
    }

    if (startIdx > endIdx || startIdx >= entries.length || endIdx < 0) {
      Seq.empty
    } else {
      val clampedStart = math.max(0, startIdx)
      val clampedEnd = math.min(entries.length - 1, endIdx)
      entries.slice(clampedStart, clampedEnd + 1).map(_.rowGroupIndex).distinct
    }
  }

  /** Convenience: find row groups for a BETWEEN query (inclusive). */
  def findRowGroupsForRange(start: Any, end: Any): Seq[Int] =
    findRowGroups(
      Some(toComparable(start)),
      Some(toComparable(end))
    )

  /** Find row groups with values greater than the given value. */
  def findRowGroupsGreaterThan(value: Any): Seq[Int] =
    findRowGroups(Some(toComparable(value)), None)

  /** Find row groups with values less than the given value. */
  def findRowGroupsLessThan(value: Any): Seq[Int] =
    findRowGroups(None, Some(toComparable(value)))

  // -------------------------------------------------------------------------
  // Binary search helpers using scala.collection.Searching
  // -------------------------------------------------------------------------

  /**
   * Find the first index whose entry value >= lowerBound.
   * If lowerBound is between two entries, we include the entry before it
   * because that row group may still contain values >= lowerBound.
   */
  private def lowerBoundIndex(lb: Comparable[_]): Int = {
    val idx = binarySearchPosition(lb)
    math.max(0, idx - 1)
  }

  /**
   * Find the last index whose entry value <= upperBound.
   * If upperBound is between two entries, we include the entry after it
   * because that row group may still contain values <= upperBound.
   */
  private def upperBoundIndex(ub: Comparable[_]): Int = {
    val idx = binarySearchPosition(ub)
    math.min(entries.length - 1, idx)
  }

  /**
   * Binary search returning the insertion point (or exact match index).
   * Uses the implicit Ordering derived from Comparable.
   */
  private def binarySearchPosition(target: Comparable[_]): Int = {
    implicit val ord: Ordering[Comparable[_]] = compareValues
    val searchSeq = entries.map(_.value)
    searchSeq.search(target) match {
      case Found(foundIndex)              => foundIndex
      case InsertionPoint(insertionPoint) => insertionPoint
    }
  }

  private def toComparable(v: Any): Comparable[_] = v match {
    case c: Comparable[_] => c
    case other => throw new IllegalArgumentException(
      s"Value of type ${other.getClass.getName} does not implement Comparable"
    )
  }
}

// ---------------------------------------------------------------------------
// Ordering for heterogeneous Comparable values
// ---------------------------------------------------------------------------

private[indexing] object compareValues extends Ordering[Comparable[_]] {
  @SuppressWarnings(Array("unchecked"))
  override def compare(x: Comparable[_], y: Comparable[_]): Int =
    x.asInstanceOf[Comparable[Any]].compareTo(y.asInstanceOf[Any])
}

// ---------------------------------------------------------------------------
// SkipListIndexer — builder and serialization
// ---------------------------------------------------------------------------

object SkipListIndexer extends LazyLogging {

  /** Default sampling interval: one entry every N rows. */
  private val DefaultSampleInterval: Int = 1000

  // =========================================================================
  // Build from a sorted Arrow FieldVector
  // =========================================================================

  def buildFromSortedColumn(
    vector: FieldVector,
    columnName: String,
    rowGroupIndex: Int,
    rowGroupSize: Int
  ): Seq[SkipListEntry] = {
    val valueCount = vector.getValueCount
    if (valueCount == 0) return Seq.empty

    val sampleInterval = math.max(1, math.min(DefaultSampleInterval, valueCount))
    val builder = Vector.newBuilder[SkipListEntry]

    var i = 0
    while (i < valueCount) {
      if (!vector.isNull(i)) {
        val value = extractComparable(vector, i)
        val startRow = i.toLong
        val endRow = math.min(i.toLong + sampleInterval - 1, valueCount.toLong - 1)
        builder += SkipListEntry(value, rowGroupIndex, startRow, endRow)
      }
      i += sampleInterval
    }

    // Always include the last element if it was not already sampled
    val lastIdx = valueCount - 1
    if (lastIdx % sampleInterval != 0 && !vector.isNull(lastIdx)) {
      val value = extractComparable(vector, lastIdx)
      builder += SkipListEntry(value, rowGroupIndex, lastIdx.toLong, lastIdx.toLong)
    }

    logger.debug(
      "Built {} skip list entries for column '{}', rowGroup={}, rows={}",
      builder.result().size.toString,
      columnName,
      rowGroupIndex.toString,
      valueCount.toString
    )

    builder.result()
  }

  // =========================================================================
  // Build from a Parquet file
  // =========================================================================

  def buildFromParquetFile(filePath: String, columnName: String): IO[SkipListIndex] =
    IO.blocking {
      val conf = new Configuration()
      val path = new Path(filePath)
      val inputFile = HadoopInputFile.fromPath(path, conf)
      val reader = ParquetFileReader.open(inputFile)
      try {
        val fileMetadata = reader.getFooter.getBlocks
        val numRowGroups = fileMetadata.size()
        val allEntries = Vector.newBuilder[SkipListEntry]

        var rgIdx = 0
        while (rgIdx < numRowGroups) {
          val blockMeta = fileMetadata.get(rgIdx)
          val rowCount = blockMeta.getRowCount

          val columns = blockMeta.getColumns
          var colIdx = 0
          var found = false
          while (colIdx < columns.size() && !found) {
            val colPath = columns.get(colIdx).getPath.toDotString
            if (colPath == columnName) {
              found = true
              val stats = columns.get(colIdx).getStatistics
              if (stats != null && stats.hasNonNullValue) {
                val minValue = statsToComparable(stats.genericGetMin)
                val maxValue = statsToComparable(stats.genericGetMax)
                allEntries += SkipListEntry(minValue, rgIdx, 0L, rowCount - 1)
                if (compareValues.compare(minValue, maxValue) != 0) {
                  allEntries += SkipListEntry(maxValue, rgIdx, 0L, rowCount - 1)
                }
              }
            }
            colIdx += 1
          }
          rgIdx += 1
        }

        val sorted = allEntries.result().sortWith { (a, b) =>
          compareValues.compare(a.value, b.value) < 0
        }

        logger.info(
          "Built skip list index from Parquet file '{}' on column '{}': {} entries across {} row groups",
          filePath,
          columnName,
          sorted.size.toString,
          numRowGroups.toString
        )

        SkipListIndex(sorted)
      } finally {
        reader.close()
      }
    }

  // =========================================================================
  // Serialization
  // =========================================================================

  def serialize(index: SkipListIndex): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    try {
      dos.writeInt(index.entries.size)
      index.entries.foreach { entry =>
        writeTypedValue(dos, entry.value)
        dos.writeInt(entry.rowGroupIndex)
        dos.writeLong(entry.startRow)
        dos.writeLong(entry.endRow)
      }
      dos.flush()
      baos.toByteArray
    } finally {
      dos.close()
    }
  }

  def deserialize(bytes: Array[Byte]): SkipListIndex = {
    val bais = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bais)
    try {
      val count = dis.readInt()
      val builder = Vector.newBuilder[SkipListEntry]
      builder.sizeHint(count)

      var i = 0
      while (i < count) {
        val value = readTypedValue(dis)
        val rowGroupIndex = dis.readInt()
        val startRow = dis.readLong()
        val endRow = dis.readLong()
        builder += SkipListEntry(value, rowGroupIndex, startRow, endRow)
        i += 1
      }

      SkipListIndex(builder.result())
    } finally {
      dis.close()
    }
  }

  /** Naming convention: `<parquetFileName>.skiplist.<columnName>` */
  def indexFileName(parquetPath: java.nio.file.Path, columnName: String): java.nio.file.Path = {
    val fileName = parquetPath.getFileName.toString
    parquetPath.resolveSibling(s"$fileName.skiplist.$columnName")
  }

  def writeToFile(index: SkipListIndex, path: java.nio.file.Path): IO[Unit] =
    IO.blocking {
      val bytes = serialize(index)
      Files.write(
        path,
        bytes,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE
      )
      logger.info("Wrote skip list index ({} bytes) to '{}'", bytes.length.toString, path.toString)
      ()
    }

  def readFromFile(path: java.nio.file.Path): IO[SkipListIndex] =
    IO.blocking {
      val bytes = Files.readAllBytes(path)
      logger.info("Read skip list index ({} bytes) from '{}'", bytes.length.toString, path.toString)
      deserialize(bytes)
    }

  // =========================================================================
  // Value extraction from Arrow vectors
  // =========================================================================

  private def extractComparable(vector: FieldVector, index: Int): Comparable[_] =
    vector match {
      case v: BigIntVector  => java.lang.Long.valueOf(v.get(index))
      case v: IntVector     => java.lang.Integer.valueOf(v.get(index))
      case v: Float8Vector  => java.lang.Double.valueOf(v.get(index))
      case v: VarCharVector => v.getObject(index).toString
      case other => throw new IllegalArgumentException(
        s"Unsupported vector type for skip list indexing: ${other.getClass.getName}"
      )
    }

  // =========================================================================
  // Parquet statistics to Comparable
  // =========================================================================

  private def statsToComparable(value: Any): Comparable[_] = value match {
    case v: java.lang.Long      => v
    case v: java.lang.Integer   => v
    case v: java.lang.Double    => v
    case v: java.lang.Float     => java.lang.Double.valueOf(v.doubleValue())
    case v: String              => v
    case v: org.apache.parquet.io.api.Binary =>
      v.toStringUsingUTF8
    case v: Comparable[_]       => v
    case other => throw new IllegalArgumentException(
      s"Cannot convert Parquet statistics value of type ${other.getClass.getName} to Comparable"
    )
  }

  // =========================================================================
  // Typed value serialization (type tag + value bytes)
  // =========================================================================

  private def writeTypedValue(dos: DataOutputStream, value: Comparable[_]): Unit =
    value match {
      case v: java.lang.Long =>
        dos.writeByte(ValueTypeTag.LongTag.toInt)
        dos.writeLong(v.longValue())
      case v: java.lang.Integer =>
        dos.writeByte(ValueTypeTag.IntTag.toInt)
        dos.writeInt(v.intValue())
      case v: java.lang.Double =>
        dos.writeByte(ValueTypeTag.DoubleTag.toInt)
        dos.writeDouble(v.doubleValue())
      case v: String =>
        dos.writeByte(ValueTypeTag.StringTag.toInt)
        val bytes = v.getBytes(StandardCharsets.UTF_8)
        dos.writeInt(bytes.length)
        dos.write(bytes)
      case other => throw new IllegalArgumentException(
        s"Cannot serialize value of type ${other.getClass.getName} for skip list index"
      )
    }

  private def readTypedValue(dis: DataInputStream): Comparable[_] =
    dis.readByte() match {
      case ValueTypeTag.LongTag   => java.lang.Long.valueOf(dis.readLong())
      case ValueTypeTag.IntTag    => java.lang.Integer.valueOf(dis.readInt())
      case ValueTypeTag.DoubleTag => java.lang.Double.valueOf(dis.readDouble())
      case ValueTypeTag.StringTag =>
        val len = dis.readInt()
        val bytes = new Array[Byte](len)
        dis.readFully(bytes)
        new String(bytes, StandardCharsets.UTF_8)
      case tag => throw new IllegalArgumentException(
        s"Unknown value type tag in skip list index: $tag"
      )
    }
}
