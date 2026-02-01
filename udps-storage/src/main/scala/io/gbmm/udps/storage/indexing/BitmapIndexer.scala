package io.gbmm.udps.storage.indexing

import cats.effect.IO
import org.apache.arrow.vector.{BigIntVector, BitVector, FieldVector, IntVector, VarCharVector}
import org.roaringbitmap.RoaringBitmap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets

import scala.collection.mutable

/**
 * Bitmap index over a single column, mapping each distinct string-encoded value
 * to a [[RoaringBitmap]] of row positions where that value appears.
 *
 * Designed for low-cardinality categorical columns where the number of distinct
 * values is small relative to row count.
 */
final case class BitmapIndex(columnName: String, bitmaps: Map[String, RoaringBitmap]) {

  /** Row positions for a single value. Returns an empty bitmap if value is absent. */
  def query(value: String): RoaringBitmap =
    bitmaps.getOrElse(value, new RoaringBitmap())

  /** Intersection of row positions across all given values. */
  def queryAnd(values: Seq[String]): RoaringBitmap = {
    if (values.isEmpty) return new RoaringBitmap()
    val iter = values.iterator
    val result = query(iter.next()).clone()
    while (iter.hasNext) {
      result.and(query(iter.next()))
    }
    result
  }

  /** Union of row positions across all given values. */
  def queryOr(values: Seq[String]): RoaringBitmap = {
    if (values.isEmpty) return new RoaringBitmap()
    val iter = values.iterator
    val result = query(iter.next()).clone()
    while (iter.hasNext) {
      result.or(query(iter.next()))
    }
    result
  }

  /** Complement: all rows NOT matching the given value, within [0, totalRows). */
  def queryNot(value: String, totalRows: Int): RoaringBitmap = {
    val universe = new RoaringBitmap()
    if (totalRows > 0) {
      universe.add(0L, totalRows.toLong)
    }
    val matching = query(value)
    RoaringBitmap.andNot(universe, matching)
  }

  /** Number of distinct values in this index. */
  def cardinality: Int = bitmaps.size
}

/**
 * Builder and utilities for [[BitmapIndex]] — scans Apache Arrow column vectors,
 * builds per-value bitmaps, and provides serialization / file I/O.
 */
object BitmapIndexer {

  // ---------------------------------------------------------------------------
  // Build from Arrow FieldVector
  // ---------------------------------------------------------------------------

  /** Scan an Arrow column vector and produce a [[BitmapIndex]]. */
  def buildFromColumn(vector: FieldVector, columnName: String): BitmapIndex = {
    val rowCount = vector.getValueCount
    val acc = mutable.HashMap.empty[String, RoaringBitmap]

    var row = 0
    while (row < rowCount) {
      if (!vector.isNull(row)) {
        val key = extractValue(vector, row)
        val bitmap = acc.getOrElseUpdate(key, new RoaringBitmap())
        bitmap.add(row)
      }
      row += 1
    }

    BitmapIndex(columnName, acc.toMap)
  }

  // ---------------------------------------------------------------------------
  // Incremental update
  // ---------------------------------------------------------------------------

  /**
   * Add rows from a new vector into an existing index. Row positions in the
   * vector are offset by `startRowOffset` so they don't collide with existing
   * positions.
   */
  def addRows(index: BitmapIndex, vector: FieldVector, startRowOffset: Int): BitmapIndex = {
    val rowCount = vector.getValueCount
    val acc = mutable.HashMap.empty[String, RoaringBitmap]

    // Clone existing bitmaps
    index.bitmaps.foreach { case (k, bm) =>
      acc.put(k, bm.clone())
    }

    var row = 0
    while (row < rowCount) {
      if (!vector.isNull(row)) {
        val key = extractValue(vector, row)
        val bitmap = acc.getOrElseUpdate(key, new RoaringBitmap())
        bitmap.add(startRowOffset + row)
      }
      row += 1
    }

    BitmapIndex(index.columnName, acc.toMap)
  }

  // ---------------------------------------------------------------------------
  // Serialization
  // ---------------------------------------------------------------------------

  private val MagicBytes: Int = 0x424D4158 // "BMAX"
  private val FormatVersion: Int = 1

  /**
   * Serialize a [[BitmapIndex]] to a byte array.
   *
   * Format:
   *   - 4 bytes: magic
   *   - 4 bytes: format version
   *   - 4 bytes: column name length (UTF-8 byte count)
   *   - N bytes: column name (UTF-8)
   *   - 4 bytes: number of distinct values
   *   - For each value:
   *     - 4 bytes: value string length (UTF-8 byte count)
   *     - N bytes: value string (UTF-8)
   *     - 4 bytes: serialized bitmap byte count
   *     - N bytes: RoaringBitmap serialized bytes
   */
  def serialize(index: BitmapIndex): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    try {
      dos.writeInt(MagicBytes)
      dos.writeInt(FormatVersion)

      val columnNameBytes = index.columnName.getBytes(StandardCharsets.UTF_8)
      dos.writeInt(columnNameBytes.length)
      dos.write(columnNameBytes)

      dos.writeInt(index.bitmaps.size)

      index.bitmaps.foreach { case (value, bitmap) =>
        val valueBytes = value.getBytes(StandardCharsets.UTF_8)
        dos.writeInt(valueBytes.length)
        dos.write(valueBytes)

        bitmap.runOptimize()
        val bitmapBaos = new ByteArrayOutputStream()
        val bitmapDos = new DataOutputStream(bitmapBaos)
        try {
          bitmap.serialize(bitmapDos)
          bitmapDos.flush()
        } finally {
          bitmapDos.close()
        }
        val bitmapBytes = bitmapBaos.toByteArray
        dos.writeInt(bitmapBytes.length)
        dos.write(bitmapBytes)
      }

      dos.flush()
      baos.toByteArray
    } finally {
      dos.close()
    }
  }

  /** Deserialize a [[BitmapIndex]] from a byte array. */
  def deserialize(bytes: Array[Byte]): BitmapIndex = {
    val bais = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bais)
    try {
      val magic = dis.readInt()
      require(magic == MagicBytes, s"Invalid bitmap index magic: 0x${magic.toHexString}")

      val version = dis.readInt()
      require(version == FormatVersion, s"Unsupported bitmap index version: $version")

      val columnNameLen = dis.readInt()
      val columnNameBytes = new Array[Byte](columnNameLen)
      dis.readFully(columnNameBytes)
      val columnName = new String(columnNameBytes, StandardCharsets.UTF_8)

      val valueCount = dis.readInt()
      val acc = Map.newBuilder[String, RoaringBitmap]

      var i = 0
      while (i < valueCount) {
        val valueLen = dis.readInt()
        val valueBytes = new Array[Byte](valueLen)
        dis.readFully(valueBytes)
        val value = new String(valueBytes, StandardCharsets.UTF_8)

        val bitmapLen = dis.readInt()
        val bitmapBytes = new Array[Byte](bitmapLen)
        dis.readFully(bitmapBytes)

        val bitmap = new RoaringBitmap()
        val bitmapBais = new ByteArrayInputStream(bitmapBytes)
        val bitmapDis = new DataInputStream(bitmapBais)
        try {
          bitmap.deserialize(bitmapDis)
        } finally {
          bitmapDis.close()
        }

        acc += (value -> bitmap)
        i += 1
      }

      BitmapIndex(columnName, acc.result())
    } finally {
      dis.close()
    }
  }

  // ---------------------------------------------------------------------------
  // File I/O
  // ---------------------------------------------------------------------------

  /**
   * Write a bitmap index to the given path. File naming convention:
   * `data.parquet.bitmap.<columnName>`
   */
  def writeToFile(index: BitmapIndex, path: Path): IO[Unit] =
    IO.blocking {
      val bytes = serialize(index)
      val parent = path.getParent
      if (parent != null) {
        Files.createDirectories(parent)
      }
      Files.write(path, bytes)
      ()
    }

  /**
   * Read a bitmap index from the given path.
   */
  def readFromFile(path: Path): IO[BitmapIndex] =
    IO.blocking {
      val bytes = Files.readAllBytes(path)
      deserialize(bytes)
    }

  /**
   * Derive the conventional sidecar file path for a bitmap index column.
   * E.g., for `/data/table/part-0.parquet` and column `status`, produces
   * `/data/table/part-0.parquet.bitmap.status`.
   */
  def sidecarPath(parquetPath: Path, columnName: String): Path =
    parquetPath.resolveSibling(parquetPath.getFileName.toString + ".bitmap." + columnName)

  // ---------------------------------------------------------------------------
  // Internal: value extraction from Arrow vectors
  // ---------------------------------------------------------------------------

  private def extractValue(vector: FieldVector, row: Int): String = vector match {
    case v: VarCharVector =>
      new String(v.get(row), StandardCharsets.UTF_8)

    case v: IntVector =>
      v.get(row).toString

    case v: BigIntVector =>
      v.get(row).toString

    case v: BitVector =>
      if (v.get(row) == 1) "true" else "false"

    case other =>
      val obj = other.getObject(row)
      if (obj != null) obj.toString
      else "null"
  }
}
