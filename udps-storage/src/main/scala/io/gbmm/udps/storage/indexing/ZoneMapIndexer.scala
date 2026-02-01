package io.gbmm.udps.storage.indexing

import io.gbmm.udps.storage.parquet.Predicate

import org.apache.arrow.vector._
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.PrimitiveType

import java.time.{Instant, LocalDate}
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * A single zone map entry capturing min/max/nullCount statistics for one
 * column within one row group.
 *
 * Values are stored as encoded strings so they can be universally compared
 * across types and serialised into Parquet key-value metadata.
 *
 * @param columnName    the column this entry belongs to
 * @param rowGroupIndex the zero-based row group ordinal
 * @param minValue      the minimum non-null value (None when all values are null)
 * @param maxValue      the maximum non-null value (None when all values are null)
 * @param nullCount     number of null values in this row group for this column
 * @param rowCount      total number of rows in this row group for this column
 */
final case class ZoneMapEntry(
    columnName: String,
    rowGroupIndex: Int,
    minValue: Option[String],
    maxValue: Option[String],
    nullCount: Long,
    rowCount: Long
)

/**
 * A complete zone map index for a file, consisting of entries for every
 * (column, rowGroup) pair.
 */
final case class ZoneMapIndex(entries: Vector[ZoneMapEntry]) {

  /** All entries for a given column. */
  def entriesForColumn(columnName: String): Vector[ZoneMapEntry] =
    entries.filter(_.columnName == columnName)

  /** Entry for a specific column and row group, if present. */
  def entry(columnName: String, rowGroupIndex: Int): Option[ZoneMapEntry] =
    entries.find(e => e.columnName == columnName && e.rowGroupIndex == rowGroupIndex)

  /** The set of distinct column names covered by this index. */
  def columns: Set[String] = entries.map(_.columnName).toSet

  /** The number of row groups indexed. */
  def rowGroupCount: Int =
    if (entries.isEmpty) 0
    else entries.map(_.rowGroupIndex).max + 1
}

object ZoneMapIndex {
  val empty: ZoneMapIndex = ZoneMapIndex(Vector.empty)
}

/**
 * Builds zone map indexes from Arrow VectorSchemaRoot batches and Parquet
 * file footer metadata, and evaluates predicates against them for row-group
 * pruning.
 */
object ZoneMapIndexer {

  // ---------------------------------------------------------------------------
  // Encoding helpers — values are normalised to zero-padded / fixed-width
  // strings so that lexicographic comparison preserves numeric ordering.
  // ---------------------------------------------------------------------------

  private val LongPadWidth = 20
  private val IntPadWidth = 11

  private def encodeLong(v: Long): String = {
    val shifted = v - Long.MinValue
    shifted.toString.reverse.padTo(LongPadWidth, '0').reverse
  }

  private def encodeInt(v: Int): String = {
    val shifted = v.toLong - Int.MinValue.toLong
    shifted.toString.reverse.padTo(IntPadWidth, '0').reverse
  }

  private def encodeFloat(v: Float): String =
    encodeDouble(v.toDouble)

  private def encodeDouble(v: Double): String = {
    val bits = java.lang.Double.doubleToLongBits(v)
    val adjusted = if (bits < 0) ~bits else bits ^ Long.MinValue
    adjusted.toString.reverse.padTo(LongPadWidth, '0').reverse
  }

  private def encodeString(v: String): String = v

  private def encodeDate(v: LocalDate): String =
    encodeLong(v.toEpochDay)

  private def encodeValue(v: Any): Option[String] = v match {
    case null                          => None
    case i: java.lang.Integer          => Some(encodeInt(i.intValue()))
    case l: java.lang.Long             => Some(encodeLong(l.longValue()))
    case f: java.lang.Float            => Some(encodeFloat(f.floatValue()))
    case d: java.lang.Double           => Some(encodeDouble(d.doubleValue()))
    case s: String                     => Some(encodeString(s))
    case ld: LocalDate                 => Some(encodeDate(ld))
    case inst: Instant                 => Some(encodeLong(inst.toEpochMilli))
    case _                             => Some(v.toString)
  }

  // ---------------------------------------------------------------------------
  // Build from Arrow VectorSchemaRoot
  // ---------------------------------------------------------------------------

  /**
   * Extract zone map entries from all columns in an Arrow VectorSchemaRoot
   * for the given row group index.
   */
  def buildFromVectorSchemaRoot(
      root: VectorSchemaRoot,
      rowGroupIndex: Int
  ): ZoneMapIndex = {
    val rowCount = root.getRowCount
    val fields = root.getSchema.getFields.asScala.toVector

    val entries = fields.flatMap { field =>
      val vector = root.getVector(field.getName)
      extractFromVector(vector, field.getName, rowGroupIndex, rowCount)
    }

    ZoneMapIndex(entries)
  }

  private def extractFromVector(
      vector: FieldVector,
      columnName: String,
      rowGroupIndex: Int,
      rowCount: Int
  ): Option[ZoneMapEntry] = {
    var nullCount = 0L
    var minEncoded: Option[String] = None
    var maxEncoded: Option[String] = None

    var i = 0
    while (i < rowCount) {
      if (vector.isNull(i)) {
        nullCount += 1
      } else {
        val encoded = encodeVectorValue(vector, i)
        encoded.foreach { enc =>
          minEncoded = minEncoded match {
            case None                       => Some(enc)
            case Some(cur) if enc < cur     => Some(enc)
            case existing                   => existing
          }
          maxEncoded = maxEncoded match {
            case None                       => Some(enc)
            case Some(cur) if enc > cur     => Some(enc)
            case existing                   => existing
          }
        }
      }
      i += 1
    }

    Some(ZoneMapEntry(
      columnName = columnName,
      rowGroupIndex = rowGroupIndex,
      minValue = minEncoded,
      maxValue = maxEncoded,
      nullCount = nullCount,
      rowCount = rowCount.toLong
    ))
  }

  private def encodeVectorValue(vector: FieldVector, index: Int): Option[String] =
    vector match {
      case v: IntVector       => Some(encodeInt(v.get(index)))
      case v: BigIntVector    => Some(encodeLong(v.get(index)))
      case v: Float4Vector    => Some(encodeFloat(v.get(index)))
      case v: Float8Vector    => Some(encodeDouble(v.get(index)))
      case v: VarCharVector   =>
        val bytes = v.get(index)
        if (bytes == null) None
        else Some(encodeString(new String(bytes, java.nio.charset.StandardCharsets.UTF_8)))
      case v: SmallIntVector  => Some(encodeInt(v.get(index).toInt))
      case v: TinyIntVector   => Some(encodeInt(v.get(index).toInt))
      case v: DateDayVector   => Some(encodeLong(v.get(index).toLong))
      case v: DateMilliVector => Some(encodeLong(v.get(index)))
      case v: TimeStampVector => Some(encodeLong(v.get(index)))
      case _                  => None
    }

  // ---------------------------------------------------------------------------
  // Build from Parquet file footer metadata
  // ---------------------------------------------------------------------------

  /**
   * Extract zone map entries from Parquet file footer statistics.
   */
  def buildFromParquetMetadata(metadata: ParquetMetadata): ZoneMapIndex = {
    val blocks = metadata.getBlocks.asScala.toVector
    val fileSchema = metadata.getFileMetaData.getSchema

    val entries = blocks.zipWithIndex.flatMap { case (block, rowGroupIndex) =>
      val columns = block.getColumns.asScala.toVector
      columns.flatMap { columnChunkMeta =>
        val colPath = columnChunkMeta.getPath.toDotString
        val stats = columnChunkMeta.getStatistics
        val rowCount = block.getRowCount

        if (stats == null || stats.isEmpty) {
          Some(ZoneMapEntry(
            columnName = colPath,
            rowGroupIndex = rowGroupIndex,
            minValue = None,
            maxValue = None,
            nullCount = 0L,
            rowCount = rowCount
          ))
        } else {
          val primitiveType: Option[PrimitiveType] = fileSchema.getColumns.asScala
            .find(cd => cd.getPath.mkString(".") == colPath)
            .map(_.getPrimitiveType)

          val (minEnc: Option[String], maxEnc: Option[String]) = primitiveType match {
            case Some(pt) => encodeParquetStats(stats, pt)
            case None     => (None: Option[String], None: Option[String])
          }

          Some(ZoneMapEntry(
            columnName = colPath,
            rowGroupIndex = rowGroupIndex,
            minValue = minEnc,
            maxValue = maxEnc,
            nullCount = stats.getNumNulls,
            rowCount = rowCount
          ))
        }
      }
    }

    ZoneMapIndex(entries)
  }

  private def encodeParquetStats(
      stats: org.apache.parquet.column.statistics.Statistics[_ <: Comparable[_]],
      primitiveType: PrimitiveType
  ): (Option[String], Option[String]) = {
    if (!stats.hasNonNullValue) return (None, None)

    import PrimitiveType.PrimitiveTypeName._

    primitiveType.getPrimitiveTypeName match {
      case INT32 =>
        val s = stats.asInstanceOf[org.apache.parquet.column.statistics.IntStatistics]
        (Some(encodeInt(s.getMin)), Some(encodeInt(s.getMax)))

      case INT64 =>
        val s = stats.asInstanceOf[org.apache.parquet.column.statistics.LongStatistics]
        (Some(encodeLong(s.getMin)), Some(encodeLong(s.getMax)))

      case FLOAT =>
        val s = stats.asInstanceOf[org.apache.parquet.column.statistics.FloatStatistics]
        (Some(encodeFloat(s.getMin)), Some(encodeFloat(s.getMax)))

      case DOUBLE =>
        val s = stats.asInstanceOf[org.apache.parquet.column.statistics.DoubleStatistics]
        (Some(encodeDouble(s.getMin)), Some(encodeDouble(s.getMax)))

      case BINARY | FIXED_LEN_BYTE_ARRAY =>
        val s = stats.asInstanceOf[org.apache.parquet.column.statistics.BinaryStatistics]
        val minStr = Option(s.genericGetMin).map(_.toStringUsingUTF8)
        val maxStr = Option(s.genericGetMax).map(_.toStringUsingUTF8)
        (minStr, maxStr)

      case BOOLEAN =>
        val s = stats.asInstanceOf[org.apache.parquet.column.statistics.BooleanStatistics]
        (Some(if (s.getMin) "1" else "0"), Some(if (s.getMax) "1" else "0"))

      case INT96 =>
        (None, None)
    }
  }

  // ---------------------------------------------------------------------------
  // Predicate evaluation — row group pruning
  // ---------------------------------------------------------------------------

  /**
   * Returns `true` if the given row group can be safely skipped (i.e. no
   * rows in the group can satisfy the predicate).
   */
  def shouldSkipRowGroup(
      index: ZoneMapIndex,
      rowGroup: Int,
      predicate: Predicate
  ): Boolean = predicate match {

    case Predicate.Eq(column, value) =>
      index.entry(column, rowGroup) match {
        case None => false
        case Some(e) =>
          encodeValue(value) match {
            case None =>
              // Eq(col, null) semantics: skip if no nulls
              e.nullCount == 0
            case Some(enc) =>
              (e.minValue, e.maxValue) match {
                case (Some(mn), Some(mx)) => enc < mn || enc > mx
                case _                    => false
              }
          }
      }

    case Predicate.NotEq(column, value) =>
      index.entry(column, rowGroup) match {
        case None => false
        case Some(e) =>
          encodeValue(value) match {
            case None =>
              // NotEq(col, null): skip if all are null
              e.nullCount == e.rowCount
            case Some(enc) =>
              (e.minValue, e.maxValue) match {
                case (Some(mn), Some(mx)) =>
                  mn == mx && mn == enc && e.nullCount == 0
                case _ => false
              }
          }
      }

    case Predicate.Lt(column, value) =>
      index.entry(column, rowGroup) match {
        case None => false
        case Some(e) =>
          encodeValue(value) match {
            case None => false
            case Some(enc) =>
              e.minValue match {
                case Some(mn) => mn >= enc
                case None     => e.nullCount == e.rowCount
              }
          }
      }

    case Predicate.LtEq(column, value) =>
      index.entry(column, rowGroup) match {
        case None => false
        case Some(e) =>
          encodeValue(value) match {
            case None => false
            case Some(enc) =>
              e.minValue match {
                case Some(mn) => mn > enc
                case None     => e.nullCount == e.rowCount
              }
          }
      }

    case Predicate.Gt(column, value) =>
      index.entry(column, rowGroup) match {
        case None => false
        case Some(e) =>
          encodeValue(value) match {
            case None => false
            case Some(enc) =>
              e.maxValue match {
                case Some(mx) => mx <= enc
                case None     => e.nullCount == e.rowCount
              }
          }
      }

    case Predicate.GtEq(column, value) =>
      index.entry(column, rowGroup) match {
        case None => false
        case Some(e) =>
          encodeValue(value) match {
            case None => false
            case Some(enc) =>
              e.maxValue match {
                case Some(mx) => mx < enc
                case None     => e.nullCount == e.rowCount
              }
          }
      }

    case Predicate.In(column, values) =>
      if (values.isEmpty) true
      else {
        index.entry(column, rowGroup) match {
          case None => false
          case Some(e) =>
            (e.minValue, e.maxValue) match {
              case (Some(mn), Some(mx)) =>
                val encoded = values.flatMap(encodeValue)
                if (encoded.isEmpty) {
                  e.nullCount == 0
                } else {
                  encoded.forall(enc => enc < mn || enc > mx)
                }
              case _ => false
            }
        }
      }

    case Predicate.IsNull(column) =>
      index.entry(column, rowGroup) match {
        case None    => false
        case Some(e) => e.nullCount == 0
      }

    case Predicate.IsNotNull(column) =>
      index.entry(column, rowGroup) match {
        case None    => false
        case Some(e) => e.nullCount == e.rowCount
      }

    case Predicate.And(left, right) =>
      shouldSkipRowGroup(index, rowGroup, left) ||
        shouldSkipRowGroup(index, rowGroup, right)

    case Predicate.Or(left, right) =>
      shouldSkipRowGroup(index, rowGroup, left) &&
        shouldSkipRowGroup(index, rowGroup, right)

    case Predicate.Not(inner) =>
      // Negation inverts skip logic — conservative: do not skip unless
      // we can prove the inner predicate is always true for this row group.
      // For safety, we only skip when the inner predicate covers all rows.
      inner match {
        case Predicate.IsNull(col) =>
          shouldSkipRowGroup(index, rowGroup, Predicate.IsNotNull(col))
        case Predicate.IsNotNull(col) =>
          shouldSkipRowGroup(index, rowGroup, Predicate.IsNull(col))
        case _ =>
          false
      }
  }

  // ---------------------------------------------------------------------------
  // Serialization — to/from Parquet key-value metadata
  // ---------------------------------------------------------------------------

  private val MetadataPrefix = "zonemap."
  private val EntriesCountKey = "zonemap.count"

  /**
   * Serialize a [[ZoneMapIndex]] to a flat Map[String, String] suitable for
   * storage in Parquet file-level key-value metadata.
   */
  def serialize(index: ZoneMapIndex): Map[String, String] = {
    val builder = Map.newBuilder[String, String]
    builder += (EntriesCountKey -> index.entries.size.toString)

    index.entries.zipWithIndex.foreach { case (entry, i) =>
      val prefix = s"$MetadataPrefix$i."
      builder += (s"${prefix}column"   -> entry.columnName)
      builder += (s"${prefix}rowGroup" -> entry.rowGroupIndex.toString)
      entry.minValue.foreach(v => builder += (s"${prefix}min" -> v))
      entry.maxValue.foreach(v => builder += (s"${prefix}max" -> v))
      builder += (s"${prefix}nullCount" -> entry.nullCount.toString)
      builder += (s"${prefix}rowCount"  -> entry.rowCount.toString)
    }

    builder.result()
  }

  /**
   * Deserialize a [[ZoneMapIndex]] from Parquet key-value metadata.
   */
  def deserialize(metadata: Map[String, String]): ZoneMapIndex = {
    val count = metadata.get(EntriesCountKey).flatMap(s => Try(s.toInt).toOption).getOrElse(0)

    val entries = (0 until count).flatMap { i =>
      val prefix = s"$MetadataPrefix$i."
      for {
        column   <- metadata.get(s"${prefix}column")
        rowGroup <- metadata.get(s"${prefix}rowGroup").flatMap(s => Try(s.toInt).toOption)
      } yield {
        ZoneMapEntry(
          columnName = column,
          rowGroupIndex = rowGroup,
          minValue = metadata.get(s"${prefix}min"),
          maxValue = metadata.get(s"${prefix}max"),
          nullCount = metadata.get(s"${prefix}nullCount").flatMap(s => Try(s.toLong).toOption).getOrElse(0L),
          rowCount = metadata.get(s"${prefix}rowCount").flatMap(s => Try(s.toLong).toOption).getOrElse(0L)
        )
      }
    }.toVector

    ZoneMapIndex(entries)
  }

  // ---------------------------------------------------------------------------
  // Convenience: merge indexes (e.g. when appending row groups)
  // ---------------------------------------------------------------------------

  /**
   * Merge two zone map indexes. Entries from `other` are appended; if both
   * indexes contain entries for the same (column, rowGroup) pair, the entry
   * from `other` wins.
   */
  def merge(base: ZoneMapIndex, other: ZoneMapIndex): ZoneMapIndex = {
    val otherKeys = other.entries.map(e => (e.columnName, e.rowGroupIndex)).toSet
    val kept = base.entries.filterNot(e => otherKeys.contains((e.columnName, e.rowGroupIndex)))
    ZoneMapIndex(kept ++ other.entries)
  }
}
