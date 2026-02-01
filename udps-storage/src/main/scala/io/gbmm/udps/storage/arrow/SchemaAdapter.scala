package io.gbmm.udps.storage.arrow

import io.gbmm.udps.core.domain._

import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import java.time.Instant
import java.util.{UUID, ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

import scala.jdk.CollectionConverters._

/**
 * Bidirectional adapter between UDPS domain models and Apache Arrow Schema.
 *
 * Preserves custom metadata (PII tags, descriptions, table properties) via
 * Arrow schema-level and field-level metadata maps. A "udps.datatype" field
 * metadata entry disambiguates Arrow types that map to multiple UDPS DataType
 * variants (e.g. Timestamp with different time-units).
 */
object SchemaAdapter {

  // ---------------------------------------------------------------------------
  // Metadata key constants
  // ---------------------------------------------------------------------------

  private val TagPrefix = "tag."
  private val DescriptionKey = "description"
  private val UdpsDataTypeKey = "udps.datatype"

  private val TableIdKey = "table.id"
  private val TableNameKey = "table.name"
  private val TableNamespaceKey = "table.namespace"
  private val SchemaVersionKey = "table.schema.version"
  private val SchemaCreatedAtKey = "table.schema.createdAt"
  private val SchemaUpdatedAtKey = "table.schema.updatedAt"
  private val PrimaryKeyKey = "table.primaryKey"
  private val PartitionKeysKey = "table.partitionKeys"
  private val TableCreatedAtKey = "table.createdAt"
  private val TableUpdatedAtKey = "table.updatedAt"
  private val StorageTierKey = "table.storageTier"
  private val CompressionKey = "table.compression"
  private val RowCountKey = "table.rowCount"
  private val SizeBytesKey = "table.sizeBytes"
  private val PartitionStrategyKey = "table.partition.strategy"
  private val PartitionColumnsKey = "table.partition.columns"
  private val PartitionCountKey = "table.partition.count"
  private val IndexCountKey = "table.index.count"
  private val IndexPrefixKey = "table.index."

  private val MetadataSeparator = ","

  // ---------------------------------------------------------------------------
  // TableMetadata -> Arrow Schema
  // ---------------------------------------------------------------------------

  def toArrowSchema(table: TableMetadata): Schema = {
    val fields: JList[Field] = new JArrayList[Field]()
    table.schema.columns.foreach { col =>
      fields.add(toArrowField(col))
    }

    val metadata: JMap[String, String] = new JHashMap[String, String]()

    metadata.put(TableIdKey, table.id.toString)
    metadata.put(TableNameKey, table.name)
    metadata.put(TableNamespaceKey, table.namespace)
    metadata.put(SchemaVersionKey, table.schema.version.toString)
    metadata.put(SchemaCreatedAtKey, table.schema.createdAt.toString)
    metadata.put(SchemaUpdatedAtKey, table.schema.updatedAt.toString)
    metadata.put(TableCreatedAtKey, table.createdAt.toString)
    metadata.put(TableUpdatedAtKey, table.updatedAt.toString)
    metadata.put(StorageTierKey, storageTierToString(table.storageTier))
    metadata.put(CompressionKey, compressionToString(table.compression))
    metadata.put(RowCountKey, table.rowCount.toString)
    metadata.put(SizeBytesKey, table.sizeBytes.toString)

    table.schema.primaryKey.foreach { pk =>
      metadata.put(PrimaryKeyKey, pk.mkString(MetadataSeparator))
    }

    if (table.schema.partitionKeys.nonEmpty) {
      metadata.put(PartitionKeysKey, table.schema.partitionKeys.mkString(MetadataSeparator))
    }

    table.partitionInfo.foreach { pi =>
      metadata.put(PartitionStrategyKey, partitionStrategyToString(pi.strategy))
      metadata.put(PartitionColumnsKey, pi.columns.mkString(MetadataSeparator))
      metadata.put(PartitionCountKey, pi.partitionCount.toString)
    }

    encodeIndexes(table.indexes, metadata)

    table.properties.foreach { case (k, v) =>
      metadata.put(k, v)
    }

    new Schema(fields, metadata)
  }

  // ---------------------------------------------------------------------------
  // Arrow Schema -> TableMetadata (partial reconstruction)
  // ---------------------------------------------------------------------------

  def fromArrowSchema(schema: Schema): TableMetadata = {
    val meta: Map[String, String] = Option(schema.getCustomMetadata)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty)

    val columns: Seq[ColumnMetadata] = schema.getFields.asScala.toSeq.map(fromArrowField)

    val primaryKey: Option[Seq[String]] = meta.get(PrimaryKeyKey).map(_.split(MetadataSeparator, -1).toSeq)

    val partitionKeys: Seq[String] = meta.get(PartitionKeysKey)
      .map(_.split(MetadataSeparator, -1).toSeq)
      .getOrElse(Seq.empty)

    val schemaVersion: Int = meta.get(SchemaVersionKey).map(_.toInt).getOrElse(1)
    val schemaCreatedAt: Instant = meta.get(SchemaCreatedAtKey).map(Instant.parse).getOrElse(Instant.EPOCH)
    val schemaUpdatedAt: Instant = meta.get(SchemaUpdatedAtKey).map(Instant.parse).getOrElse(Instant.EPOCH)

    val schemaInfo = SchemaInfo(
      columns = columns,
      primaryKey = primaryKey,
      partitionKeys = partitionKeys,
      version = schemaVersion,
      createdAt = schemaCreatedAt,
      updatedAt = schemaUpdatedAt
    )

    val tableId: UUID = meta.get(TableIdKey).map(UUID.fromString).getOrElse(new UUID(0L, 0L))
    val tableName: String = meta.getOrElse(TableNameKey, "")
    val tableNamespace: String = meta.getOrElse(TableNamespaceKey, "")
    val storageTier: StorageTier = meta.get(StorageTierKey).map(stringToStorageTier).getOrElse(StorageTier.Hot)
    val compression: CompressionCodec = meta.get(CompressionKey).map(stringToCompression).getOrElse(CompressionCodec.Uncompressed)
    val rowCount: Long = meta.get(RowCountKey).map(_.toLong).getOrElse(0L)
    val sizeBytes: Long = meta.get(SizeBytesKey).map(_.toLong).getOrElse(0L)
    val createdAt: Instant = meta.get(TableCreatedAtKey).map(Instant.parse).getOrElse(Instant.EPOCH)
    val updatedAt: Instant = meta.get(TableUpdatedAtKey).map(Instant.parse).getOrElse(Instant.EPOCH)

    val partitionInfo: Option[PartitionInfo] = meta.get(PartitionStrategyKey).map { stratStr =>
      PartitionInfo(
        strategy = stringToPartitionStrategy(stratStr),
        columns = meta.get(PartitionColumnsKey).map(_.split(MetadataSeparator, -1).toSeq).getOrElse(Seq.empty),
        partitionCount = meta.get(PartitionCountKey).map(_.toInt).getOrElse(0)
      )
    }

    val indexes: Seq[IndexInfo] = decodeIndexes(meta)

    val reservedKeys: Set[String] = Set(
      TableIdKey, TableNameKey, TableNamespaceKey,
      SchemaVersionKey, SchemaCreatedAtKey, SchemaUpdatedAtKey,
      TableCreatedAtKey, TableUpdatedAtKey,
      StorageTierKey, CompressionKey, RowCountKey, SizeBytesKey,
      PrimaryKeyKey, PartitionKeysKey,
      PartitionStrategyKey, PartitionColumnsKey, PartitionCountKey,
      IndexCountKey
    )

    val indexKeyPattern = (IndexPrefixKey + """\d+\..*""").r
    val properties: Map[String, String] = meta.filterNot { case (k, _) =>
      reservedKeys.contains(k) || indexKeyPattern.findFirstIn(k).isDefined
    }

    TableMetadata(
      id = tableId,
      name = tableName,
      namespace = tableNamespace,
      schema = schemaInfo,
      storageTier = storageTier,
      compression = compression,
      partitionInfo = partitionInfo,
      indexes = indexes,
      rowCount = rowCount,
      sizeBytes = sizeBytes,
      createdAt = createdAt,
      updatedAt = updatedAt,
      properties = properties
    )
  }

  // ---------------------------------------------------------------------------
  // ColumnMetadata <-> Arrow Field
  // ---------------------------------------------------------------------------

  def toArrowField(col: ColumnMetadata): Field = {
    val arrowType: ArrowType = dataTypeToArrowType(col.dataType)
    val fieldMeta: JMap[String, String] = new JHashMap[String, String]()

    fieldMeta.put(UdpsDataTypeKey, dataTypeDiscriminator(col.dataType))

    col.description.foreach(d => fieldMeta.put(DescriptionKey, d))

    col.tags.foreach { case (k, v) =>
      fieldMeta.put(TagPrefix + k, v)
    }

    col.statistics.foreach { stats =>
      fieldMeta.put("stats.nullCount", stats.nullCount.toString)
      fieldMeta.put("stats.distinctCount", stats.distinctCount.toString)
      stats.minValue.foreach(v => fieldMeta.put("stats.minValue", v))
      stats.maxValue.foreach(v => fieldMeta.put("stats.maxValue", v))
      stats.avgSize.foreach(v => fieldMeta.put("stats.avgSize", v.toString))
    }

    val children: JList[Field] = col.dataType match {
      case DataType.List(elementType) =>
        val child = toArrowField(ColumnMetadata(
          name = "element",
          dataType = elementType,
          nullable = true,
          description = None,
          tags = Map.empty,
          statistics = None
        ))
        val list = new JArrayList[Field]()
        list.add(child)
        list

      case DataType.Struct(fields) =>
        val list = new JArrayList[Field]()
        fields.foreach(f => list.add(toArrowField(f)))
        list

      case DataType.Map(keyType, valueType) =>
        val entriesField = buildMapEntriesField(keyType, valueType)
        val list = new JArrayList[Field]()
        list.add(entriesField)
        list

      case _ =>
        new JArrayList[Field]()
    }

    new Field(col.name, new FieldType(col.nullable, arrowType, null, fieldMeta), children)
  }

  def fromArrowField(field: Field): ColumnMetadata = {
    val fieldMeta: Map[String, String] = Option(field.getMetadata)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty)

    val udpsDiscriminator: Option[String] = fieldMeta.get(UdpsDataTypeKey)
    val dataType: DataType = arrowTypeToDataType(field, udpsDiscriminator)

    val description: Option[String] = fieldMeta.get(DescriptionKey)

    val tags: Map[String, String] = fieldMeta
      .collect { case (k, v) if k.startsWith(TagPrefix) => (k.stripPrefix(TagPrefix), v) }

    val statistics: Option[ColumnStatistics] = fieldMeta.get("stats.nullCount").map { nc =>
      ColumnStatistics(
        nullCount = nc.toLong,
        distinctCount = fieldMeta.get("stats.distinctCount").map(_.toLong).getOrElse(0L),
        minValue = fieldMeta.get("stats.minValue"),
        maxValue = fieldMeta.get("stats.maxValue"),
        avgSize = fieldMeta.get("stats.avgSize").map(_.toLong)
      )
    }

    ColumnMetadata(
      name = field.getName,
      dataType = dataType,
      nullable = field.isNullable,
      description = description,
      tags = tags,
      statistics = statistics
    )
  }

  // ---------------------------------------------------------------------------
  // DataType -> ArrowType
  // ---------------------------------------------------------------------------

  def dataTypeToArrowType(dt: DataType): ArrowType = dt match {
    case DataType.Boolean       => ArrowType.Bool.INSTANCE
    case DataType.Int8          => new ArrowType.Int(8, true)
    case DataType.Int16         => new ArrowType.Int(16, true)
    case DataType.Int32         => new ArrowType.Int(32, true)
    case DataType.Int64         => new ArrowType.Int(64, true)
    case DataType.UInt8         => new ArrowType.Int(8, false)
    case DataType.UInt16        => new ArrowType.Int(16, false)
    case DataType.UInt32        => new ArrowType.Int(32, false)
    case DataType.UInt64        => new ArrowType.Int(64, false)
    case DataType.Float16       => new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)
    case DataType.Float32       => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case DataType.Float64       => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case DataType.Decimal(p, s) => new ArrowType.Decimal(p, s, 128)
    case DataType.Utf8          => ArrowType.Utf8.INSTANCE
    case DataType.Binary        => ArrowType.Binary.INSTANCE
    case DataType.Date32        => new ArrowType.Date(DateUnit.DAY)
    case DataType.Date64        => new ArrowType.Date(DateUnit.MILLISECOND)
    case DataType.TimestampSec    => new ArrowType.Timestamp(TimeUnit.SECOND, null)
    case DataType.TimestampMillis => new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)
    case DataType.TimestampMicros => new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
    case DataType.TimestampNanos  => new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)
    case DataType.List(_)       => ArrowType.List.INSTANCE
    case DataType.Struct(_)     => ArrowType.Struct.INSTANCE
    case DataType.Map(_, _)     => new ArrowType.Map(false)
    case DataType.Null          => ArrowType.Null.INSTANCE
  }

  // ---------------------------------------------------------------------------
  // ArrowType + discriminator -> DataType
  // ---------------------------------------------------------------------------

  private def arrowTypeToDataType(field: Field, discriminator: Option[String]): DataType = {
    discriminator match {
      case Some(disc) => discriminatorToDataType(disc, field)
      case None       => inferDataTypeFromArrow(field)
    }
  }

  private def discriminatorToDataType(disc: String, field: Field): DataType = disc match {
    case "Boolean"        => DataType.Boolean
    case "Int8"           => DataType.Int8
    case "Int16"          => DataType.Int16
    case "Int32"          => DataType.Int32
    case "Int64"          => DataType.Int64
    case "UInt8"          => DataType.UInt8
    case "UInt16"         => DataType.UInt16
    case "UInt32"         => DataType.UInt32
    case "UInt64"         => DataType.UInt64
    case "Float16"        => DataType.Float16
    case "Float32"        => DataType.Float32
    case "Float64"        => DataType.Float64
    case d if d.startsWith("Decimal(") =>
      val inner = d.stripPrefix("Decimal(").stripSuffix(")")
      val parts = inner.split(",")
      DataType.Decimal(parts(0).trim.toInt, parts(1).trim.toInt)
    case "Utf8"           => DataType.Utf8
    case "Binary"         => DataType.Binary
    case "Date32"         => DataType.Date32
    case "Date64"         => DataType.Date64
    case "TimestampSec"   => DataType.TimestampSec
    case "TimestampMillis" => DataType.TimestampMillis
    case "TimestampMicros" => DataType.TimestampMicros
    case "TimestampNanos" => DataType.TimestampNanos
    case "List"           =>
      val children = field.getChildren.asScala.toSeq
      val elementType = if (children.nonEmpty) fromArrowField(children.head).dataType else DataType.Null
      DataType.List(elementType)
    case "Struct"         =>
      val children = field.getChildren.asScala.toSeq.map(fromArrowField)
      DataType.Struct(children)
    case "Map"            =>
      val entriesField = field.getChildren.asScala.toSeq.headOption
      val (keyType, valueType) = entriesField match {
        case Some(entries) =>
          val entryChildren = entries.getChildren.asScala.toSeq
          val kt = if (entryChildren.nonEmpty) fromArrowField(entryChildren.head).dataType else DataType.Null
          val vt = if (entryChildren.size > 1) fromArrowField(entryChildren(1)).dataType else DataType.Null
          (kt, vt)
        case None => (DataType.Null, DataType.Null)
      }
      DataType.Map(keyType, valueType)
    case "Null"           => DataType.Null
    case _                => inferDataTypeFromArrow(field)
  }

  private def inferDataTypeFromArrow(field: Field): DataType = {
    field.getType match {
      case _: ArrowType.Bool => DataType.Boolean

      case intType: ArrowType.Int =>
        (intType.getBitWidth, intType.getIsSigned) match {
          case (8, true)   => DataType.Int8
          case (16, true)  => DataType.Int16
          case (32, true)  => DataType.Int32
          case (64, true)  => DataType.Int64
          case (8, false)  => DataType.UInt8
          case (16, false) => DataType.UInt16
          case (32, false) => DataType.UInt32
          case (64, false) => DataType.UInt64
          case _           => DataType.Int64
        }

      case fpType: ArrowType.FloatingPoint =>
        fpType.getPrecision match {
          case FloatingPointPrecision.HALF   => DataType.Float16
          case FloatingPointPrecision.SINGLE => DataType.Float32
          case FloatingPointPrecision.DOUBLE => DataType.Float64
        }

      case decType: ArrowType.Decimal => DataType.Decimal(decType.getPrecision, decType.getScale)

      case _: ArrowType.Utf8 => DataType.Utf8

      case _: ArrowType.Binary => DataType.Binary

      case dateType: ArrowType.Date =>
        dateType.getUnit match {
          case DateUnit.DAY         => DataType.Date32
          case DateUnit.MILLISECOND => DataType.Date64
        }

      case tsType: ArrowType.Timestamp =>
        tsType.getUnit match {
          case TimeUnit.SECOND      => DataType.TimestampSec
          case TimeUnit.MILLISECOND => DataType.TimestampMillis
          case TimeUnit.MICROSECOND => DataType.TimestampMicros
          case TimeUnit.NANOSECOND  => DataType.TimestampNanos
        }

      case _: ArrowType.List =>
        val children = field.getChildren.asScala.toSeq
        val elementType = if (children.nonEmpty) fromArrowField(children.head).dataType else DataType.Null
        DataType.List(elementType)

      case _: ArrowType.Struct =>
        val children = field.getChildren.asScala.toSeq.map(fromArrowField)
        DataType.Struct(children)

      case _: ArrowType.Map =>
        val entriesField = field.getChildren.asScala.toSeq.headOption
        val (keyType, valueType) = entriesField match {
          case Some(entries) =>
            val entryChildren = entries.getChildren.asScala.toSeq
            val kt = if (entryChildren.nonEmpty) fromArrowField(entryChildren.head).dataType else DataType.Null
            val vt = if (entryChildren.size > 1) fromArrowField(entryChildren(1)).dataType else DataType.Null
            (kt, vt)
          case None => (DataType.Null, DataType.Null)
        }
        DataType.Map(keyType, valueType)

      case _: ArrowType.Null => DataType.Null

      case _ => DataType.Binary
    }
  }

  // ---------------------------------------------------------------------------
  // DataType discriminator string (for round-trip fidelity)
  // ---------------------------------------------------------------------------

  private def dataTypeDiscriminator(dt: DataType): String = dt match {
    case DataType.Boolean        => "Boolean"
    case DataType.Int8           => "Int8"
    case DataType.Int16          => "Int16"
    case DataType.Int32          => "Int32"
    case DataType.Int64          => "Int64"
    case DataType.UInt8          => "UInt8"
    case DataType.UInt16         => "UInt16"
    case DataType.UInt32         => "UInt32"
    case DataType.UInt64         => "UInt64"
    case DataType.Float16        => "Float16"
    case DataType.Float32        => "Float32"
    case DataType.Float64        => "Float64"
    case DataType.Decimal(p, s)  => s"Decimal($p, $s)"
    case DataType.Utf8           => "Utf8"
    case DataType.Binary         => "Binary"
    case DataType.Date32         => "Date32"
    case DataType.Date64         => "Date64"
    case DataType.TimestampSec   => "TimestampSec"
    case DataType.TimestampMillis => "TimestampMillis"
    case DataType.TimestampMicros => "TimestampMicros"
    case DataType.TimestampNanos => "TimestampNanos"
    case DataType.List(_)        => "List"
    case DataType.Struct(_)      => "Struct"
    case DataType.Map(_, _)      => "Map"
    case DataType.Null           => "Null"
  }

  // ---------------------------------------------------------------------------
  // Map entries helper: Arrow Map requires a "struct" child named "entries"
  // ---------------------------------------------------------------------------

  private def buildMapEntriesField(keyType: DataType, valueType: DataType): Field = {
    val keyField = toArrowField(ColumnMetadata(
      name = "key",
      dataType = keyType,
      nullable = false,
      description = None,
      tags = Map.empty,
      statistics = None
    ))
    val valueField = toArrowField(ColumnMetadata(
      name = "value",
      dataType = valueType,
      nullable = true,
      description = None,
      tags = Map.empty,
      statistics = None
    ))
    val children = new JArrayList[Field]()
    children.add(keyField)
    children.add(valueField)
    new Field("entries", new FieldType(false, ArrowType.Struct.INSTANCE, null), children)
  }

  // ---------------------------------------------------------------------------
  // StorageTier serialization
  // ---------------------------------------------------------------------------

  private def storageTierToString(tier: StorageTier): String = tier match {
    case StorageTier.Hot     => "Hot"
    case StorageTier.Warm    => "Warm"
    case StorageTier.Cold    => "Cold"
    case StorageTier.Archive => "Archive"
  }

  private def stringToStorageTier(s: String): StorageTier = s match {
    case "Hot"     => StorageTier.Hot
    case "Warm"    => StorageTier.Warm
    case "Cold"    => StorageTier.Cold
    case "Archive" => StorageTier.Archive
    case _         => StorageTier.Hot
  }

  // ---------------------------------------------------------------------------
  // CompressionCodec serialization
  // ---------------------------------------------------------------------------

  private def compressionToString(codec: CompressionCodec): String = codec match {
    case CompressionCodec.Uncompressed          => "Uncompressed"
    case CompressionCodec.Snappy                => "Snappy"
    case CompressionCodec.Gzip                  => "Gzip"
    case CompressionCodec.Lz4                   => "Lz4"
    case CompressionCodec.Zstd                  => "Zstd"
    case CompressionCodec.Brotli                => "Brotli"
    case CompressionCodec.DeltaBinaryPacked     => "DeltaBinaryPacked"
    case CompressionCodec.DeltaLengthByteArray  => "DeltaLengthByteArray"
  }

  private def stringToCompression(s: String): CompressionCodec = s match {
    case "Uncompressed"          => CompressionCodec.Uncompressed
    case "Snappy"                => CompressionCodec.Snappy
    case "Gzip"                  => CompressionCodec.Gzip
    case "Lz4"                   => CompressionCodec.Lz4
    case "Zstd"                  => CompressionCodec.Zstd
    case "Brotli"                => CompressionCodec.Brotli
    case "DeltaBinaryPacked"     => CompressionCodec.DeltaBinaryPacked
    case "DeltaLengthByteArray"  => CompressionCodec.DeltaLengthByteArray
    case _                       => CompressionCodec.Uncompressed
  }

  // ---------------------------------------------------------------------------
  // PartitionStrategy serialization
  // ---------------------------------------------------------------------------

  private def partitionStrategyToString(strategy: PartitionStrategy): String = strategy match {
    case PartitionStrategy.Hash       => "Hash"
    case PartitionStrategy.Range      => "Range"
    case PartitionStrategy.List       => "List"
    case PartitionStrategy.RoundRobin => "RoundRobin"
  }

  private def stringToPartitionStrategy(s: String): PartitionStrategy = s match {
    case "Hash"       => PartitionStrategy.Hash
    case "Range"      => PartitionStrategy.Range
    case "List"       => PartitionStrategy.List
    case "RoundRobin" => PartitionStrategy.RoundRobin
    case _            => PartitionStrategy.Hash
  }

  // ---------------------------------------------------------------------------
  // IndexInfo serialization into schema metadata
  // ---------------------------------------------------------------------------

  private def encodeIndexes(indexes: Seq[IndexInfo], metadata: JMap[String, String]): Unit = {
    metadata.put(IndexCountKey, indexes.size.toString)
    indexes.zipWithIndex.foreach { case (idx, i) =>
      val prefix = s"${IndexPrefixKey}$i."
      metadata.put(prefix + "name", idx.name)
      metadata.put(prefix + "type", indexTypeToString(idx.indexType))
      metadata.put(prefix + "columns", idx.columns.mkString(MetadataSeparator))
      metadata.put(prefix + "sizeBytes", idx.sizeBytes.toString)
    }
  }

  private def decodeIndexes(meta: Map[String, String]): Seq[IndexInfo] = {
    val count = meta.get(IndexCountKey).map(_.toInt).getOrElse(0)
    (0 until count).map { i =>
      val prefix = s"${IndexPrefixKey}$i."
      IndexInfo(
        name = meta.getOrElse(prefix + "name", ""),
        indexType = meta.get(prefix + "type").map(stringToIndexType).getOrElse(IndexType.BTree),
        columns = meta.get(prefix + "columns").map(_.split(MetadataSeparator, -1).toSeq).getOrElse(Seq.empty),
        sizeBytes = meta.get(prefix + "sizeBytes").map(_.toLong).getOrElse(0L)
      )
    }
  }

  // ---------------------------------------------------------------------------
  // IndexType serialization
  // ---------------------------------------------------------------------------

  private def indexTypeToString(it: IndexType): String = it match {
    case IndexType.BTree       => "BTree"
    case IndexType.ZoneMap     => "ZoneMap"
    case IndexType.BloomFilter => "BloomFilter"
    case IndexType.Bitmap      => "Bitmap"
    case IndexType.SkipList    => "SkipList"
    case IndexType.Inverted    => "Inverted"
  }

  private def stringToIndexType(s: String): IndexType = s match {
    case "BTree"       => IndexType.BTree
    case "ZoneMap"     => IndexType.ZoneMap
    case "BloomFilter" => IndexType.BloomFilter
    case "Bitmap"      => IndexType.Bitmap
    case "SkipList"    => IndexType.SkipList
    case "Inverted"    => IndexType.Inverted
    case _             => IndexType.BTree
  }
}
