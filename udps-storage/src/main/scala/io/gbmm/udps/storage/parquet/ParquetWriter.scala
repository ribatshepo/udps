package io.gbmm.udps.storage.parquet

import cats.effect.{IO, Resource}
import com.typesafe.scalalogging.LazyLogging

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{BaseRepeatedValueVector, ListVector, MapVector, StructVector}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.hadoop.{ParquetWriter => HadoopParquetWriter}
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema._

import java.time.Instant
import java.util.{HashMap => JHashMap}

import scala.jdk.CollectionConverters._

/**
 * Writes Apache Arrow [[VectorSchemaRoot]] batches to Parquet files
 * using cats-effect [[IO]] and [[Resource]] for safe lifecycle management.
 *
 * Internally delegates to a custom [[ArrowWriteSupport]] that translates
 * Arrow columnar vectors into Parquet row-oriented RecordConsumer calls.
 */
object ParquetWriter extends LazyLogging {

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /**
   * Metadata attached to each Parquet file for provenance tracking.
   *
   * @param tableName     logical table name
   * @param namespace     logical namespace / database
   * @param partitionInfo free-form partition descriptor (e.g. "date=2026-02-01")
   * @param creationTime  when the file was written
   * @param properties    arbitrary extra key-value pairs
   */
  final case class FileMetadata(
      tableName: String,
      namespace: String,
      partitionInfo: Option[String],
      creationTime: Instant,
      properties: Map[String, String]
  )

  object FileMetadata {

    /**
     * Creates a [[FileMetadata]] with the current wall-clock time and no
     * extra properties.
     */
    def now(tableName: String, namespace: String, partitionInfo: Option[String]): FileMetadata =
      FileMetadata(
        tableName = tableName,
        namespace = namespace,
        partitionInfo = partitionInfo,
        creationTime = Instant.now(),
        properties = Map.empty
      )
  }

  /**
   * Opens a Parquet writer as a [[Resource]]. The writer is closed (and
   * the file footer flushed) when the resource is released.
   *
   * @param path              destination Parquet file path
   * @param root              Arrow VectorSchemaRoot whose schema defines the
   *                          Parquet message type
   * @param config            compression and layout configuration
   * @param metadata          file-level metadata written into the Parquet footer
   * @param hadoopConf        Hadoop configuration (default: empty)
   * @param overwrite         if true, overwrite an existing file; otherwise append
   *                          by writing a new file (Parquet does not support
   *                          in-place append)
   */
  def resource(
      path: Path,
      root: VectorSchemaRoot,
      config: CompressionConfig,
      metadata: FileMetadata,
      hadoopConf: Configuration = new Configuration(),
      overwrite: Boolean = true
  ): Resource[IO, Writer] =
    Resource.make(
      IO.blocking {
        val parquetSchema  = arrowSchemaToParquetMessageType(root.getSchema)
        val extraMeta      = buildExtraMetadata(metadata)
        val rowIndexHolder = new RowIndexHolder
        val writeSupport   = new ArrowWriteSupport(parquetSchema, extraMeta, rowIndexHolder)

        val writer = new ArrowParquetWriterBuilder(writeSupport, path)
          .withConf(hadoopConf)
          .withCompressionCodec(config.compressionCodec)
          .withRowGroupSize(config.rowGroupSizeBytes)
          .withPageSize(config.pageSizeBytes)
          .withDictionaryPageSize(config.dictionaryPageSizeBytes)
          .withDictionaryEncoding(config.enableDictionary)
          .withWriterVersion(config.writerVersion)
          .withWriteMode(if (overwrite) Mode.OVERWRITE else Mode.CREATE)
          .build()

        logger.info(
          "Opened Parquet writer: path={}, compression={}, rowGroupSize={}",
          path,
          config.compressionCodec,
          config.rowGroupSizeBytes.toString
        )

        new Writer(writer, rowIndexHolder)
      }
    )(writer => IO.blocking(writer.close()))

  /**
   * Convenience: writes an entire [[VectorSchemaRoot]] batch to a single
   * Parquet file in one shot.
   */
  def writeBatch(
      path: Path,
      root: VectorSchemaRoot,
      config: CompressionConfig,
      metadata: FileMetadata,
      hadoopConf: Configuration = new Configuration()
  ): IO[Long] =
    resource(path, root, config, metadata, hadoopConf).use { writer =>
      writer.writeBatch(root)
    }

  // -------------------------------------------------------------------------
  // Writer handle
  // -------------------------------------------------------------------------

  /**
   * Thin wrapper around the Hadoop [[HadoopParquetWriter]] providing an
   * effect-safe API.
   */
  final class Writer private[ParquetWriter] (
      underlying: HadoopParquetWriter[VectorSchemaRoot],
      rowIndexHolder: RowIndexHolder
  ) {

    private var totalRows: Long = 0L

    /**
     * Writes every row in the given [[VectorSchemaRoot]] batch.
     * Returns the cumulative number of rows written so far.
     */
    def writeBatch(batch: VectorSchemaRoot): IO[Long] =
      IO.blocking {
        val rowCount = batch.getRowCount
        var idx = 0
        while (idx < rowCount) {
          rowIndexHolder.index = idx
          underlying.write(batch)
          idx += 1
        }
        totalRows += rowCount
        logger.debug("Wrote batch of {} rows (total: {})", rowCount.toString, totalRows.toString)
        totalRows
      }

    /** Returns the total rows written so far. */
    def rowsWritten: Long = totalRows

    private[ParquetWriter] def close(): Unit = {
      underlying.close()
      logger.info("Closed Parquet writer after {} rows", totalRows.toString)
    }
  }

  // -------------------------------------------------------------------------
  // Arrow Schema -> Parquet MessageType conversion
  // -------------------------------------------------------------------------

  private val ParquetSchemaName = "arrow_schema"

  private[parquet] def arrowSchemaToParquetMessageType(
      schema: org.apache.arrow.vector.types.pojo.Schema
  ): MessageType = {
    val fields = schema.getFields.asScala.map(arrowFieldToParquetType).toList
    new MessageType(ParquetSchemaName, fields.asJava)
  }

  private def arrowFieldToParquetType(
      field: org.apache.arrow.vector.types.pojo.Field
  ): org.apache.parquet.schema.Type = {
    val repetition =
      if (field.isNullable) Type.Repetition.OPTIONAL
      else Type.Repetition.REQUIRED

    field.getType match {
      case _: org.apache.arrow.vector.types.pojo.ArrowType.Bool =>
        Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition)
          .named(field.getName)

      case intType: org.apache.arrow.vector.types.pojo.ArrowType.Int =>
        intType.getBitWidth match {
          case 8 =>
            Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
              .as(LogicalTypeAnnotation.intType(8, intType.getIsSigned))
              .named(field.getName)
          case 16 =>
            Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
              .as(LogicalTypeAnnotation.intType(16, intType.getIsSigned))
              .named(field.getName)
          case 32 =>
            Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
              .as(LogicalTypeAnnotation.intType(32, intType.getIsSigned))
              .named(field.getName)
          case _ =>
            Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
              .as(LogicalTypeAnnotation.intType(64, intType.getIsSigned))
              .named(field.getName)
        }

      case fpType: org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint =>
        import org.apache.arrow.vector.types.FloatingPointPrecision
        fpType.getPrecision match {
          case FloatingPointPrecision.HALF | FloatingPointPrecision.SINGLE =>
            Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition)
              .named(field.getName)
          case FloatingPointPrecision.DOUBLE =>
            Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition)
              .named(field.getName)
        }

      case decType: org.apache.arrow.vector.types.pojo.ArrowType.Decimal =>
        Types
          .primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
          .length(decimalByteLength(decType.getPrecision))
          .as(LogicalTypeAnnotation.decimalType(decType.getScale, decType.getPrecision))
          .named(field.getName)

      case _: org.apache.arrow.vector.types.pojo.ArrowType.Utf8 =>
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
          .as(LogicalTypeAnnotation.stringType())
          .named(field.getName)

      case _: org.apache.arrow.vector.types.pojo.ArrowType.Binary =>
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
          .named(field.getName)

      case dateType: org.apache.arrow.vector.types.pojo.ArrowType.Date =>
        import org.apache.arrow.vector.types.DateUnit
        dateType.getUnit match {
          case DateUnit.DAY =>
            Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
              .as(LogicalTypeAnnotation.dateType())
              .named(field.getName)
          case DateUnit.MILLISECOND =>
            Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
              .as(LogicalTypeAnnotation.dateType())
              .named(field.getName)
        }

      case tsType: org.apache.arrow.vector.types.pojo.ArrowType.Timestamp =>
        val unit = tsType.getUnit match {
          case org.apache.arrow.vector.types.TimeUnit.SECOND      =>
            LogicalTypeAnnotation.TimeUnit.MILLIS
          case org.apache.arrow.vector.types.TimeUnit.MILLISECOND =>
            LogicalTypeAnnotation.TimeUnit.MILLIS
          case org.apache.arrow.vector.types.TimeUnit.MICROSECOND =>
            LogicalTypeAnnotation.TimeUnit.MICROS
          case org.apache.arrow.vector.types.TimeUnit.NANOSECOND  =>
            LogicalTypeAnnotation.TimeUnit.NANOS
        }
        val isUtcAdjusted = Option(tsType.getTimezone).exists(_.nonEmpty)
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
          .as(LogicalTypeAnnotation.timestampType(isUtcAdjusted, unit))
          .named(field.getName)

      case _: org.apache.arrow.vector.types.pojo.ArrowType.List =>
        val children = field.getChildren.asScala.toList
        val elementType =
          if (children.nonEmpty) arrowFieldToParquetType(children.head)
          else Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
            .named("element")
        Types.list(repetition)
          .element(elementType)
          .named(field.getName)

      case _: org.apache.arrow.vector.types.pojo.ArrowType.Struct =>
        val children = field.getChildren.asScala.toList.map(arrowFieldToParquetType)
        val builder = Types.buildGroup(repetition)
        children.foreach(builder.addField)
        builder.named(field.getName)

      case _: org.apache.arrow.vector.types.pojo.ArrowType.Map =>
        val entries = field.getChildren.asScala.toList.headOption
        val (keyType, valueType) = entries match {
          case Some(entriesField) =>
            val entryChildren = entriesField.getChildren.asScala.toList
            val kt =
              if (entryChildren.nonEmpty) arrowFieldToParquetType(entryChildren.head)
              else Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                .named("key")
            val vt =
              if (entryChildren.size > 1) arrowFieldToParquetType(entryChildren(1))
              else Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                .named("value")
            (kt, vt)
          case None =>
            val kt = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
              .named("key")
            val vt = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
              .named("value")
            (kt, vt)
        }
        Types.map(repetition)
          .key(keyType)
          .value(valueType)
          .named(field.getName)

      case _: org.apache.arrow.vector.types.pojo.ArrowType.Null =>
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
          .named(field.getName)

      case _ =>
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
          .named(field.getName)
    }
  }

  /** Compute the byte length needed for a decimal with the given precision. */
  private def decimalByteLength(precision: Int): Int = {
    val bitsNeeded = Math.ceil(precision * Math.log(10) / Math.log(2)).toInt + 1
    Math.max((bitsNeeded + 7) / 8, 1)
  }

  // -------------------------------------------------------------------------
  // Metadata helpers
  // -------------------------------------------------------------------------

  private val MetaKeyTable     = "udps.table.name"
  private val MetaKeyNamespace = "udps.namespace"
  private val MetaKeyPartition = "udps.partition"
  private val MetaKeyCreatedAt = "udps.created.at"

  private def buildExtraMetadata(fm: FileMetadata): java.util.Map[String, String] = {
    val meta = new JHashMap[String, String]()
    meta.put(MetaKeyTable, fm.tableName)
    meta.put(MetaKeyNamespace, fm.namespace)
    fm.partitionInfo.foreach(p => meta.put(MetaKeyPartition, p))
    meta.put(MetaKeyCreatedAt, fm.creationTime.toString)
    fm.properties.foreach { case (k, v) => meta.put(k, v) }
    meta
  }

  // -------------------------------------------------------------------------
  // Custom WriteSupport
  // -------------------------------------------------------------------------

  /**
   * Mutable holder for the current row index, shared between the
   * [[Writer]] and the [[ArrowWriteSupport]] so that the writer
   * can control which row within a batch the write support emits.
   */
  private[parquet] final class RowIndexHolder {
    @volatile var index: Int = 0
  }

  private[parquet] final class ArrowWriteSupport(
      parquetSchema: MessageType,
      extraMetadata: java.util.Map[String, String],
      rowIndexHolder: RowIndexHolder
  ) extends WriteSupport[VectorSchemaRoot] {

    private var consumer: RecordConsumer = _

    override def init(configuration: Configuration): WriteContext =
      new WriteContext(parquetSchema, extraMetadata)

    override def prepareForWrite(recordConsumer: RecordConsumer): Unit =
      consumer = recordConsumer

    override def write(record: VectorSchemaRoot): Unit = {
      val rowIdx = rowIndexHolder.index
      consumer.startMessage()

      val fieldVectors = record.getFieldVectors.asScala
      val schemaFields = parquetSchema.getFields.asScala

      var fieldIdx = 0
      while (fieldIdx < fieldVectors.size) {
        val vector = fieldVectors(fieldIdx)
        val parquetField = schemaFields(fieldIdx)

        if (!vector.isNull(rowIdx)) {
          consumer.startField(parquetField.getName, fieldIdx)
          writeValue(vector, rowIdx)
          consumer.endField(parquetField.getName, fieldIdx)
        }
        fieldIdx += 1
      }

      consumer.endMessage()
    }

    // -----------------------------------------------------------------------
    // Vector value writers
    // -----------------------------------------------------------------------

    private def writeValue(vector: FieldVector, rowIdx: Int): Unit =
      vector match {
        case v: BitVector =>
          consumer.addBoolean(v.get(rowIdx) == 1)

        case v: TinyIntVector =>
          consumer.addInteger(v.get(rowIdx).toInt)

        case v: SmallIntVector =>
          consumer.addInteger(v.get(rowIdx).toInt)

        case v: IntVector =>
          consumer.addInteger(v.get(rowIdx))

        case v: BigIntVector =>
          consumer.addLong(v.get(rowIdx))

        case v: UInt1Vector =>
          consumer.addInteger(v.get(rowIdx) & 0xFF)

        case v: UInt2Vector =>
          consumer.addInteger(v.get(rowIdx) & 0xFFFF)

        case v: UInt4Vector =>
          consumer.addInteger(v.get(rowIdx))

        case v: UInt8Vector =>
          consumer.addLong(v.get(rowIdx))

        case v: Float4Vector =>
          consumer.addFloat(v.get(rowIdx))

        case v: Float8Vector =>
          consumer.addDouble(v.get(rowIdx))

        case v: DecimalVector =>
          val arrowBuf = v.get(rowIdx)
          val bytes = new Array[Byte](arrowBuf.capacity().toInt)
          arrowBuf.getBytes(0, bytes)
          consumer.addBinary(Binary.fromConstantByteArray(bytes))

        case v: Decimal256Vector =>
          val arrowBuf = v.get(rowIdx)
          val bytes = new Array[Byte](arrowBuf.capacity().toInt)
          arrowBuf.getBytes(0, bytes)
          consumer.addBinary(Binary.fromConstantByteArray(bytes))

        case v: VarCharVector =>
          val bytes = v.get(rowIdx)
          consumer.addBinary(Binary.fromConstantByteArray(bytes))

        case v: LargeVarCharVector =>
          val bytes = v.get(rowIdx)
          consumer.addBinary(Binary.fromConstantByteArray(bytes))

        case v: VarBinaryVector =>
          val bytes = v.get(rowIdx)
          consumer.addBinary(Binary.fromConstantByteArray(bytes))

        case v: LargeVarBinaryVector =>
          val bytes = v.get(rowIdx)
          consumer.addBinary(Binary.fromConstantByteArray(bytes))

        case v: FixedSizeBinaryVector =>
          val bytes = v.get(rowIdx)
          consumer.addBinary(Binary.fromConstantByteArray(bytes))

        case v: DateDayVector =>
          consumer.addInteger(v.get(rowIdx))

        case v: DateMilliVector =>
          consumer.addLong(v.get(rowIdx))

        case v: TimeStampSecVector =>
          consumer.addLong(v.get(rowIdx) * 1000L)

        case v: TimeStampMilliVector =>
          consumer.addLong(v.get(rowIdx))

        case v: TimeStampMicroVector =>
          consumer.addLong(v.get(rowIdx))

        case v: TimeStampNanoVector =>
          consumer.addLong(v.get(rowIdx))

        case v: TimeStampSecTZVector =>
          consumer.addLong(v.get(rowIdx) * 1000L)

        case v: TimeStampMilliTZVector =>
          consumer.addLong(v.get(rowIdx))

        case v: TimeStampMicroTZVector =>
          consumer.addLong(v.get(rowIdx))

        case v: TimeStampNanoTZVector =>
          consumer.addLong(v.get(rowIdx))

        case v: MapVector =>
          writeMapValue(v, rowIdx)

        case v: ListVector =>
          writeListValue(v, rowIdx)

        case v: StructVector =>
          writeStructValue(v, rowIdx)

        case _ =>
          logger.warn(
            "Unsupported Arrow vector type {}, writing empty binary",
            vector.getClass.getSimpleName
          )
          consumer.addBinary(Binary.EMPTY)
      }

    private def writeListValue(vector: ListVector, rowIdx: Int): Unit = {
      val start = vector.getOffsetBuffer.getInt(rowIdx.toLong * BaseRepeatedValueVector.OFFSET_WIDTH)
      val end   = vector.getOffsetBuffer.getInt((rowIdx.toLong + 1L) * BaseRepeatedValueVector.OFFSET_WIDTH)
      val dataVector = vector.getDataVector

      consumer.startGroup()
      if (start < end) {
        consumer.startField("list", 0)
        var i = start
        while (i < end) {
          consumer.startGroup()
          if (!dataVector.isNull(i)) {
            consumer.startField("element", 0)
            writeValue(dataVector, i)
            consumer.endField("element", 0)
          }
          consumer.endGroup()
          i += 1
        }
        consumer.endField("list", 0)
      }
      consumer.endGroup()
    }

    private def writeStructValue(vector: StructVector, rowIdx: Int): Unit = {
      consumer.startGroup()
      var fieldIdx = 0
      val childCount = vector.size()
      while (fieldIdx < childCount) {
        val child = vector.getVectorById(fieldIdx).asInstanceOf[FieldVector]
        if (!child.isNull(rowIdx)) {
          consumer.startField(child.getName, fieldIdx)
          writeValue(child, rowIdx)
          consumer.endField(child.getName, fieldIdx)
        }
        fieldIdx += 1
      }
      consumer.endGroup()
    }

    private def writeMapValue(vector: MapVector, rowIdx: Int): Unit = {
      val dataVector = vector.getDataVector.asInstanceOf[StructVector]
      val start = vector.getOffsetBuffer.getInt(rowIdx.toLong * BaseRepeatedValueVector.OFFSET_WIDTH)
      val end   = vector.getOffsetBuffer.getInt((rowIdx.toLong + 1L) * BaseRepeatedValueVector.OFFSET_WIDTH)

      consumer.startGroup()
      if (start < end) {
        consumer.startField("key_value", 0)
        var i = start
        while (i < end) {
          consumer.startGroup()
          val keyVector = dataVector.getVectorById(0).asInstanceOf[FieldVector]
          if (!keyVector.isNull(i)) {
            consumer.startField("key", 0)
            writeValue(keyVector, i)
            consumer.endField("key", 0)
          }
          val valueVector = dataVector.getVectorById(1).asInstanceOf[FieldVector]
          if (!valueVector.isNull(i)) {
            consumer.startField("value", 1)
            writeValue(valueVector, i)
            consumer.endField("value", 1)
          }
          consumer.endGroup()
          i += 1
        }
        consumer.endField("key_value", 0)
      }
      consumer.endGroup()
    }
  }

  // -------------------------------------------------------------------------
  // ParquetWriter.Builder implementation
  // -------------------------------------------------------------------------

  /**
   * Builder that wires [[ArrowWriteSupport]] into the Hadoop
   * [[HadoopParquetWriter]] builder framework.
   */
  private final class ArrowParquetWriterBuilder(
      writeSupport: ArrowWriteSupport,
      path: Path
  ) extends HadoopParquetWriter.Builder[VectorSchemaRoot, ArrowParquetWriterBuilder](path) {

    override protected def self(): ArrowParquetWriterBuilder = this

    override protected def getWriteSupport(
        conf: Configuration
    ): WriteSupport[VectorSchemaRoot] = writeSupport
  }
}
