package io.gbmm.udps.storage.parquet

import cats.effect.{IO, Resource}
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.storage.arrow.SchemaAdapter
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.{Field, Schema => ArrowSchema}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{ParquetMetadata => HadoopParquetMetadata}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.{ColumnIOFactory, MessageColumnIO, RecordReader}
import org.apache.parquet.schema.{MessageType, Type}

import java.util.{ArrayList => JArrayList}
import scala.jdk.CollectionConverters._

/**
 * Metadata extracted from a Parquet file footer.
 *
 * @param schema         the Parquet message schema
 * @param rowCount       total number of rows across all row groups
 * @param rowGroupCount  number of row groups in the file
 * @param createdBy      the "created by" string from the footer, if present
 * @param keyValueMeta   key-value metadata pairs stored in the file footer
 */
final case class ParquetFileMetadata(
  schema: MessageType,
  rowCount: Long,
  rowGroupCount: Int,
  createdBy: Option[String],
  keyValueMeta: Map[String, String]
)

/**
 * Result of reading a Parquet file into Arrow format.
 *
 * @param root     the populated [[VectorSchemaRoot]] — caller is responsible
 *                 for closing via the [[Resource]] wrapper returned by
 *                 [[ParquetReader.read]].
 * @param metadata file-level metadata extracted from the Parquet footer.
 */
final case class ParquetReadResult(
  root: VectorSchemaRoot,
  metadata: ParquetFileMetadata
)

/**
 * Reads Parquet files into Apache Arrow [[VectorSchemaRoot]] with support for:
 *
 *  - Column projection (read only requested columns)
 *  - Predicate pushdown (row-group filtering via [[PredicatePushdown]])
 *  - Schema evolution (gracefully handle added/removed columns)
 *  - Metadata extraction from the file footer
 *  - cats-effect [[Resource]] management for allocators and readers
 */
object ParquetReader extends LazyLogging {

  private val DefaultAllocatorLimit: Long = Long.MaxValue

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Read a Parquet file and return Arrow vectors wrapped in a cats-effect
   * [[Resource]] that guarantees cleanup of both the [[VectorSchemaRoot]] and
   * the Arrow [[BufferAllocator]].
   *
   * @param path            Hadoop-compatible file path
   * @param columns         optional column projection; when empty all columns
   *                        are read
   * @param predicate       optional predicate for row-group pushdown filtering
   * @param hadoopConf      Hadoop [[Configuration]]; a default is used when
   *                        None
   * @param allocatorLimit  maximum bytes the Arrow allocator may use
   * @return a [[Resource]] wrapping the [[ParquetReadResult]]
   */
  def read(
    path: String,
    columns: Seq[String] = Seq.empty,
    predicate: Option[Predicate] = None,
    hadoopConf: Option[Configuration] = None,
    allocatorLimit: Long = DefaultAllocatorLimit
  ): Resource[IO, ParquetReadResult] =
    for {
      allocator <- makeAllocator(allocatorLimit)
      result    <- readInternal(path, columns, predicate, hadoopConf, allocator)
    } yield result

  /**
   * Read only the file-level metadata without materialising row data.
   */
  def readMetadata(
    path: String,
    hadoopConf: Option[Configuration] = None
  ): IO[ParquetFileMetadata] =
    openReader(path, hadoopConf, filter = None).use { reader =>
      IO.blocking {
        extractMetadata(reader)
      }
    }

  /**
   * Read the Parquet file schema and convert it to the UDPS domain
   * [[io.gbmm.udps.core.domain.TableMetadata]] via [[SchemaAdapter]].
   */
  def readTableMetadata(
    path: String,
    hadoopConf: Option[Configuration] = None
  ): IO[io.gbmm.udps.core.domain.TableMetadata] =
    readMetadata(path, hadoopConf).flatMap { meta =>
      readArrowSchema(path, Seq.empty, hadoopConf).map { arrowSchema =>
        val enrichedMeta = new java.util.HashMap[String, String]()
        meta.keyValueMeta.foreach { case (k, v) => enrichedMeta.put(k, v) }
        val enrichedSchema = new ArrowSchema(arrowSchema.getFields, enrichedMeta)
        SchemaAdapter.fromArrowSchema(enrichedSchema)
      }
    }

  // ---------------------------------------------------------------------------
  // Internal: Resource helpers
  // ---------------------------------------------------------------------------

  private def makeAllocator(limit: Long): Resource[IO, BufferAllocator] =
    Resource.make(
      IO.blocking(new RootAllocator(limit): BufferAllocator)
    )(alloc => IO.blocking(alloc.close()))

  private def openReader(
    path: String,
    hadoopConf: Option[Configuration],
    filter: Option[FilterCompat.Filter]
  ): Resource[IO, ParquetFileReader] =
    Resource.make(
      IO.blocking {
        val conf = hadoopConf.getOrElse(new Configuration())
        val inputFile = HadoopInputFile.fromPath(new HadoopPath(path), conf)
        val options = {
          val builder = org.apache.parquet.ParquetReadOptions.builder()
          filter.foreach(f => builder.withRecordFilter(f))
          builder.build()
        }
        ParquetFileReader.open(inputFile, options)
      }
    )(reader => IO.blocking(reader.close()))

  // ---------------------------------------------------------------------------
  // Internal: Core read pipeline
  // ---------------------------------------------------------------------------

  private def readInternal(
    path: String,
    columns: Seq[String],
    predicate: Option[Predicate],
    hadoopConf: Option[Configuration],
    allocator: BufferAllocator
  ): Resource[IO, ParquetReadResult] = {
    val filter = predicate.map(PredicatePushdown.toFilter)

    openReader(path, hadoopConf, filter).evalMap { reader =>
      IO.blocking {
        val fileMetadata = extractMetadata(reader)
        val fileSchema = reader.getFooter.getFileMetaData.getSchema

        val readSchema = projectSchema(fileSchema, columns)
        val arrowSchema = parquetSchemaToArrowSchema(readSchema)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)

        try {
          populateVectors(reader, readSchema, fileSchema, root)
          ParquetReadResult(root, fileMetadata)
        } catch {
          case ex: Throwable =>
            root.close()
            throw ex
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Schema projection
  // ---------------------------------------------------------------------------

  /**
   * Build a projected Parquet [[MessageType]] containing only the requested
   * columns.  When the column list is empty the full schema is returned.
   * Columns not present in the file schema are silently dropped (schema
   * evolution: removed columns).
   */
  private def projectSchema(
    fileSchema: MessageType,
    columns: Seq[String]
  ): MessageType = {
    if (columns.isEmpty) return fileSchema

    val fileFieldsByName: Map[String, Type] = fileSchema.getFields.asScala.map(f => f.getName -> f).toMap
    val projectedFields: Seq[Type] = columns.flatMap { col =>
      fileFieldsByName.get(col) match {
        case some @ Some(_) => some
        case None =>
          logger.debug("Column '{}' requested for projection not found in file schema (schema evolution)", col)
          None
      }
    }

    if (projectedFields.isEmpty) {
      logger.warn("No requested columns found in file schema; returning full schema")
      fileSchema
    } else {
      new MessageType(fileSchema.getName, projectedFields.asJava)
    }
  }

  // ---------------------------------------------------------------------------
  // Parquet -> Arrow schema conversion (structural, not via SchemaAdapter)
  // ---------------------------------------------------------------------------

  private def parquetSchemaToArrowSchema(messageType: MessageType): ArrowSchema = {
    val fields = new JArrayList[Field]()
    messageType.getFields.asScala.foreach { parquetField =>
      fields.add(parquetFieldToArrowField(parquetField))
    }
    new ArrowSchema(fields)
  }

  private def parquetFieldToArrowField(parquetField: Type): Field = {
    import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}
    import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
    import org.apache.parquet.schema.LogicalTypeAnnotation

    val nullable = parquetField.getRepetition != Type.Repetition.REQUIRED

    if (parquetField.isPrimitive) {
      val primitiveType = parquetField.asPrimitiveType()
      val logicalType = primitiveType.getLogicalTypeAnnotation
      val arrowType: ArrowType = primitiveType.getPrimitiveTypeName match {
        case PrimitiveTypeName.BOOLEAN => ArrowType.Bool.INSTANCE

        case PrimitiveTypeName.INT32 =>
          logicalType match {
            case d: LogicalTypeAnnotation.DateLogicalTypeAnnotation =>
              new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)
            case i: LogicalTypeAnnotation.IntLogicalTypeAnnotation =>
              new ArrowType.Int(i.getBitWidth, i.isSigned)
            case d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation =>
              new ArrowType.Decimal(d.getPrecision, d.getScale, 128)
            case _ =>
              new ArrowType.Int(32, true)
          }

        case PrimitiveTypeName.INT64 =>
          logicalType match {
            case ts: LogicalTypeAnnotation.TimestampLogicalTypeAnnotation =>
              val unit = ts.getUnit match {
                case LogicalTypeAnnotation.TimeUnit.MILLIS  => org.apache.arrow.vector.types.TimeUnit.MILLISECOND
                case LogicalTypeAnnotation.TimeUnit.MICROS  => org.apache.arrow.vector.types.TimeUnit.MICROSECOND
                case LogicalTypeAnnotation.TimeUnit.NANOS   => org.apache.arrow.vector.types.TimeUnit.NANOSECOND
              }
              new ArrowType.Timestamp(unit, null)
            case i: LogicalTypeAnnotation.IntLogicalTypeAnnotation =>
              new ArrowType.Int(i.getBitWidth, i.isSigned)
            case d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation =>
              new ArrowType.Decimal(d.getPrecision, d.getScale, 128)
            case _ =>
              new ArrowType.Int(64, true)
          }

        case PrimitiveTypeName.INT96 =>
          // INT96 is legacy timestamp in nanoseconds
          new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, null)

        case PrimitiveTypeName.FLOAT =>
          new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)

        case PrimitiveTypeName.DOUBLE =>
          new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)

        case PrimitiveTypeName.BINARY =>
          logicalType match {
            case _: LogicalTypeAnnotation.StringLogicalTypeAnnotation =>
              ArrowType.Utf8.INSTANCE
            case _: LogicalTypeAnnotation.EnumLogicalTypeAnnotation =>
              ArrowType.Utf8.INSTANCE
            case _: LogicalTypeAnnotation.JsonLogicalTypeAnnotation =>
              ArrowType.Utf8.INSTANCE
            case d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation =>
              new ArrowType.Decimal(d.getPrecision, d.getScale, 128)
            case _ =>
              ArrowType.Binary.INSTANCE
          }

        case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
          logicalType match {
            case d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation =>
              new ArrowType.Decimal(d.getPrecision, d.getScale, 128)
            case _ =>
              ArrowType.Binary.INSTANCE
          }
      }

      new Field(
        parquetField.getName,
        new FieldType(nullable, arrowType, null),
        new JArrayList[Field]()
      )
    } else {
      // Group type — map to Struct
      val groupType = parquetField.asGroupType()
      val children = new JArrayList[Field]()
      groupType.getFields.asScala.foreach { child =>
        children.add(parquetFieldToArrowField(child))
      }
      new Field(
        parquetField.getName,
        new FieldType(nullable, ArrowType.Struct.INSTANCE, null),
        children
      )
    }
  }

  // ---------------------------------------------------------------------------
  // Vector population
  // ---------------------------------------------------------------------------

  /**
   * Iterate through all row groups, read records via the Parquet column IO
   * API, and populate the Arrow vectors in [[VectorSchemaRoot]].
   *
   * For schema evolution (added columns): if the read schema has fewer fields
   * than the Arrow schema, the extra Arrow vectors remain with their default
   * null values.
   */
  private def populateVectors(
    reader: ParquetFileReader,
    readSchema: MessageType,
    fileSchema: MessageType,
    root: VectorSchemaRoot
  ): Unit = {
    root.setRowCount(0)
    root.allocateNew()

    val columnIO: MessageColumnIO = new ColumnIOFactory().getColumnIO(readSchema, fileSchema)
    var globalRow: Int = 0

    var pages: PageReadStore = reader.readNextRowGroup()
    while (pages != null) {
      val rowCountInGroup: Long = pages.getRowCount
      val recordReader: RecordReader[Group] = columnIO.getRecordReader(
        pages,
        new GroupRecordConverter(readSchema)
      )

      var i: Long = 0L
      while (i < rowCountInGroup) {
        val group: Group = recordReader.read()
        writeGroupToVectors(group, readSchema, root, globalRow)
        globalRow += 1
        i += 1
      }

      pages = reader.readNextRowGroup()
    }

    root.setRowCount(globalRow)
  }

  /**
   * Write a single Parquet [[Group]] record into the Arrow vectors at the
   * given row index.
   */
  private def writeGroupToVectors(
    group: Group,
    schema: MessageType,
    root: VectorSchemaRoot,
    rowIndex: Int
  ): Unit = {
    val fieldCount = schema.getFieldCount
    var fieldIdx = 0
    while (fieldIdx < fieldCount) {
      val fieldName = schema.getFieldName(fieldIdx)
      val vector = root.getVector(fieldName)
      if (vector != null) {
        val repetitionCount = group.getFieldRepetitionCount(fieldIdx)
        if (repetitionCount == 0) {
          setNull(vector, rowIndex)
        } else {
          writeValue(group, fieldIdx, vector, rowIndex)
        }
      }
      fieldIdx += 1
    }
  }

  /**
   * Write a single field value from a Parquet [[Group]] into the corresponding
   * Arrow vector.
   */
  private def writeValue(
    group: Group,
    fieldIdx: Int,
    vector: FieldVector,
    rowIndex: Int
  ): Unit = {
    vector match {
      case v: BitVector =>
        v.setSafe(rowIndex, if (group.getBoolean(fieldIdx, 0)) 1 else 0)

      case v: IntVector =>
        v.setSafe(rowIndex, group.getInteger(fieldIdx, 0))

      case v: SmallIntVector =>
        v.setSafe(rowIndex, group.getInteger(fieldIdx, 0).toShort)

      case v: TinyIntVector =>
        v.setSafe(rowIndex, group.getInteger(fieldIdx, 0).toByte)

      case v: BigIntVector =>
        v.setSafe(rowIndex, group.getLong(fieldIdx, 0))

      case v: Float4Vector =>
        v.setSafe(rowIndex, group.getFloat(fieldIdx, 0))

      case v: Float8Vector =>
        v.setSafe(rowIndex, group.getDouble(fieldIdx, 0))

      case v: VarCharVector =>
        val bytes = group.getString(fieldIdx, 0).getBytes(java.nio.charset.StandardCharsets.UTF_8)
        v.setSafe(rowIndex, bytes, 0, bytes.length)

      case v: VarBinaryVector =>
        val binary = group.getBinary(fieldIdx, 0)
        v.setSafe(rowIndex, binary.getBytes, 0, binary.length())

      case v: DateDayVector =>
        v.setSafe(rowIndex, group.getInteger(fieldIdx, 0))

      case v: DateMilliVector =>
        v.setSafe(rowIndex, group.getLong(fieldIdx, 0))

      case v: TimeStampMilliVector =>
        v.setSafe(rowIndex, group.getLong(fieldIdx, 0))

      case v: TimeStampMicroVector =>
        v.setSafe(rowIndex, group.getLong(fieldIdx, 0))

      case v: TimeStampNanoVector =>
        v.setSafe(rowIndex, group.getLong(fieldIdx, 0))

      case v: TimeStampSecVector =>
        v.setSafe(rowIndex, group.getLong(fieldIdx, 0))

      case v: DecimalVector =>
        val binary = group.getBinary(fieldIdx, 0)
        val unscaled = new java.math.BigInteger(binary.getBytes)
        val decimal = new java.math.BigDecimal(unscaled, v.getScale)
        v.setSafe(rowIndex, decimal)

      case _ =>
        logger.warn(
          "Unsupported Arrow vector type '{}' for field at index {}; setting null",
          vector.getClass.getSimpleName,
          fieldIdx.toString
        )
        setNull(vector, rowIndex)
    }
  }

  /**
   * Set a null value on any Arrow [[FieldVector]] at the given row index.
   */
  private def setNull(vector: FieldVector, rowIndex: Int): Unit = {
    vector match {
      case v: BitVector            => v.setNull(rowIndex)
      case v: IntVector            => v.setNull(rowIndex)
      case v: SmallIntVector       => v.setNull(rowIndex)
      case v: TinyIntVector        => v.setNull(rowIndex)
      case v: BigIntVector         => v.setNull(rowIndex)
      case v: Float4Vector         => v.setNull(rowIndex)
      case v: Float8Vector         => v.setNull(rowIndex)
      case v: VarCharVector        => v.setNull(rowIndex)
      case v: VarBinaryVector      => v.setNull(rowIndex)
      case v: DateDayVector        => v.setNull(rowIndex)
      case v: DateMilliVector      => v.setNull(rowIndex)
      case v: TimeStampMilliVector => v.setNull(rowIndex)
      case v: TimeStampMicroVector => v.setNull(rowIndex)
      case v: TimeStampNanoVector  => v.setNull(rowIndex)
      case v: TimeStampSecVector   => v.setNull(rowIndex)
      case v: DecimalVector        => v.setNull(rowIndex)
      case _ =>
        logger.debug(
          "Cannot set null on vector type '{}'; skipping",
          vector.getClass.getSimpleName
        )
    }
  }

  // ---------------------------------------------------------------------------
  // Arrow schema reading (for domain metadata extraction)
  // ---------------------------------------------------------------------------

  private def readArrowSchema(
    path: String,
    columns: Seq[String],
    hadoopConf: Option[Configuration]
  ): IO[ArrowSchema] =
    openReader(path, hadoopConf, filter = None).use { reader =>
      IO.blocking {
        val fileSchema = reader.getFooter.getFileMetaData.getSchema
        val projected = projectSchema(fileSchema, columns)
        parquetSchemaToArrowSchema(projected)
      }
    }

  // ---------------------------------------------------------------------------
  // Metadata extraction
  // ---------------------------------------------------------------------------

  private def extractMetadata(reader: ParquetFileReader): ParquetFileMetadata = {
    val footer: HadoopParquetMetadata = reader.getFooter
    val fileMeta = footer.getFileMetaData
    val rowGroups = footer.getBlocks.asScala.toSeq
    val totalRows = rowGroups.map(_.getRowCount).sum

    val kvMeta: Map[String, String] = Option(fileMeta.getKeyValueMetaData)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty)

    ParquetFileMetadata(
      schema = fileMeta.getSchema,
      rowCount = totalRows,
      rowGroupCount = rowGroups.size,
      createdBy = Option(fileMeta.getCreatedBy),
      keyValueMeta = kvMeta
    )
  }
}
