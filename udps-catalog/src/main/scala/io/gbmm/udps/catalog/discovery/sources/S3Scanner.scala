package io.gbmm.udps.catalog.discovery.sources

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.catalog.discovery._
import io.gbmm.udps.core.domain.DataType
import io.minio.{ListObjectsArgs, MinioClient}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

final class S3Scanner extends DataSourceScanner with LazyLogging {

  override val sourceType: String = "s3"

  override def scan(config: ScanConfig): IO[DiscoveryResult] =
    IO.blocking {
      val bucket = config.options.getOrElse("bucket", "")
      val prefix = config.options.getOrElse("prefix", "")
      val accessKey = config.options.getOrElse("accessKey", "")
      val secretKey = config.options.getOrElse("secretKey", "")

      if (bucket.isEmpty) {
        DiscoveryResult(tables = Seq.empty, errors = Seq("S3 scan requires 'bucket' option"))
      } else {
        val client = buildMinioClient(config.connectionString, accessKey, secretKey)
        discoverFromBucket(client, bucket, prefix, config)
      }
    }

  private def buildMinioClient(endpoint: String, accessKey: String, secretKey: String): MinioClient = {
    val builder = MinioClient.builder().endpoint(endpoint)
    if (accessKey.nonEmpty && secretKey.nonEmpty) {
      builder.credentials(accessKey, secretKey)
    }
    builder.build()
  }

  private def discoverFromBucket(
    client: MinioClient,
    bucket: String,
    prefix: String,
    config: ScanConfig
  ): DiscoveryResult = {
    val listArgs = {
      val builder = ListObjectsArgs.builder().bucket(bucket).recursive(true)
      if (prefix.nonEmpty) builder.prefix(prefix)
      builder.build()
    }

    val objectsByTable = mutable.Map.empty[String, mutable.ListBuffer[String]]
    val errors = mutable.ListBuffer.empty[String]

    val results = client.listObjects(listArgs)
    results.forEach { item =>
      try {
        val obj = item.get()
        val objectName = obj.objectName()
        if (isParquetFile(objectName)) {
          val tableName = extractTableName(objectName, prefix)
          objectsByTable.getOrElseUpdate(tableName, mutable.ListBuffer.empty) += objectName
        }
      } catch {
        case ex: Exception =>
          errors += s"Error listing object: ${ex.getMessage}"
      }
    }

    val tables = objectsByTable.toSeq.flatMap { case (tableName, objects) =>
      val firstObject = objects.head
      readParquetSchema(config.connectionString, bucket, firstObject, config) match {
        case Right(columns) =>
          Seq(DiscoveredTable(
            name = tableName,
            columns = columns,
            rowCount = None,
            primaryKey = None
          ))
        case Left(error) =>
          errors += s"Error reading Parquet schema for $tableName: $error"
          Seq.empty
      }
    }

    DiscoveryResult(tables = tables, errors = errors.toSeq)
  }

  private def readParquetSchema(
    endpoint: String,
    bucket: String,
    objectKey: String,
    config: ScanConfig
  ): Either[String, Seq[DiscoveredColumn]] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.s3a.endpoint", endpoint)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    val accessKey = config.options.getOrElse("accessKey", "")
    val secretKey = config.options.getOrElse("secretKey", "")
    if (accessKey.nonEmpty) hadoopConf.set("fs.s3a.access.key", accessKey)
    if (secretKey.nonEmpty) hadoopConf.set("fs.s3a.secret.key", secretKey)

    val path = new Path(s"s3a://$bucket/$objectKey")
    val readerResource = scala.util.Using(
      ParquetFileReader.open(HadoopInputFile.fromPath(path, hadoopConf))
    ) { reader =>
      val schema = reader.getFooter.getFileMetaData.getSchema
      val fields = schema.getFields.asScala.toSeq
      fields.zipWithIndex.map { case (field, idx) =>
        val repetition = field.getRepetition
        val nullable = repetition != org.apache.parquet.schema.Type.Repetition.REQUIRED
        DiscoveredColumn(
          name = field.getName,
          dataType = mapParquetType(field),
          nullable = nullable,
          ordinalPosition = idx + 1
        )
      }
    }
    readerResource.toEither.left.map(_.getMessage)
  }

  private def mapParquetType(field: org.apache.parquet.schema.Type): DataType = {
    if (field.isPrimitive) {
      val primitive = field.asPrimitiveType()
      val logicalType = Option(primitive.getLogicalTypeAnnotation)
      mapPrimitiveType(primitive, logicalType)
    } else {
      val groupType = field.asGroupType()
      val childFields = groupType.getFields.asScala.toSeq.map { f =>
        import io.gbmm.udps.core.domain.ColumnMetadata
        ColumnMetadata(
          name = f.getName,
          dataType = mapParquetType(f),
          nullable = f.getRepetition != org.apache.parquet.schema.Type.Repetition.REQUIRED,
          description = None,
          tags = Map.empty,
          statistics = None
        )
      }
      DataType.Struct(childFields)
    }
  }

  private def mapPrimitiveType(
    primitive: PrimitiveType,
    logicalType: Option[LogicalTypeAnnotation]
  ): DataType = {
    import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
    logicalType match {
      case Some(_: LogicalTypeAnnotation.StringLogicalTypeAnnotation) =>
        DataType.Utf8
      case Some(_: LogicalTypeAnnotation.DateLogicalTypeAnnotation) =>
        DataType.Date32
      case Some(lt: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) =>
        DataType.Decimal(precision = lt.getPrecision, scale = lt.getScale)
      case Some(lt: LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) =>
        lt.getUnit match {
          case LogicalTypeAnnotation.TimeUnit.MILLIS  => DataType.TimestampMillis
          case LogicalTypeAnnotation.TimeUnit.MICROS  => DataType.TimestampMicros
          case LogicalTypeAnnotation.TimeUnit.NANOS   => DataType.TimestampNanos
        }
      case Some(lt: LogicalTypeAnnotation.IntLogicalTypeAnnotation) =>
        lt.getBitWidth match {
          case 8  => if (lt.isSigned) DataType.Int8 else DataType.UInt8
          case 16 => if (lt.isSigned) DataType.Int16 else DataType.UInt16
          case 32 => if (lt.isSigned) DataType.Int32 else DataType.UInt32
          case 64 => if (lt.isSigned) DataType.Int64 else DataType.UInt64
          case _  => DataType.Int64
        }
      case _ =>
        primitive.getPrimitiveTypeName match {
          case BOOLEAN              => DataType.Boolean
          case INT32                => DataType.Int32
          case INT64                => DataType.Int64
          case INT96                => DataType.TimestampNanos
          case FLOAT                => DataType.Float32
          case DOUBLE               => DataType.Float64
          case BINARY | FIXED_LEN_BYTE_ARRAY => DataType.Binary
        }
    }
  }

  private def isParquetFile(objectName: String): Boolean =
    objectName.endsWith(".parquet") || objectName.endsWith(".parq")

  private def extractTableName(objectName: String, prefix: String): String = {
    val withoutPrefix = if (prefix.nonEmpty && objectName.startsWith(prefix)) {
      objectName.drop(prefix.length).dropWhile(_ == '/')
    } else {
      objectName
    }
    val parts = withoutPrefix.split('/')
    if (parts.length > 1) parts.dropRight(1).mkString(".")
    else parts.head.replaceAll("\\.(parquet|parq)$", "")
  }
}
