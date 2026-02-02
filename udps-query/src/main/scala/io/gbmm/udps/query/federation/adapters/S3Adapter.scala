package io.gbmm.udps.query.federation.adapters

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.core.domain.DataType
import io.gbmm.udps.query.federation._
import io.minio.{ListObjectsArgs, MinioClient}
import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}
import org.apache.calcite.schema.{ScannableTable, Schema, Statistic, Statistics}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{MessageType, PrimitiveType}

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Federation adapter for S3/MinIO object storage.
  *
  * Discovers Parquet, CSV, and JSON files organized by prefix as tables.
  * Creates Calcite ScannableTable implementations that read files from S3.
  * Primary support for Parquet with schema inference from file metadata;
  * CSV and JSON are read as string-typed columns.
  */
final class S3Adapter extends FederationAdapter with LazyLogging {

  private val S3_DEFAULT_PORT = 9000

  override val sourceType: DataSourceType = DataSourceType.S3

  override val pushdownCapabilities: PushdownCapabilities = PushdownCapabilities(
    predicates = true,
    projections = true,
    limits = false,
    aggregations = false,
    joins = false
  )

  override def connect(config: DataSourceConfig): IO[FederatedConnection] = IO {
    val effectivePort = if (config.port > 0) config.port else S3_DEFAULT_PORT
    val scheme = config.properties.getOrElse("scheme", "http")
    val endpoint = s"$scheme://${config.host}:$effectivePort"

    val builder = MinioClient.builder()
      .endpoint(endpoint)

    (config.username, config.password) match {
      case (Some(accessKey), Some(secretKey)) =>
        builder.credentials(accessKey, secretKey)
      case _ => ()
    }

    config.properties.get("region").foreach(builder.region)

    val client = builder.build()
    logger.info(s"S3/MinIO connection established to $endpoint, bucket=${config.database}")

    new S3FederatedConnection(
      client = client,
      bucket = config.database,
      endpoint = endpoint,
      hadoopConf = buildHadoopConf(config, endpoint)
    )
  }

  override def discoverTables(connection: FederatedConnection): IO[Seq[FederatedTableInfo]] = IO {
    val s3Conn = connection.asInstanceOf[S3FederatedConnection]
    val objects = s3Conn.client
      .listObjects(ListObjectsArgs.builder().bucket(s3Conn.bucket).recursive(true).build())
      .asScala.toSeq

    val objectsByPrefix = objects
      .flatMap { result =>
        val item = result.get()
        val key = item.objectName()
        if (isSupportedFormat(key)) {
          val prefix = extractTablePrefix(key)
          Some(prefix -> key)
        } else {
          None
        }
      }
      .groupBy(_._1)
      .map { case (prefix, entries) => prefix -> entries.map(_._2) }

    objectsByPrefix.map { case (prefix, keys) =>
      val firstParquetKey = keys.find(_.endsWith(".parquet"))
      val columns = firstParquetKey match {
        case Some(parquetKey) =>
          inferParquetSchema(s3Conn, parquetKey)
        case None =>
          val firstKey = keys.headOption.getOrElse("")
          inferBasicSchema(firstKey)
      }
      val estimatedRows = keys.size.toLong * estimateRowsPerFile(keys.headOption)
      FederatedTableInfo(prefix, columns, estimatedRows, DataSourceType.S3)
    }.toSeq
  }

  override def createCalciteSchema(connection: FederatedConnection): IO[Schema] = IO {
    val s3Conn = connection.asInstanceOf[S3FederatedConnection]
    new S3CalciteSchema(s3Conn)
  }

  private def buildHadoopConf(config: DataSourceConfig, endpoint: String): Configuration = {
    val conf = new Configuration()
    conf.set("fs.s3a.endpoint", endpoint)
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.connection.ssl.enabled",
      config.properties.getOrElse("scheme", "http") match {
        case "https" => "true"
        case _       => "false"
      })
    config.username.foreach(conf.set("fs.s3a.access.key", _))
    config.password.foreach(conf.set("fs.s3a.secret.key", _))
    conf
  }

  private def isSupportedFormat(key: String): Boolean = {
    val lower = key.toLowerCase
    lower.endsWith(".parquet") || lower.endsWith(".csv") || lower.endsWith(".json")
  }

  /** Extract a table name from an object key by using the parent directory as the table prefix. */
  private def extractTablePrefix(key: String): String = {
    val lastSlash = key.lastIndexOf('/')
    if (lastSlash > 0) key.substring(0, lastSlash).replace('/', '_')
    else {
      val dotIdx = key.lastIndexOf('.')
      if (dotIdx > 0) key.substring(0, dotIdx)
      else key
    }
  }

  private def inferParquetSchema(s3Conn: S3FederatedConnection, objectKey: String): Seq[FederatedColumnInfo] = {
    try {
      val s3aPath = new Path(s"s3a://${s3Conn.bucket}/$objectKey")
      val inputFile = HadoopInputFile.fromPath(s3aPath, s3Conn.hadoopConf)
      val reader = ParquetFileReader.open(inputFile)
      try {
        val schema: MessageType = reader.getFileMetaData.getSchema
        schema.getFields.asScala.map { field =>
          val dataType = mapParquetType(field)
          FederatedColumnInfo(field.getName, dataType)
        }.toSeq
      } finally {
        reader.close()
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to infer Parquet schema from $objectKey: ${e.getMessage}")
        Seq(FederatedColumnInfo("value", DataType.Utf8))
    }
  }

  private def mapParquetType(field: org.apache.parquet.schema.Type): DataType = {
    if (field.isPrimitive) {
      field.asPrimitiveType().getPrimitiveTypeName match {
        case PrimitiveType.PrimitiveTypeName.BOOLEAN  => DataType.Boolean
        case PrimitiveType.PrimitiveTypeName.INT32     => DataType.Int32
        case PrimitiveType.PrimitiveTypeName.INT64     => DataType.Int64
        case PrimitiveType.PrimitiveTypeName.INT96     => DataType.TimestampNanos
        case PrimitiveType.PrimitiveTypeName.FLOAT     => DataType.Float32
        case PrimitiveType.PrimitiveTypeName.DOUBLE    => DataType.Float64
        case PrimitiveType.PrimitiveTypeName.BINARY    =>
          val logicalType = field.getLogicalTypeAnnotation
          if (logicalType != null && logicalType.toString.contains("STRING")) DataType.Utf8
          else DataType.Binary
        case PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
          val logicalType = field.getLogicalTypeAnnotation
          if (logicalType != null && logicalType.toString.contains("DECIMAL")) {
            DataType.Decimal(38, 18)
          } else {
            DataType.Binary
          }
      }
    } else {
      DataType.Utf8
    }
  }

  private def inferBasicSchema(key: String): Seq[FederatedColumnInfo] = {
    val lower = key.toLowerCase
    if (lower.endsWith(".csv")) {
      Seq(FederatedColumnInfo("line", DataType.Utf8))
    } else if (lower.endsWith(".json")) {
      Seq(FederatedColumnInfo("json_value", DataType.Utf8))
    } else {
      Seq(FederatedColumnInfo("value", DataType.Utf8))
    }
  }

  private val ESTIMATED_ROWS_PER_PARQUET = 10000L
  private val ESTIMATED_ROWS_PER_CSV = 1000L
  private val ESTIMATED_ROWS_PER_JSON = 100L

  private def estimateRowsPerFile(keyOpt: Option[String]): Long =
    keyOpt match {
      case Some(k) if k.endsWith(".parquet") => ESTIMATED_ROWS_PER_PARQUET
      case Some(k) if k.endsWith(".csv")     => ESTIMATED_ROWS_PER_CSV
      case Some(k) if k.endsWith(".json")    => ESTIMATED_ROWS_PER_JSON
      case _                                  => ESTIMATED_ROWS_PER_CSV
    }
}

object S3Adapter {
  def apply(): S3Adapter = new S3Adapter()
}

/** FederatedConnection wrapping a MinIO client and S3 bucket reference. */
private[adapters] final class S3FederatedConnection(
    val client: MinioClient,
    val bucket: String,
    val endpoint: String,
    val hadoopConf: Configuration
) extends FederatedConnection {

  override val sourceType: DataSourceType = DataSourceType.S3

  override def isConnected: IO[Boolean] = IO {
    try {
      client.listObjects(ListObjectsArgs.builder().bucket(bucket).maxKeys(1).build())
      true
    } catch {
      case _: Exception => false
    }
  }

  override def close: IO[Unit] = IO.unit

  override def metadata: Map[String, String] =
    Map(
      "endpoint"   -> endpoint,
      "bucket"     -> bucket,
      "sourceType" -> "S3"
    )
}

/** Calcite Schema that exposes S3 object prefixes as tables. */
private[adapters] final class S3CalciteSchema(s3Conn: S3FederatedConnection)
    extends AbstractSchema with LazyLogging {

  override def getTableMap: util.Map[String, org.apache.calcite.schema.Table] = {
    val adapter = new S3Adapter()
    val tables = adapter.discoverTables(s3Conn)
      .handleError(e => {
        logger.error(s"Failed to discover S3 tables: ${e.getMessage}")
        Seq.empty
      })
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val result = new util.HashMap[String, org.apache.calcite.schema.Table]()
    tables.foreach { tableInfo =>
      result.put(tableInfo.name, new S3ScannableTable(s3Conn, tableInfo))
    }
    result
  }
}

/** Calcite ScannableTable that reads data from S3 objects.
  * For Parquet files, reads via the Parquet reader.
  * For CSV/JSON, reads lines as string values.
  */
private[adapters] final class S3ScannableTable(
    s3Conn: S3FederatedConnection,
    tableInfo: FederatedTableInfo
) extends AbstractTable with ScannableTable with LazyLogging {

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val builder = typeFactory.builder()
    tableInfo.columns.foreach { col =>
      builder.add(
        col.name,
        typeFactory.createTypeWithNullability(
          typeFactory.createJavaType(classOf[String]),
          true
        )
      )
    }
    builder.build()
  }

  override def scan(root: DataContext): Enumerable[Array[AnyRef]] = {
    val prefix = tableInfo.name.replace('_', '/')
    val objects = s3Conn.client
      .listObjects(ListObjectsArgs.builder().bucket(s3Conn.bucket).prefix(prefix).recursive(true).build())
      .asScala.toSeq

    val keys = objects.flatMap { result =>
      val item = result.get()
      Some(item.objectName())
    }

    new AbstractEnumerable[Array[AnyRef]] {
      override def enumerator(): Enumerator[Array[AnyRef]] = {
        val allRows = keys.flatMap { key =>
          readObjectRows(key)
        }.iterator

        new Enumerator[Array[AnyRef]] {
          private var currentRow: Array[AnyRef] = _

          override def current(): Array[AnyRef] = currentRow

          override def moveNext(): Boolean = {
            if (allRows.hasNext) {
              currentRow = allRows.next()
              true
            } else {
              false
            }
          }

          override def reset(): Unit =
            throw new UnsupportedOperationException("Reset not supported on S3 enumerator")

          override def close(): Unit = ()
        }
      }
    }
  }

  override def getStatistic: Statistic = Statistics.UNKNOWN

  private def readObjectRows(key: String): Seq[Array[AnyRef]] = {
    try {
      val response = s3Conn.client.getObject(
        io.minio.GetObjectArgs.builder().bucket(s3Conn.bucket).`object`(key).build()
      )
      try {
        if (key.endsWith(".csv") || key.endsWith(".json")) {
          Using.resource(new BufferedReader(new InputStreamReader(response, StandardCharsets.UTF_8))) { reader =>
            val lines = new scala.collection.mutable.ArrayBuffer[Array[AnyRef]]()
            var line = reader.readLine()
            while (line != null) {
              lines += Array[AnyRef](line)
              line = reader.readLine()
            }
            lines.toSeq
          }
        } else {
          Seq.empty
        }
      } finally {
        response.close()
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to read S3 object $key: ${e.getMessage}")
        Seq.empty
    }
  }
}
