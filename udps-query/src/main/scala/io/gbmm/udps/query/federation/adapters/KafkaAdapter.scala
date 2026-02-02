package io.gbmm.udps.query.federation.adapters

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.core.domain.DataType
import io.gbmm.udps.query.federation._
import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}
import org.apache.calcite.schema.{ScannableTable, Schema, Statistic, Statistics}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import java.time.Duration
import java.util
import java.util.{Properties => JProperties}
import scala.jdk.CollectionConverters._

/** Federation adapter for Apache Kafka.
  *
  * Discovers topics as tables with a fixed schema of (key, value, partition, offset, timestamp).
  * If a schema registry URL is configured, attempts to retrieve the value schema for richer
  * column definitions. Creates Calcite ScannableTable implementations that consume from topics
  * with configurable offset and message limits.
  */
final class KafkaAdapter extends FederationAdapter with LazyLogging {

  private val KAFKA_DEFAULT_PORT = 9092
  private val DEFAULT_MAX_MESSAGES = 10000

  override val sourceType: DataSourceType = DataSourceType.Kafka

  override val pushdownCapabilities: PushdownCapabilities = PushdownCapabilities(
    predicates = false,
    projections = false,
    limits = true,
    aggregations = false,
    joins = false
  )

  override def connect(config: DataSourceConfig): IO[FederatedConnection] = IO {
    val effectivePort = if (config.port > 0) config.port else KAFKA_DEFAULT_PORT
    val bootstrapServers = s"${config.host}:$effectivePort"

    val props = new JProperties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000")
    config.properties.foreach { case (k, v) => props.put(k, v) }

    val adminClient = AdminClient.create(props)
    logger.info(s"Kafka AdminClient connected to $bootstrapServers")

    new KafkaFederatedConnection(
      adminClient = adminClient,
      bootstrapServers = bootstrapServers,
      topicPrefix = config.database,
      schemaRegistryUrl = config.properties.get("schema.registry.url"),
      consumerProperties = config.properties,
      maxMessages = config.properties.get("max.messages").flatMap(s => scala.util.Try(s.toInt).toOption)
        .getOrElse(DEFAULT_MAX_MESSAGES)
    )
  }

  override def discoverTables(connection: FederatedConnection): IO[Seq[FederatedTableInfo]] = IO {
    val kafkaConn = connection.asInstanceOf[KafkaFederatedConnection]
    val topicNames = kafkaConn.adminClient.listTopics().names().get().asScala.toSeq
    val filteredTopics = if (kafkaConn.topicPrefix.nonEmpty && kafkaConn.topicPrefix != "*") {
      topicNames.filter(_.startsWith(kafkaConn.topicPrefix))
    } else {
      topicNames.filterNot(_.startsWith("__"))
    }

    val descriptions = kafkaConn.adminClient
      .describeTopics(filteredTopics.asJava)
      .allTopicNames().get().asScala

    filteredTopics.map { topicName =>
      val columns = buildTopicColumns(kafkaConn.schemaRegistryUrl, topicName)
      val partitionCount = descriptions.get(topicName)
        .map(_.partitions().size().toLong)
        .getOrElse(1L)

      FederatedTableInfo(
        name = topicName,
        columns = columns,
        estimatedRows = partitionCount * kafkaConn.maxMessages,
        sourceType = DataSourceType.Kafka
      )
    }
  }

  override def createCalciteSchema(connection: FederatedConnection): IO[Schema] = IO {
    val kafkaConn = connection.asInstanceOf[KafkaFederatedConnection]
    new KafkaCalciteSchema(kafkaConn)
  }

  /** Build column definitions for a topic.
    * Base columns are always: key (Utf8), value (Utf8), partition (Int32), offset (Int64), timestamp (TimestampMillis).
    * If a schema registry is configured, the value column may be expanded with typed fields.
    */
  private def buildTopicColumns(
      schemaRegistryUrl: Option[String],
      topicName: String
  ): Seq[FederatedColumnInfo] = {
    val baseColumns = Seq(
      FederatedColumnInfo("key", DataType.Utf8),
      FederatedColumnInfo("value", DataType.Utf8),
      FederatedColumnInfo("partition", DataType.Int32),
      FederatedColumnInfo("offset", DataType.Int64),
      FederatedColumnInfo("timestamp", DataType.TimestampMillis)
    )

    schemaRegistryUrl match {
      case Some(registryUrl) =>
        inferSchemaFromRegistry(registryUrl, topicName).getOrElse(baseColumns)
      case None =>
        baseColumns
    }
  }

  /** Attempt to infer a richer schema from the Confluent Schema Registry.
    * Falls back to None if the registry is unreachable or the subject is not found.
    */
  private def inferSchemaFromRegistry(
      registryUrl: String,
      topicName: String
  ): Option[Seq[FederatedColumnInfo]] = {
    try {
      val subjectUrl = s"$registryUrl/subjects/$topicName-value/versions/latest"
      val connection = new java.net.URL(subjectUrl).openConnection()
        .asInstanceOf[java.net.HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setRequestProperty("Accept", "application/json")
      connection.setConnectTimeout(5000)
      connection.setReadTimeout(5000)

      try {
        val responseCode = connection.getResponseCode
        if (responseCode == 200) {
          val body = scala.io.Source.fromInputStream(connection.getInputStream).mkString

          import io.circe.parser._
          parse(body).toOption.flatMap { json =>
            json.hcursor.downField("schema").as[String].toOption.flatMap { schemaStr =>
              parseAvroSchemaToColumns(schemaStr, topicName)
            }
          }
        } else {
          None
        }
      } finally {
        connection.disconnect()
      }
    } catch {
      case e: Exception =>
        logger.debug(s"Schema registry lookup failed for $topicName: ${e.getMessage}")
        None
    }
  }

  /** Parse an Avro schema JSON string into FederatedColumnInfo.
    * Extracts field names and types from the Avro record schema.
    */
  private def parseAvroSchemaToColumns(
      avroSchemaJson: String,
      topicName: String
  ): Option[Seq[FederatedColumnInfo]] = {
    import io.circe.parser._
    parse(avroSchemaJson).toOption.flatMap { json =>
      json.hcursor.downField("fields").focus.flatMap(_.asArray).map { fields =>
        val valueColumns = fields.flatMap { field =>
          for {
            name <- field.hcursor.downField("name").as[String].toOption
            avroType = field.hcursor.downField("type").focus
            dataType = mapAvroType(avroType)
          } yield FederatedColumnInfo(name, dataType)
        }

        Seq(
          FederatedColumnInfo("key", DataType.Utf8)
        ) ++ valueColumns ++ Seq(
          FederatedColumnInfo("partition", DataType.Int32),
          FederatedColumnInfo("offset", DataType.Int64),
          FederatedColumnInfo("timestamp", DataType.TimestampMillis)
        )
      }
    }
  }

  private def mapAvroType(avroType: Option[io.circe.Json]): DataType = {
    avroType match {
      case Some(json) =>
        json.asString match {
          case Some("string")  => DataType.Utf8
          case Some("int")     => DataType.Int32
          case Some("long")    => DataType.Int64
          case Some("float")   => DataType.Float32
          case Some("double")  => DataType.Float64
          case Some("boolean") => DataType.Boolean
          case Some("bytes")   => DataType.Binary
          case Some("null")    => DataType.Null
          case _               =>
            json.asArray match {
              case Some(unionTypes) =>
                unionTypes.find(t => t.asString.exists(_ != "null"))
                  .map(t => mapAvroType(Some(t)))
                  .getOrElse(DataType.Utf8)
              case None => DataType.Utf8
            }
        }
      case None => DataType.Utf8
    }
  }
}

object KafkaAdapter {
  def apply(): KafkaAdapter = new KafkaAdapter()
}

/** FederatedConnection wrapping a Kafka AdminClient. */
private[adapters] final class KafkaFederatedConnection(
    val adminClient: AdminClient,
    val bootstrapServers: String,
    val topicPrefix: String,
    val schemaRegistryUrl: Option[String],
    val consumerProperties: Map[String, String],
    val maxMessages: Int
) extends FederatedConnection {

  override val sourceType: DataSourceType = DataSourceType.Kafka

  override def isConnected: IO[Boolean] = IO {
    try {
      adminClient.listTopics().names().get()
      true
    } catch {
      case _: Exception => false
    }
  }

  override def close: IO[Unit] = IO {
    adminClient.close()
  }

  override def metadata: Map[String, String] =
    Map(
      "bootstrapServers" -> bootstrapServers,
      "topicPrefix"      -> topicPrefix,
      "sourceType"       -> "Kafka"
    ) ++ schemaRegistryUrl.map("schemaRegistryUrl" -> _)
}

/** Calcite Schema exposing Kafka topics as tables. */
private[adapters] final class KafkaCalciteSchema(
    kafkaConn: KafkaFederatedConnection
) extends AbstractSchema with LazyLogging {

  override def getTableMap: util.Map[String, org.apache.calcite.schema.Table] = {
    val adapter = new KafkaAdapter()
    val tables = adapter.discoverTables(kafkaConn)
      .handleError(e => {
        logger.error(s"Failed to discover Kafka topics: ${e.getMessage}")
        Seq.empty
      })
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val result = new util.HashMap[String, org.apache.calcite.schema.Table]()
    tables.foreach { tableInfo =>
      result.put(tableInfo.name, new KafkaScannableTable(kafkaConn, tableInfo))
    }
    result
  }
}

/** Calcite ScannableTable that consumes messages from a Kafka topic.
  *
  * Reads up to `maxMessages` from all partitions starting from the beginning
  * (or a configured offset). Each row is: (key, value, partition, offset, timestamp).
  */
private[adapters] final class KafkaScannableTable(
    kafkaConn: KafkaFederatedConnection,
    tableInfo: FederatedTableInfo
) extends AbstractTable with ScannableTable with LazyLogging {

  private val CONSUMER_GROUP_PREFIX = "udps-federation-"

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val builder = typeFactory.builder()
    tableInfo.columns.foreach { col =>
      val javaType = col.dataType match {
        case DataType.Int32          => typeFactory.createJavaType(classOf[java.lang.Integer])
        case DataType.Int64          => typeFactory.createJavaType(classOf[java.lang.Long])
        case DataType.Float32        => typeFactory.createJavaType(classOf[java.lang.Float])
        case DataType.Float64        => typeFactory.createJavaType(classOf[java.lang.Double])
        case DataType.Boolean        => typeFactory.createJavaType(classOf[java.lang.Boolean])
        case DataType.TimestampMillis => typeFactory.createJavaType(classOf[java.lang.Long])
        case _                       => typeFactory.createJavaType(classOf[String])
      }
      builder.add(col.name, typeFactory.createTypeWithNullability(javaType, true))
    }
    builder.build()
  }

  override def scan(root: DataContext): Enumerable[Array[AnyRef]] = {
    new AbstractEnumerable[Array[AnyRef]] {
      override def enumerator(): Enumerator[Array[AnyRef]] = {
        val consumer = createConsumer()
        val topicPartitions = consumer.partitionsFor(tableInfo.name).asScala.map { pi =>
          new TopicPartition(pi.topic(), pi.partition())
        }.asJava
        consumer.assign(topicPartitions)
        consumer.seekToBeginning(topicPartitions)

        new KafkaEnumerator(consumer, kafkaConn.maxMessages, tableInfo.columns.size)
      }
    }
  }

  override def getStatistic: Statistic = Statistics.UNKNOWN

  private def createConsumer(): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val props = new JProperties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConn.bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, s"$CONSUMER_GROUP_PREFIX${tableInfo.name}-${System.nanoTime()}")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaConn.maxMessages.toString)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)

    kafkaConn.consumerProperties
      .filterNot { case (k, _) => k.startsWith("schema.registry") || k == "max.messages" }
      .foreach { case (k, v) => props.put(k, v) }

    new KafkaConsumer[Array[Byte], Array[Byte]](props)
  }
}

/** Enumerator that polls Kafka and produces rows until maxMessages is reached or no more data. */
private[adapters] final class KafkaEnumerator(
    consumer: KafkaConsumer[Array[Byte], Array[Byte]],
    maxMessages: Int,
    columnCount: Int
) extends Enumerator[Array[AnyRef]] with LazyLogging {

  private val POLL_TIMEOUT = Duration.ofMillis(5000)
  private val MAX_EMPTY_POLLS = 3

  private var currentRow: Array[AnyRef] = _
  private var totalConsumed: Int = 0
  private var emptyPolls: Int = 0
  private val buffer = new java.util.ArrayDeque[Array[AnyRef]]()

  override def current(): Array[AnyRef] = currentRow

  override def moveNext(): Boolean = {
    if (!buffer.isEmpty) {
      currentRow = buffer.poll()
      return true
    }

    if (totalConsumed >= maxMessages || emptyPolls >= MAX_EMPTY_POLLS) {
      return false
    }

    val records = consumer.poll(POLL_TIMEOUT)
    if (records.isEmpty) {
      emptyPolls += 1
      return false
    }

    emptyPolls = 0
    records.asScala.foreach { record =>
      if (totalConsumed < maxMessages) {
        val charset = java.nio.charset.StandardCharsets.UTF_8
        val keyStr: AnyRef = if (record.key() != null) new String(record.key(), charset) else null
        val valueStr: AnyRef = if (record.value() != null) new String(record.value(), charset) else null
        val partition: AnyRef = Integer.valueOf(record.partition())
        val offset: AnyRef = java.lang.Long.valueOf(record.offset())
        val timestamp: AnyRef = java.lang.Long.valueOf(record.timestamp())

        val row = if (columnCount <= 5) {
          Array[AnyRef](keyStr, valueStr, partition, offset, timestamp)
        } else {
          val arr = new Array[AnyRef](columnCount)
          arr(0) = keyStr
          arr(columnCount - 3) = partition
          arr(columnCount - 2) = offset
          arr(columnCount - 1) = timestamp
          if (valueStr != null) {
            arr(1) = valueStr
          }
          arr
        }
        buffer.add(row)
        totalConsumed += 1
      }
    }

    if (!buffer.isEmpty) {
      currentRow = buffer.poll()
      true
    } else {
      false
    }
  }

  override def reset(): Unit =
    throw new UnsupportedOperationException("Reset not supported on Kafka enumerator")

  override def close(): Unit = {
    try {
      consumer.close()
    } catch {
      case e: Exception =>
        logger.warn(s"Error closing Kafka consumer: ${e.getMessage}")
    }
  }
}
