package io.gbmm.udps.catalog.discovery.sources

import cats.effect.{IO, Resource}
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.catalog.discovery._
import io.gbmm.udps.core.domain.DataType
import io.circe.{Json, JsonNumber}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters._

final class KafkaScanner extends DataSourceScanner with LazyLogging {

  override val sourceType: String = "kafka"

  private val SampleTimeoutMs = 5000L
  private val TopicListTimeoutSec = 30
  private val AdminClientCloseTimeoutSec = 5
  private val ConsumerCloseTimeoutSec = 5

  override def scan(config: ScanConfig): IO[DiscoveryResult] = {
    val topicFilter = config.options.getOrElse("topicFilter", ".*")
    val topicRegex = topicFilter.r
    val excludeInternal = config.options.getOrElse("excludeInternal", "true").toBoolean

    adminClientResource(config.connectionString)
      .use { adminClient =>
        IO.blocking {
          val topicNames = adminClient
            .listTopics()
            .names()
            .get(TopicListTimeoutSec.toLong, java.util.concurrent.TimeUnit.SECONDS)
            .asScala
            .filter(name => topicRegex.findFirstIn(name).isDefined)
            .filter(name => !excludeInternal || !name.startsWith("__"))
            .toSeq

          val tables = topicNames.flatMap { topicName =>
            inferTopicSchema(config.connectionString, topicName, config.options) match {
              case Right(table) =>
                Seq(table)
              case Left(error) =>
                logger.warn("Could not infer schema for topic {}: {}", topicName, error)
                Seq(DiscoveredTable(
                  name = topicName,
                  columns = Seq(DiscoveredColumn("value", DataType.Binary, nullable = true, ordinalPosition = 1)),
                  rowCount = None,
                  primaryKey = None
                ))
            }
          }

          DiscoveryResult(tables = tables, errors = Seq.empty)
        }
      }
      .handleErrorWith { ex =>
        IO.delay {
          logger.error("Kafka scan failed for {}", config.connectionString, ex)
          DiscoveryResult(tables = Seq.empty, errors = Seq(s"Kafka scan error: ${ex.getMessage}"))
        }
      }
  }

  private def adminClientResource(bootstrapServers: String): Resource[IO, AdminClient] =
    Resource.make(IO.blocking {
      val props = new Properties()
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
      AdminClient.create(props)
    })(client => IO.blocking(client.close(Duration.ofSeconds(AdminClientCloseTimeoutSec))).handleErrorWith(_ => IO.unit))

  private def consumerResource(bootstrapServers: String, options: Map[String, String]): Resource[IO, KafkaConsumer[String, String]] =
    Resource.make(IO.blocking {
      val props = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      props.put(ConsumerConfig.GROUP_ID_CONFIG, options.getOrElse("groupId", "udps-schema-discovery"))
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
      new KafkaConsumer[String, String](props)
    })(consumer => IO.blocking(consumer.close(Duration.ofSeconds(ConsumerCloseTimeoutSec))).handleErrorWith(_ => IO.unit))

  private def inferTopicSchema(
    bootstrapServers: String,
    topicName: String,
    options: Map[String, String]
  ): Either[String, DiscoveredTable] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, options.getOrElse("groupId", "udps-schema-discovery"))
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")

    var consumer: KafkaConsumer[String, String] = null
    try {
      consumer = new KafkaConsumer[String, String](props)
      val partitions = consumer.partitionsFor(topicName).asScala.toList
      if (partitions.isEmpty) {
        return Left(s"No partitions found for topic $topicName")
      }

      val topicPartition = new TopicPartition(topicName, partitions.head.partition())
      consumer.assign(java.util.Collections.singletonList(topicPartition))
      consumer.seekToEnd(java.util.Collections.singletonList(topicPartition))

      val endOffset = consumer.position(topicPartition)
      if (endOffset <= 0) {
        return Left(s"Topic $topicName is empty")
      }

      consumer.seek(topicPartition, endOffset - 1)
      val records = consumer.poll(Duration.ofMillis(SampleTimeoutMs))

      if (records.isEmpty) {
        Left(s"Could not read sample message from topic $topicName")
      } else {
        val record = records.iterator().next()
        val columns = inferColumnsFromJson(record.value())
        Right(DiscoveredTable(
          name = topicName,
          columns = columns,
          rowCount = None,
          primaryKey = None
        ))
      }
    } catch {
      case ex: Exception =>
        Left(s"Error reading topic $topicName: ${ex.getMessage}")
    } finally {
      if (consumer != null) {
        try { consumer.close(Duration.ofSeconds(ConsumerCloseTimeoutSec)) } catch { case _: Exception => () }
      }
    }
  }

  private def inferColumnsFromJson(jsonStr: String): Seq[DiscoveredColumn] =
    io.circe.parser.parse(jsonStr) match {
      case Right(json) =>
        json.asObject match {
          case Some(obj) =>
            obj.toList.zipWithIndex.map { case ((key, value), idx) =>
              DiscoveredColumn(
                name = key,
                dataType = inferJsonType(value),
                nullable = value.isNull,
                ordinalPosition = idx + 1
              )
            }
          case None =>
            Seq(DiscoveredColumn("value", inferJsonType(json), nullable = true, ordinalPosition = 1))
        }
      case Left(_) =>
        Seq(DiscoveredColumn("value", DataType.Utf8, nullable = true, ordinalPosition = 1))
    }

  private def inferJsonType(json: Json): DataType =
    if (json.isBoolean) DataType.Boolean
    else if (json.isNumber) inferNumericType(json.asNumber)
    else if (json.isString) DataType.Utf8
    else if (json.isArray) {
      val elements = json.asArray.getOrElse(Vector.empty)
      val elementType = elements.headOption.map(inferJsonType).getOrElse(DataType.Utf8)
      DataType.List(elementType)
    }
    else if (json.isObject) {
      val fields = json.asObject.map(_.toList).getOrElse(List.empty)
      val columnFields = fields.map { case (name, value) =>
        import io.gbmm.udps.core.domain.ColumnMetadata
        ColumnMetadata(
          name = name,
          dataType = inferJsonType(value),
          nullable = value.isNull,
          description = None,
          tags = Map.empty,
          statistics = None
        )
      }
      DataType.Struct(columnFields)
    }
    else DataType.Utf8

  private def inferNumericType(numOpt: Option[JsonNumber]): DataType =
    numOpt match {
      case Some(num) =>
        num.toLong match {
          case Some(_) => DataType.Int64
          case None    => DataType.Float64
        }
      case None => DataType.Float64
    }
}
