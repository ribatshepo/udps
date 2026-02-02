package io.gbmm.udps.integration.ustrp

import cats.effect.{IO, Resource}
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import fs2.kafka._
import fs2.{Chunk, Stream}
import io.gbmm.udps.core.config.KafkaConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration._

/** Consumer-specific configuration for the USTRP Kafka consumer.
  *
  * @param topics          list of Kafka topics to subscribe to
  * @param batchSize       maximum number of records per batch
  * @param batchTimeout    maximum time to wait before flushing an incomplete batch
  * @param dlqTopic        topic to send unprocessable messages to
  * @param maxPollRecords  max records returned per poll
  * @param commitBatchSize number of offsets to accumulate before committing
  */
final case class KafkaConsumerConfig(
    topics: List[String],
    batchSize: Int,
    batchTimeout: FiniteDuration,
    dlqTopic: String,
    maxPollRecords: Int,
    commitBatchSize: Int
)

object KafkaConsumerConfig {
  implicit val reader: ConfigReader[KafkaConsumerConfig] =
    deriveReader[KafkaConsumerConfig]
}

/** Metrics interface for the USTRP Kafka consumer.
  *
  * Implementations capture operational metrics for monitoring.
  */
trait ConsumerMetrics {
  def recordConsumed(topic: String, partition: Int): IO[Unit]
  def recordLag(topic: String, partition: Int, lag: Long): IO[Unit]
  def recordThroughput(bytesPerSec: Double): IO[Unit]
  def recordDlq(topic: String, reason: String): IO[Unit]
}

/** Default metrics implementation backed by atomic counters.
  *
  * Suitable for local development and as a base for exporting to
  * a metrics system such as Prometheus via a periodic scrape.
  */
final class DefaultConsumerMetrics extends ConsumerMetrics with LazyLogging {

  val totalConsumed: AtomicLong = new AtomicLong(0L)
  val totalDlq: AtomicLong = new AtomicLong(0L)
  val lastLag: AtomicLong = new AtomicLong(0L)
  val lastThroughputBytesPerSec: AtomicLong = new AtomicLong(0L)

  override def recordConsumed(topic: String, partition: Int): IO[Unit] =
    IO.delay {
      val count = totalConsumed.incrementAndGet()
      if (count % LogIntervalRecords == 0L) {
        logger.info(
          s"Consumed $count records total (latest: topic=$topic partition=$partition)"
        )
      }
    }

  override def recordLag(
      topic: String,
      partition: Int,
      lag: Long
  ): IO[Unit] =
    IO.delay {
      lastLag.set(lag)
      logger.debug(s"Lag topic=$topic partition=$partition lag=$lag")
    }

  override def recordThroughput(bytesPerSec: Double): IO[Unit] =
    IO.delay {
      lastThroughputBytesPerSec.set(bytesPerSec.toLong)
      logger.debug(s"Throughput: ${bytesPerSec.toLong} bytes/sec")
    }

  override def recordDlq(topic: String, reason: String): IO[Unit] =
    IO.delay {
      val count = totalDlq.incrementAndGet()
      logger.warn(s"DLQ record #$count topic=$topic reason=$reason")
    }

  private val LogIntervalRecords: Long = 1000L
}

/** Abstraction for downstream batch processing (e.g. Parquet writes).
  *
  * Implementations receive a batch of consumer records and persist them.
  * The consumer commits offsets only after a successful call.
  */
trait RecordProcessor {

  /** Process a batch of consumer records.
    *
    * @param batch the records to process
    * @return IO that completes when the batch has been persisted
    */
  def process(
      batch: List[ConsumerRecord[String, Array[Byte]]]
  ): IO[Unit]
}

/** USTRP Kafka consumer that reads from configured topics, batches records,
  * delegates processing to a [[RecordProcessor]], commits offsets on success,
  * publishes failed messages to a DLQ topic, and exposes metrics.
  *
  * Back-pressure is handled naturally by fs2-kafka: if the processor is slow,
  * the internal queue fills and the consumer pauses polling until capacity is
  * available. The `maxPollRecords` setting further bounds per-poll volume.
  *
  * @param kafkaConfig    shared Kafka connectivity settings
  * @param consumerConfig consumer-specific settings
  * @param processor      downstream batch processor
  * @param metrics        metrics collector
  */
final class UstrpKafkaConsumer private (
    kafkaConfig: KafkaConfig,
    consumerConfig: KafkaConsumerConfig,
    processor: RecordProcessor,
    metrics: ConsumerMetrics
) extends LazyLogging {

  private val consumerSettings: ConsumerSettings[IO, String, Array[Byte]] =
    ConsumerSettings[IO, String, Array[Byte]]
      .withBootstrapServers(kafkaConfig.bootstrapServers)
      .withGroupId(kafkaConfig.groupId)
      .withAutoOffsetReset(parseAutoOffsetReset(kafkaConfig.autoOffsetReset))
      .withProperty("max.poll.records", consumerConfig.maxPollRecords.toString)
      .withEnableAutoCommit(false)

  private val producerSettings: ProducerSettings[IO, String, Array[Byte]] =
    ProducerSettings[IO, String, Array[Byte]]
      .withBootstrapServers(kafkaConfig.bootstrapServers)
      .withProperty("acks", "all")

  /** The main consumer stream.
    *
    * Subscribes to the configured topics, groups records into batches by
    * size or timeout, processes each batch, commits offsets, and sends
    * failures to the DLQ.
    *
    * Back-pressure is achieved through fs2-kafka's internal prefetch
    * mechanism: when the downstream processor is slow, the consumer
    * automatically pauses polling, preventing unbounded memory growth.
    */
  def stream: Stream[IO, Unit] =
    KafkaProducer
      .stream(producerSettings)
      .flatMap { dlqProducer =>
        KafkaConsumer
          .stream(consumerSettings)
          .subscribeTo(consumerConfig.topics.head, consumerConfig.topics.tail: _*)
          .records
          .groupWithin(
            consumerConfig.batchSize,
            consumerConfig.batchTimeout
          )
          .evalMap { chunk =>
            processBatch(chunk, dlqProducer)
          }
      }

  private def processBatch(
      chunk: Chunk[CommittableConsumerRecord[IO, String, Array[Byte]]],
      dlqProducer: KafkaProducer.Metrics[IO, String, Array[Byte]]
  ): IO[Unit] = {
    val records = chunk.toList
    val consumerRecords = records.map(_.record)

    for {
      batchStartNanos <- IO.delay(System.nanoTime())
      _ <- consumerRecords.traverse_ { rec =>
        metrics.recordConsumed(rec.topic, rec.partition)
      }
      _ <- processor
        .process(consumerRecords)
        .flatMap { _ =>
          commitOffsets(records) *> recordThroughputMetric(
            consumerRecords,
            batchStartNanos
          )
        }
        .handleErrorWith { ex =>
          IO.delay(
            logger.error(
              s"Batch processing failed, sending ${records.size} records to DLQ",
              ex
            )
          ) *> sendToDlq(consumerRecords, dlqProducer, ex) *> commitOffsets(
            records
          )
        }
    } yield ()
  }

  private def commitOffsets(
      records: List[CommittableConsumerRecord[IO, String, Array[Byte]]]
  ): IO[Unit] =
    if (records.isEmpty) IO.unit
    else
      records
        .foldLeft(CommittableOffsetBatch.empty[IO])((acc, r) =>
          acc.updated(r.offset)
        )
        .commit

  private def sendToDlq(
      records: List[ConsumerRecord[String, Array[Byte]]],
      producer: KafkaProducer.Metrics[IO, String, Array[Byte]],
      error: Throwable
  ): IO[Unit] =
    records.traverse_ { rec =>
      val dlqRecord = ProducerRecord(
        consumerConfig.dlqTopic,
        rec.key,
        rec.value
      ).withHeaders(
        Headers(
          Header("dlq.source.topic", rec.topic.getBytes("UTF-8")),
          Header(
            "dlq.source.partition",
            rec.partition.toString.getBytes("UTF-8")
          ),
          Header(
            "dlq.source.offset",
            rec.offset.toString.getBytes("UTF-8")
          ),
          Header(
            "dlq.error.message",
            Option(error.getMessage).getOrElse("unknown").getBytes("UTF-8")
          ),
          Header(
            "dlq.error.class",
            error.getClass.getName.getBytes("UTF-8")
          )
        )
      )
      val producerRecords =
        ProducerRecords.one(dlqRecord)
      producer.produce(producerRecords).flatten.void *>
        metrics.recordDlq(rec.topic, error.getMessage)
    }

  private def recordThroughputMetric(
      records: List[ConsumerRecord[String, Array[Byte]]],
      batchStartNanos: Long
  ): IO[Unit] =
    IO.delay {
      val elapsedNanos = System.nanoTime() - batchStartNanos
      val elapsedSeconds =
        if (elapsedNanos > 0L) elapsedNanos.toDouble / NanosPerSecond
        else 1.0d
      val totalBytes = records.foldLeft(0L) { (acc, rec) =>
        acc + (if (rec.value != null) rec.value.length.toLong else 0L)
      }
      totalBytes.toDouble / elapsedSeconds
    }.flatMap(metrics.recordThroughput)

  private def parseAutoOffsetReset(value: String): AutoOffsetReset =
    value.toLowerCase match {
      case "earliest" => AutoOffsetReset.Earliest
      case "latest"   => AutoOffsetReset.Latest
      case "none"     => AutoOffsetReset.None
      case _          => AutoOffsetReset.Earliest
    }

  private val NanosPerSecond: Double = 1000000000.0d
}

object UstrpKafkaConsumer extends LazyLogging {

  /** Create a managed [[UstrpKafkaConsumer]] wrapped in a [[Resource]].
    *
    * The resource logs lifecycle events on acquisition and release,
    * making it straightforward to compose into application startup.
    *
    * @param kafkaConfig    shared Kafka connectivity settings
    * @param consumerConfig consumer-specific settings
    * @param processor      downstream batch processor
    * @param metrics        metrics collector
    * @return a resource that yields the configured consumer
    */
  def resource(
      kafkaConfig: KafkaConfig,
      consumerConfig: KafkaConsumerConfig,
      processor: RecordProcessor,
      metrics: ConsumerMetrics
  ): Resource[IO, UstrpKafkaConsumer] =
    Resource.make(
      IO.delay {
        logger.info(
          s"Creating USTRP Kafka consumer for topics=${consumerConfig.topics.mkString(",")}" +
            s" group=${kafkaConfig.groupId}" +
            s" batchSize=${consumerConfig.batchSize}" +
            s" batchTimeout=${consumerConfig.batchTimeout}"
        )
        new UstrpKafkaConsumer(
          kafkaConfig,
          consumerConfig,
          processor,
          metrics
        )
      }
    )(_ =>
      IO.delay(
        logger.info("USTRP Kafka consumer resource released")
      )
    )
}
