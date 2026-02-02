package io.gbmm.udps.integration.ustrp

import cats.effect.{IO, Resource}
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import fs2.kafka._
import fs2.{Chunk, Stream}
import io.gbmm.udps.core.config.KafkaConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

/** Configuration for exactly-once transactional consumption.
  *
  * @param transactionalId
  *   unique identifier for the Kafka transactional producer instance; must be
  *   stable across restarts for fencing to work correctly
  * @param transactionTimeout
  *   maximum duration a transaction may remain open before the broker aborts it
  * @param deduplicator
  *   configuration for the Redis-based deduplication layer
  */
final case class ExactlyOnceConfig(
    transactionalId: String,
    transactionTimeout: FiniteDuration,
    deduplicator: DeduplicatorConfig
)

object ExactlyOnceConfig {
  implicit val reader: ConfigReader[ExactlyOnceConfig] =
    deriveReader[ExactlyOnceConfig]
}

/** Kafka consumer with exactly-once processing semantics.
  *
  * Achieves exactly-once via three mechanisms:
  *   - '''Transactional producer''' — offsets are committed atomically with
  *     output records so that partial failures cannot leave offsets ahead of
  *     processing.
  *   - '''Idempotent producer''' — enabled implicitly by the transactional
  *     producer, guarding against duplicate sends on retry.
  *   - '''Redis deduplication''' — message IDs (topic-partition-offset) are
  *     checked before processing and recorded after, so that redelivered
  *     messages are skipped even across consumer restarts.
  *
  * @param kafkaConfig
  *   shared Kafka connectivity settings
  * @param consumerConfig
  *   consumer-specific settings (topics, batching, etc.)
  * @param exactlyOnceConfig
  *   transactional and deduplication settings
  * @param processor
  *   downstream batch processor (e.g. Parquet writer)
  * @param metrics
  *   consumer metrics collector
  * @param deduplicator
  *   deduplication service backed by Redis
  */
final class ExactlyOnceConsumer private (
    kafkaConfig: KafkaConfig,
    consumerConfig: KafkaConsumerConfig,
    exactlyOnceConfig: ExactlyOnceConfig,
    processor: RecordProcessor,
    metrics: ConsumerMetrics,
    deduplicator: Deduplicator
) extends LazyLogging {

  private val consumerSettings: ConsumerSettings[IO, String, Array[Byte]] =
    ConsumerSettings[IO, String, Array[Byte]]
      .withBootstrapServers(kafkaConfig.bootstrapServers)
      .withGroupId(kafkaConfig.groupId)
      .withAutoOffsetReset(parseAutoOffsetReset(kafkaConfig.autoOffsetReset))
      .withProperty("max.poll.records", consumerConfig.maxPollRecords.toString)
      .withEnableAutoCommit(false)
      .withProperty("isolation.level", "read_committed")

  private val producerSettings: TransactionalProducerSettings[IO, String, Array[Byte]] =
    TransactionalProducerSettings(
      transactionalId = exactlyOnceConfig.transactionalId,
      producerSettings = ProducerSettings[IO, String, Array[Byte]]
        .withBootstrapServers(kafkaConfig.bootstrapServers)
        .withProperty("acks", "all")
        .withProperty(
          "transaction.timeout.ms",
          exactlyOnceConfig.transactionTimeout.toMillis.toString
        )
        .withProperty("enable.idempotence", "true")
    )

  /** The main transactional consumer stream.
    *
    * Subscribes to the configured topics, batches incoming records, filters
    * duplicates via Redis, processes the remaining records, marks them as
    * processed, and commits offsets transactionally.
    *
    * Back-pressure is handled by fs2-kafka: when the processor is slow the
    * internal queue fills and polling pauses automatically.
    */
  def stream: Stream[IO, Unit] =
    TransactionalKafkaProducer
      .stream(producerSettings)
      .flatMap { txnProducer =>
        KafkaConsumer
          .stream(consumerSettings)
          .subscribeTo(consumerConfig.topics.head, consumerConfig.topics.tail: _*)
          .records
          .groupWithin(consumerConfig.batchSize, consumerConfig.batchTimeout)
          .evalMap { chunk =>
            processTransactionalBatch(chunk, txnProducer)
          }
      }

  private def processTransactionalBatch(
      chunk: Chunk[CommittableConsumerRecord[IO, String, Array[Byte]]],
      txnProducer: TransactionalKafkaProducer[IO, String, Array[Byte]]
  ): IO[Unit] = {
    val records = chunk.toList

    if (records.isEmpty) IO.unit
    else
      for {
        _ <- records.traverse_ { r =>
          metrics.recordConsumed(r.record.topic, r.record.partition)
        }
        deduplicated <- filterDuplicates(records)
        _ <- processNonDuplicates(deduplicated)
        _ <- markAllProcessed(deduplicated)
        _ <- commitTransactionally(records, txnProducer)
      } yield ()
  }

  private def messageId(
      record: CommittableConsumerRecord[IO, String, Array[Byte]]
  ): String =
    s"${record.record.topic}-${record.record.partition}-${record.record.offset}"

  private def filterDuplicates(
      records: List[CommittableConsumerRecord[IO, String, Array[Byte]]]
  ): IO[List[CommittableConsumerRecord[IO, String, Array[Byte]]]] =
    records.filterA { record =>
      deduplicator.isDuplicate(messageId(record)).map { isDup =>
        if (isDup) {
          logger.debug(
            s"Skipping duplicate message: ${messageId(record)}"
          )
        }
        !isDup
      }
    }

  private def processNonDuplicates(
      records: List[CommittableConsumerRecord[IO, String, Array[Byte]]]
  ): IO[Unit] =
    if (records.isEmpty) IO.unit
    else {
      val consumerRecords = records.map(_.record)
      processor.process(consumerRecords)
    }

  private def markAllProcessed(
      records: List[CommittableConsumerRecord[IO, String, Array[Byte]]]
  ): IO[Unit] =
    if (records.isEmpty) IO.unit
    else {
      val ids = records.map(messageId)
      deduplicator.markProcessedBatch(ids)
    }

  private def commitTransactionally(
      records: List[CommittableConsumerRecord[IO, String, Array[Byte]]],
      txnProducer: TransactionalKafkaProducer[IO, String, Array[Byte]]
  ): IO[Unit] = {
    val committableRecords = records.map { r =>
      CommittableProducerRecords(
        List.empty[ProducerRecord[String, Array[Byte]]],
        r.offset
      )
    }
    val txnRecords: TransactionalProducerRecords[IO, String, Array[Byte]] =
      Chunk.from(committableRecords)
    txnProducer.produce(txnRecords).void
  }

  private def parseAutoOffsetReset(value: String): AutoOffsetReset =
    value.toLowerCase match {
      case "earliest" => AutoOffsetReset.Earliest
      case "latest"   => AutoOffsetReset.Latest
      case "none"     => AutoOffsetReset.None
      case _          => AutoOffsetReset.Earliest
    }
}

object ExactlyOnceConsumer extends LazyLogging {

  /** Create a managed [[ExactlyOnceConsumer]] wrapped in a [[Resource]].
    *
    * @param kafkaConfig
    *   shared Kafka connectivity settings
    * @param consumerConfig
    *   consumer-specific settings
    * @param exactlyOnceConfig
    *   transactional and deduplication settings
    * @param processor
    *   downstream batch processor
    * @param metrics
    *   consumer metrics collector
    * @param deduplicator
    *   deduplication service
    * @return
    *   a resource that yields the configured consumer
    */
  def resource(
      kafkaConfig: KafkaConfig,
      consumerConfig: KafkaConsumerConfig,
      exactlyOnceConfig: ExactlyOnceConfig,
      processor: RecordProcessor,
      metrics: ConsumerMetrics,
      deduplicator: Deduplicator
  ): Resource[IO, ExactlyOnceConsumer] =
    Resource.make(
      IO.delay {
        logger.info(
          s"Creating ExactlyOnceConsumer for topics=${consumerConfig.topics.mkString(",")}" +
            s" group=${kafkaConfig.groupId}" +
            s" transactionalId=${exactlyOnceConfig.transactionalId}" +
            s" batchSize=${consumerConfig.batchSize}"
        )
        new ExactlyOnceConsumer(
          kafkaConfig,
          consumerConfig,
          exactlyOnceConfig,
          processor,
          metrics,
          deduplicator
        )
      }
    )(_ =>
      IO.delay(
        logger.info("ExactlyOnceConsumer resource released")
      )
    )
}
