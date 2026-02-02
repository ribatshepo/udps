package io.gbmm.udps.integration.ustrp

import cats.effect.IO
import cats.syntax.all._
import dev.profunktor.redis4cats.RedisCommands
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

/** Configuration for message deduplication.
  *
  * @param keyPrefix
  *   prefix for Redis keys used to track processed message IDs
  * @param ttl
  *   time-to-live for deduplication entries; after expiry a message ID may be
  *   reprocessed (should exceed the maximum expected redelivery window)
  */
final case class DeduplicatorConfig(
    keyPrefix: String,
    ttl: FiniteDuration
)

object DeduplicatorConfig {
  implicit val reader: ConfigReader[DeduplicatorConfig] =
    deriveReader[DeduplicatorConfig]
}

/** Trait for checking and recording whether a Kafka message has already been
  * processed, enabling exactly-once consumer semantics via deduplication.
  */
trait Deduplicator {

  /** Check whether a message with the given ID has already been processed.
    *
    * @param messageId
    *   unique identifier for the message (typically topic-partition-offset)
    * @return
    *   true if the message was previously marked as processed
    */
  def isDuplicate(messageId: String): IO[Boolean]

  /** Mark a single message as processed.
    *
    * @param messageId
    *   unique identifier for the message
    */
  def markProcessed(messageId: String): IO[Unit]

  /** Mark a batch of messages as processed in a single round-trip where
    * possible.
    *
    * @param messageIds
    *   identifiers for all messages to mark
    */
  def markProcessedBatch(messageIds: List[String]): IO[Unit]
}

/** Redis-backed [[Deduplicator]] that stores processed message IDs as keys with
  * a configured TTL. Duplicate checks use EXISTS; batch marking pipelines
  * multiple SETEX commands for efficiency.
  */
private[ustrp] final class RedisDeduplicator(
    commands: RedisCommands[IO, String, String],
    config: DeduplicatorConfig
) extends Deduplicator {

  private def redisKey(messageId: String): String =
    s"${config.keyPrefix}:$messageId"

  override def isDuplicate(messageId: String): IO[Boolean] =
    commands.exists(redisKey(messageId))

  override def markProcessed(messageId: String): IO[Unit] =
    commands.setEx(redisKey(messageId), "1", config.ttl).void

  override def markProcessedBatch(messageIds: List[String]): IO[Unit] =
    messageIds.traverse_ { (messageId: String) =>
      commands.setEx(redisKey(messageId), "1", config.ttl)
    }
}

object Deduplicator {

  /** Create a Redis-backed [[Deduplicator]].
    *
    * @param commands
    *   redis4cats command interface
    * @param config
    *   deduplication settings
    * @return
    *   a [[Deduplicator]] backed by Redis
    */
  def redis(
      commands: RedisCommands[IO, String, String],
      config: DeduplicatorConfig
  ): Deduplicator =
    new RedisDeduplicator(commands, config)
}
