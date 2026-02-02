package io.gbmm.udps.query.cache

import cats.effect.{IO, Ref, Resource}
import com.typesafe.scalalogging.LazyLogging
import fs2.Stream

// ---------------------------------------------------------------------------
// Table modification events
// ---------------------------------------------------------------------------

sealed trait TableModificationEvent extends Product with Serializable

object TableModificationEvent {
  final case class TableWritten(tableName: String)       extends TableModificationEvent
  final case class SchemaChanged(tableName: String)      extends TableModificationEvent
  final case class ManualInvalidation(cacheKey: String)  extends TableModificationEvent
}

// ---------------------------------------------------------------------------
// Invalidation metrics
// ---------------------------------------------------------------------------

final case class InvalidationMetrics(
  invalidationsPerformed: Long,
  eventsProcessed: Long
)

// ---------------------------------------------------------------------------
// CacheInvalidator
// ---------------------------------------------------------------------------

final class CacheInvalidator private (
  cache: QueryCache,
  invalidationCountRef: Ref[IO, Long],
  eventsProcessedRef: Ref[IO, Long]
) extends LazyLogging {

  import TableModificationEvent._

  /** Invalidate all cached queries touching the specified table due to a write. */
  def onTableWrite(tableName: String): IO[Unit] =
    for {
      count <- cache.invalidateTable(tableName)
      _     <- invalidationCountRef.update(_ + count)
      _     <- eventsProcessedRef.update(_ + 1L)
      _     <- IO(logger.info(s"Table write invalidation: table=$tableName, entries_invalidated=$count"))
    } yield ()

  /** Invalidate all cached queries touching the specified table due to a schema change. */
  def onSchemaChange(tableName: String): IO[Unit] =
    for {
      count <- cache.invalidateTable(tableName)
      _     <- invalidationCountRef.update(_ + count)
      _     <- eventsProcessedRef.update(_ + 1L)
      _     <- IO(logger.info(s"Schema change invalidation: table=$tableName, entries_invalidated=$count"))
    } yield ()

  /** Invalidate a specific cache entry by its key. */
  def onManualInvalidate(cacheKey: String): IO[Unit] =
    for {
      _ <- cache.invalidateKey(cacheKey)
      _ <- invalidationCountRef.update(_ + 1L)
      _ <- eventsProcessedRef.update(_ + 1L)
      _ <- IO(logger.info(s"Manual invalidation: key=$cacheKey"))
    } yield ()

  /** Process a single modification event, dispatching to the appropriate handler. */
  def processEvent(event: TableModificationEvent): IO[Unit] =
    event match {
      case TableWritten(table)       => onTableWrite(table)
      case SchemaChanged(table)      => onSchemaChange(table)
      case ManualInvalidation(key)   => onManualInvalidate(key)
    }

  /**
   * Process a stream of table modification events, performing cache invalidation
   * for each event. Errors on individual events are logged and do not terminate the stream.
   */
  def processEvents(events: Stream[IO, TableModificationEvent]): Stream[IO, Unit] =
    events.evalMap { event =>
      processEvent(event).handleErrorWith { err =>
        IO(logger.error(s"Error processing invalidation event=$event", err))
      }
    }

  /** Retrieve current invalidation metrics. */
  def metrics: IO[InvalidationMetrics] =
    for {
      invalidations <- invalidationCountRef.get
      processed     <- eventsProcessedRef.get
    } yield InvalidationMetrics(
      invalidationsPerformed = invalidations,
      eventsProcessed = processed
    )

  /** Reset all metrics counters. */
  def resetMetrics: IO[Unit] =
    invalidationCountRef.set(0L) *> eventsProcessedRef.set(0L)
}

object CacheInvalidator {

  def make(cache: QueryCache): Resource[IO, CacheInvalidator] =
    Resource.eval(
      for {
        invalidationCount <- Ref.of[IO, Long](0L)
        eventsProcessed   <- Ref.of[IO, Long](0L)
      } yield new CacheInvalidator(cache, invalidationCount, eventsProcessed)
    )
}
