package io.gbmm.udps.storage.views

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging

import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

// ---------------------------------------------------------------------------
// Refresh mode ADT
// ---------------------------------------------------------------------------

sealed trait RefreshMode extends Product with Serializable

object RefreshMode {
  case object OnDemand extends RefreshMode
  final case class Scheduled(cronExpression: String) extends RefreshMode
  case object Incremental extends RefreshMode
  case object Continuous extends RefreshMode
}

// ---------------------------------------------------------------------------
// View definition
// ---------------------------------------------------------------------------

final case class ViewDefinition(
    id: UUID,
    name: String,
    namespace: String,
    sqlQuery: String,
    refreshMode: RefreshMode,
    baseTables: Seq[String],
    partitionColumns: Seq[String],
    createdAt: Instant,
    updatedAt: Instant,
    lastRefreshedAt: Option[Instant],
    isValid: Boolean,
    dataPath: String
)

// ---------------------------------------------------------------------------
// Refresh result
// ---------------------------------------------------------------------------

final case class ViewRefreshResult(
    viewId: UUID,
    rowsWritten: Long,
    refreshedAt: Instant,
    durationMs: Long,
    isIncremental: Boolean
)

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/**
 * Manages the lifecycle and refresh of materialized views.
 *
 * View definitions are stored in a thread-safe [[ConcurrentHashMap]].
 * Refresh is delegated to [[ViewRefresher]] based on the view's
 * [[RefreshMode]].
 */
final class MaterializedViewEngine private (
    views: ConcurrentHashMap[UUID, ViewDefinition],
    refresher: ViewRefresher
) extends LazyLogging {

  /** Register a new materialized view definition. */
  def createView(definition: ViewDefinition): IO[ViewDefinition] =
    IO.delay {
      val existing = views.putIfAbsent(definition.id, definition)
      if (existing != null) {
        throw new IllegalArgumentException(
          s"View with id=${definition.id} already exists (name=${existing.name})"
        )
      }
      logger.info(
        "Created materialized view: name={}, namespace={}, id={}",
        definition.name,
        definition.namespace,
        definition.id.toString
      )
      definition
    }

  /** Remove a view and its data path reference. */
  def dropView(viewId: UUID): IO[Unit] =
    IO.delay {
      val removed = views.remove(viewId)
      if (removed == null) {
        throw new NoSuchElementException(s"View not found: $viewId")
      }
      logger.info("Dropped materialized view: id={}, name={}", viewId.toString, removed.name)
    }

  /** Retrieve a view by id. */
  def getView(viewId: UUID): IO[Option[ViewDefinition]] =
    IO.delay(Option(views.get(viewId)))

  /** List all views in a namespace. */
  def listViews(namespace: String): IO[Seq[ViewDefinition]] =
    IO.delay {
      views.values().asScala.filter(_.namespace == namespace).toSeq
    }

  /** Mark a view as invalid (e.g. because a base table schema changed). */
  def invalidateView(viewId: UUID): IO[Unit] =
    IO.delay {
      views.computeIfPresent(
        viewId,
        (_: UUID, existing: ViewDefinition) =>
          existing.copy(isValid = false, updatedAt = Instant.now())
      )
      logger.warn("Invalidated materialized view: id={}", viewId.toString)
    }

  /**
   * Trigger a refresh of the given view.
   *
   * The refresh strategy depends on the view's [[RefreshMode]]:
   *   - [[RefreshMode.OnDemand]] and [[RefreshMode.Scheduled]] perform a
   *     full (on-demand) refresh.
   *   - [[RefreshMode.Incremental]] performs a delta refresh since the
   *     last refresh timestamp.
   *   - [[RefreshMode.Continuous]] performs a full refresh (single-shot;
   *     for streaming use [[ViewRefresher.refreshContinuous]]).
   *
   * After a successful refresh the view definition is updated with the
   * new [[ViewDefinition.lastRefreshedAt]] and re-validated.
   */
  def refreshView(viewId: UUID): IO[ViewRefreshResult] =
    for {
      view <- IO.delay(Option(views.get(viewId))).flatMap {
        case Some(v) => IO.pure(v)
        case None    => IO.raiseError(new NoSuchElementException(s"View not found: $viewId"))
      }
      result <- view.refreshMode match {
        case RefreshMode.OnDemand | RefreshMode.Scheduled(_) =>
          refresher.refreshOnDemand(view)
        case RefreshMode.Incremental =>
          val since = view.lastRefreshedAt.getOrElse(Instant.EPOCH)
          refresher.refreshIncremental(view, since)
        case RefreshMode.Continuous =>
          refresher.refreshOnDemand(view)
      }
      _ <- IO.delay {
        views.computeIfPresent(
          viewId,
          (_: UUID, existing: ViewDefinition) =>
            existing.copy(
              lastRefreshedAt = Some(result.refreshedAt),
              isValid = true,
              updatedAt = result.refreshedAt
            )
        )
        logger.info(
          "Refreshed view id={}, rows={}, durationMs={}",
          viewId.toString,
          result.rowsWritten.toString,
          result.durationMs.toString
        )
      }
    } yield result
}

object MaterializedViewEngine {

  /**
   * Build a [[MaterializedViewEngine]] wrapped in [[IO]].
   *
   * @param refresher the [[ViewRefresher]] that performs actual data writes
   */
  def create(refresher: ViewRefresher): IO[MaterializedViewEngine] =
    IO.delay {
      val store = new ConcurrentHashMap[UUID, ViewDefinition]()
      new MaterializedViewEngine(store, refresher)
    }
}
