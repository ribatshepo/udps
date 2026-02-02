package io.gbmm.udps.storage.txn

import cats.effect.{IO, Ref}
import com.typesafe.scalalogging.LazyLogging

import java.time.Instant
import java.util.UUID

sealed trait LockType
object LockType {
  case object Shared extends LockType
  case object Exclusive extends LockType
}

final case class LockEntry(
  resource: String,
  lockType: LockType,
  holderId: UUID,
  acquiredAt: Instant
)

/**
 * Pessimistic lock manager with deadlock detection via wait-for graph DFS.
 *
 * Lock compatibility matrix:
 * {{{
 *                Existing Shared   Existing Exclusive
 * New Shared         OK               BLOCKED
 * New Exclusive    BLOCKED            BLOCKED
 * }}}
 *
 * Multiple shared locks from different transactions are allowed on the same resource.
 * An exclusive lock requires no other locks from other transactions on the resource.
 */
final class LockManager private (
  locks: Ref[IO, Map[String, Seq[LockEntry]]],
  transactionLog: TransactionLog
) extends LazyLogging {

  def acquireLock(txnId: UUID, resource: String, lockType: LockType): IO[Boolean] =
    IO.realTimeInstant.flatMap { now =>
      locks.modify { m =>
        val existing = m.getOrElse(resource, Seq.empty)
        val otherHolders = existing.filter(_.holderId != txnId)

        val canAcquire = lockType match {
          case LockType.Shared =>
            !otherHolders.exists(_.lockType == LockType.Exclusive)
          case LockType.Exclusive =>
            otherHolders.isEmpty
        }

        if (canAcquire) {
          val alreadyHeld = existing.exists(e => e.holderId == txnId && e.lockType == lockType)
          if (alreadyHeld) {
            (m, true)
          } else {
            val entry = LockEntry(resource, lockType, txnId, now)
            val updated = m.updated(resource, existing :+ entry)
            logger.debug("Lock acquired: txn={}, resource={}, type={}", txnId, resource, lockType)
            (updated, true)
          }
        } else {
          logger.debug("Lock denied: txn={}, resource={}, type={}, held by={}", txnId, resource, lockType, otherHolders.map(_.holderId))
          (m, false)
        }
      }
    }

  def releaseLock(txnId: UUID, resource: String): IO[Unit] =
    locks.update { m =>
      m.get(resource) match {
        case Some(entries) =>
          val remaining = entries.filterNot(_.holderId == txnId)
          if (remaining.isEmpty) m - resource
          else m.updated(resource, remaining)
        case None => m
      }
    } *> IO(logger.debug("Lock released: txn={}, resource={}", txnId, resource))

  def releaseAllLocks(txnId: UUID): IO[Unit] =
    locks.update { m =>
      m.map { case (resource, entries) =>
        resource -> entries.filterNot(_.holderId == txnId)
      }.filter(_._2.nonEmpty)
    } *> IO(logger.debug("All locks released for txn={}", txnId))

  /**
   * Detect deadlock using wait-for graph with DFS cycle detection.
   *
   * Builds a graph: for each active transaction that is waiting on a resource held by
   * another transaction, create an edge waiter -> holder. Then perform DFS to find cycles.
   * If a cycle is found, return the youngest transaction (most recent startedAt) in the cycle.
   */
  def detectDeadlock(txnId: UUID): IO[Option[UUID]] =
    for {
      lockState      <- locks.get
      activeEntries  <- transactionLog.getActiveTransactions
    } yield {
      val activeIds = activeEntries.map(_.transactionId).toSet
      val startTimes = activeEntries.map(e => e.transactionId -> e.startedAt).toMap

      val waitForGraph = buildWaitForGraph(lockState, activeIds)

      findCycleContaining(txnId, waitForGraph) match {
        case Some(cycle) if cycle.nonEmpty =>
          val youngest = cycle.maxBy(id => startTimes.getOrElse(id, Instant.MIN))
          logger.warn("Deadlock detected involving txn={}, cycle={}, aborting youngest={}", txnId, cycle, youngest)
          Some(youngest)
        case _ => None
      }
    }

  def getLocks(txnId: UUID): IO[Seq[LockEntry]] =
    locks.get.map { m =>
      m.values.flatten.filter(_.holderId == txnId).toSeq
    }

  /**
   * Build a wait-for graph from current lock state.
   *
   * A transaction T1 waits for T2 if T1 tried to acquire a lock on a resource
   * currently held by T2 in an incompatible mode. We approximate this by checking
   * all resources where multiple transactions hold shared locks -- any transaction
   * wanting exclusive would wait. More precisely, we look at resources with mixed
   * holders and build edges between them.
   */
  private def buildWaitForGraph(
    lockState: Map[String, Seq[LockEntry]],
    activeIds: Set[UUID]
  ): Map[UUID, Set[UUID]] = {
    val graph = scala.collection.mutable.Map[UUID, Set[UUID]]().withDefaultValue(Set.empty)

    lockState.foreach { case (_, entries) =>
      val activeEntries = entries.filter(e => activeIds.contains(e.holderId))
      val exclusiveHolders = activeEntries.filter(_.lockType == LockType.Exclusive).map(_.holderId).toSet
      val sharedHolders = activeEntries.filter(_.lockType == LockType.Shared).map(_.holderId).toSet

      // Exclusive holders block all other holders on the same resource
      for {
        excl <- exclusiveHolders
        other <- (exclusiveHolders ++ sharedHolders) - excl
      } {
        graph(other) = graph(other) + excl
      }

      // If there are shared holders, any transaction wanting exclusive waits on them
      // In our model, if a txn holds shared but wants exclusive upgrade, it waits on others
      if (sharedHolders.size > 1) {
        for {
          s1 <- sharedHolders
          s2 <- sharedHolders if s2 != s1
        } {
          // Potential upgrade deadlock: each shared holder would wait on others for exclusive
          graph(s1) = graph(s1) + s2
        }
      }
    }

    graph.toMap.withDefaultValue(Set.empty)
  }

  /**
   * DFS-based cycle detection starting from the given node.
   * Returns Some(cycle) if a cycle containing `startNode` is found, None otherwise.
   */
  private def findCycleContaining(startNode: UUID, graph: Map[UUID, Set[UUID]]): Option[Seq[UUID]] = {
    val visited = scala.collection.mutable.Set[UUID]()
    val inStack = scala.collection.mutable.Set[UUID]()
    val path = scala.collection.mutable.ListBuffer[UUID]()

    def dfs(node: UUID): Option[Seq[UUID]] = {
      if (inStack.contains(node)) {
        val cycleStart = path.indexOf(node)
        if (cycleStart >= 0) {
          val cycle = path.slice(cycleStart, path.size).toSeq
          if (cycle.contains(startNode)) return Some(cycle)
        }
        return None
      }
      if (visited.contains(node)) return None

      visited.add(node)
      inStack.add(node)
      path.append(node)

      val neighbors = graph.getOrElse(node, Set.empty)
      val result = neighbors.foldLeft(Option.empty[Seq[UUID]]) { (acc, neighbor) =>
        acc.orElse(dfs(neighbor))
      }

      path.remove(path.size - 1)
      inStack.remove(node)
      result
    }

    dfs(startNode)
  }
}

object LockManager {

  def create(transactionLog: TransactionLog): IO[LockManager] =
    Ref.of[IO, Map[String, Seq[LockEntry]]](Map.empty).map { ref =>
      new LockManager(ref, transactionLog)
    }
}
