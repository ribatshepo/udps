package io.gbmm.udps.catalog.lineage

import java.util.UUID

import cats.effect.IO
import cats.implicits._

/** A node in the lineage directed acyclic graph. */
final case class LineageNode(id: UUID, name: String, nodeType: String)

/** Represents a lineage DAG with nodes and directed edges. */
final case class LineageDAG(nodes: Map[UUID, LineageNode], edges: Seq[LineageEdge]) {

  /** Returns the set of all upstream node IDs reachable from the given node via BFS. */
  def upstream(nodeId: UUID): Set[UUID] = {
    val adjacency = edges.groupBy(_.targetTableId).map {
      case (target, edgeList) => target -> edgeList.map(_.sourceTableId).toSet
    }
    bfs(nodeId, adjacency)
  }

  /** Returns the set of all downstream node IDs reachable from the given node via BFS. */
  def downstream(nodeId: UUID): Set[UUID] = {
    val adjacency = edges.groupBy(_.sourceTableId).map {
      case (source, edgeList) => source -> edgeList.map(_.targetTableId).toSet
    }
    bfs(nodeId, adjacency)
  }

  /** Returns all downstream nodes that would be impacted by a change to the given node. */
  def impactAnalysis(nodeId: UUID): Set[UUID] =
    downstream(nodeId)

  private def bfs(startId: UUID, adjacency: Map[UUID, Set[UUID]]): Set[UUID] = {
    @annotation.tailrec
    def loop(queue: List[UUID], visited: Set[UUID]): Set[UUID] =
      queue match {
        case Nil => visited
        case head :: tail =>
          val neighbors = adjacency.getOrElse(head, Set.empty) -- visited
          loop(tail ++ neighbors.toList, visited ++ neighbors)
      }

    loop(List(startId), Set.empty) - startId
  }
}

/**
 * Builds a LineageDAG by traversing lineage edges from a root table
 * up to a specified depth.
 */
class LineageGraphBuilder(store: LineageStore) {

  private val DefaultMaxDepth: Int = 10

  /**
   * Builds a lineage DAG rooted at the given table, traversing both upstream
   * and downstream edges up to the specified depth.
   *
   * @param rootTableId the starting table ID
   * @param depth       maximum traversal depth (default 10)
   * @return the constructed lineage DAG
   */
  def build(rootTableId: UUID, depth: Int = DefaultMaxDepth): IO[LineageDAG] = {
    val effectiveDepth = math.max(1, math.min(depth, DefaultMaxDepth))
    traverseEdges(Set(rootTableId), effectiveDepth, Set.empty, Seq.empty).map { allEdges =>
      val nodeIds = allEdges.flatMap(e => Seq(e.sourceTableId, e.targetTableId)).toSet + rootTableId
      val nodes = nodeIds.map(id => id -> LineageNode(id, id.toString, "table")).toMap
      LineageDAG(nodes, allEdges)
    }
  }

  private def traverseEdges(
    frontier: Set[UUID],
    remainingDepth: Int,
    visited: Set[UUID],
    accumulated: Seq[LineageEdge]
  ): IO[Seq[LineageEdge]] = {
    if (remainingDepth <= 0 || frontier.isEmpty) {
      IO.pure(accumulated)
    } else {
      frontier.toList.traverse { tableId =>
        store.getEdgesForTable(tableId)
      }.map(_.flatten).flatMap { newEdges =>
        val uniqueEdges = newEdges.filterNot(e => accumulated.exists(_.id == e.id))
        val newNodeIds = uniqueEdges.flatMap(e => Seq(e.sourceTableId, e.targetTableId)).toSet
        val nextFrontier = (newNodeIds -- visited) -- frontier
        traverseEdges(
          nextFrontier,
          remainingDepth - 1,
          visited ++ frontier,
          accumulated ++ uniqueEdges
        )
      }
    }
  }
}
