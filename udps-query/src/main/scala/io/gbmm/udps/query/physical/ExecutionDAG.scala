package io.gbmm.udps.query.physical

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

// ---------------------------------------------------------------------------
// Execution stage -- one unit of work in the distributed execution plan
// ---------------------------------------------------------------------------

/** A single execution stage in the DAG.
  *
  * @param id             unique numeric identifier within the DAG
  * @param operator       physical operator executed by this stage
  * @param dependencies   set of stage IDs that must complete before this one
  * @param partitionCount degree of parallelism for this stage
  */
final case class ExecutionStage(
  id: Int,
  operator: PhysicalOperator,
  dependencies: Set[Int],
  partitionCount: Int
) extends Serializable

// ---------------------------------------------------------------------------
// Execution DAG
// ---------------------------------------------------------------------------

/** Directed acyclic graph of [[ExecutionStage]]s derived from a
  * [[PhysicalOperator]] tree.
  *
  * Shuffle boundaries are inserted at joins and aggregations whose children
  * originate from different data partitioning schemes, ensuring each stage
  * can execute with partition-local data.
  *
  * Instances are `Serializable` so they can be shipped to distributed workers.
  *
  * Thread-safety: all public methods operate on immutable snapshots built at
  * construction time.
  */
final case class ExecutionDAG(
  stages: Map[Int, ExecutionStage],
  rootStages: Set[Int]
) extends Serializable {

  /** The root (final output) stage. Returns the first root stage. */
  def rootStage: ExecutionStage = stages(rootStages.head)

  /** Direct dependency stages of the given stage. */
  def dependenciesOf(stageId: Int): Set[ExecutionStage] =
    stages.get(stageId) match {
      case Some(stage) => stage.dependencies.flatMap(stages.get)
      case None        => Set.empty
    }

  /** Topological ordering of stage IDs (leaves first, root last) suitable
    * for scheduling execution.
    */
  def topologicalOrder: Seq[Int] = {
    val visited  = mutable.LinkedHashSet.empty[Int]
    val visiting = mutable.Set.empty[Int]

    def visit(id: Int): Unit = {
      if (visited.contains(id)) return
      if (visiting.contains(id)) {
        throw new IllegalStateException(
          s"Cycle detected in ExecutionDAG at stage '$id'"
        )
      }
      visiting += id
      stages.get(id).foreach { stage =>
        stage.dependencies.foreach(visit)
      }
      visiting -= id
      visited += id
    }

    stages.keys.foreach(visit)
    visited.toSeq
  }

  /** Return stages in topological order as [[ExecutionStage]] objects. */
  def topologicalStages: Seq[ExecutionStage] =
    topologicalOrder.flatMap(stages.get)

  /** Number of stages in the DAG. */
  def size: Int = stages.size

  override def toString: String = {
    val sb = new StringBuilder("ExecutionDAG(\n")
    topologicalStages.foreach { s =>
      sb.append(s"  Stage(id=${s.id}, op=${s.operator.getClass.getSimpleName}, " +
        s"deps=${s.dependencies.mkString("[", ",", "]")}, " +
        s"partitions=${s.partitionCount})\n")
    }
    sb.append(")")
    sb.toString()
  }
}

object ExecutionDAG {

  private val MinPartitionCount: Int  = 1
  private val DefaultPartitionCount: Int = 4
  private val LargeTableRowThreshold: Long = 1000000L
  private val LargeTablePartitionCount: Int = 8
  private val AggregatePartitionDivisor: Int = 2

  /** Build an [[ExecutionDAG]] from a [[PhysicalOperator]] tree.
    *
    * The algorithm walks the operator tree bottom-up. A new stage boundary
    * is created at every shuffle point (joins, aggregations) and at leaf
    * scans. Pipeline-able operators (filter, project, sort, limit) are
    * tracked as individual stages with dependencies so the coordinator
    * can schedule them.
    */
  def fromOperator(root: PhysicalOperator): ExecutionDAG = {
    val counter = new AtomicInteger(0)
    val stages  = mutable.Map.empty[Int, ExecutionStage]

    val rootId = buildStages(root, counter, stages)

    ExecutionDAG(stages.toMap, Set(rootId))
  }

  // -------------------------------------------------------------------------
  // Internal recursive builder
  // -------------------------------------------------------------------------

  private def buildStages(
    op: PhysicalOperator,
    counter: AtomicInteger,
    stages: mutable.Map[Int, ExecutionStage]
  ): Int =
    op match {
      case scan: TableScanOp =>
        val id = counter.getAndIncrement()
        val partitions = partitionCountForScan(scan)
        stages(id) = ExecutionStage(id, scan, Set.empty, partitions)
        id

      case join: HashJoinOp =>
        val leftId  = buildStages(join.left, counter, stages)
        val rightId = buildStages(join.right, counter, stages)
        val id = counter.getAndIncrement()
        val partitions = math.max(
          stages(leftId).partitionCount,
          stages(rightId).partitionCount
        )
        stages(id) = ExecutionStage(id, join, Set(leftId, rightId), partitions)
        id

      case join: NestedLoopJoinOp =>
        val leftId  = buildStages(join.left, counter, stages)
        val rightId = buildStages(join.right, counter, stages)
        val id = counter.getAndIncrement()
        val partitions = math.max(
          stages(leftId).partitionCount,
          stages(rightId).partitionCount
        )
        stages(id) = ExecutionStage(id, join, Set(leftId, rightId), partitions)
        id

      case agg: HashAggregateOp =>
        val childId = buildStages(agg.child, counter, stages)
        val id = counter.getAndIncrement()
        val partitions = math.max(
          MinPartitionCount,
          stages(childId).partitionCount / AggregatePartitionDivisor
        )
        stages(id) = ExecutionStage(id, agg, Set(childId), partitions)
        id

      case union: UnionOp =>
        val inputIds = union.inputs.map(buildStages(_, counter, stages))
        val id = counter.getAndIncrement()
        val partitions = if (inputIds.nonEmpty) {
          inputIds.map(stages(_).partitionCount).max
        } else {
          MinPartitionCount
        }
        stages(id) = ExecutionStage(id, union, inputIds.toSet, partitions)
        id

      case filter: FilterOp =>
        val childId = buildStages(filter.child, counter, stages)
        val id = counter.getAndIncrement()
        stages(id) = ExecutionStage(
          id, filter, Set(childId), stages(childId).partitionCount
        )
        id

      case proj: ProjectOp =>
        val childId = buildStages(proj.child, counter, stages)
        val id = counter.getAndIncrement()
        stages(id) = ExecutionStage(
          id, proj, Set(childId), stages(childId).partitionCount
        )
        id

      case sort: SortOp =>
        val childId = buildStages(sort.child, counter, stages)
        val id = counter.getAndIncrement()
        stages(id) = ExecutionStage(
          id, sort, Set(childId), stages(childId).partitionCount
        )
        id

      case limit: LimitOp =>
        val childId = buildStages(limit.child, counter, stages)
        val id = counter.getAndIncrement()
        stages(id) = ExecutionStage(
          id, limit, Set(childId), MinPartitionCount
        )
        id
    }

  private def partitionCountForScan(scan: TableScanOp): Int =
    if (scan.estimatedRows >= LargeTableRowThreshold) LargeTablePartitionCount
    else DefaultPartitionCount
}
