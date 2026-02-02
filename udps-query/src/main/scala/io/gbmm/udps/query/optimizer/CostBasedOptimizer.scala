package io.gbmm.udps.query.optimizer

import com.typesafe.scalalogging.LazyLogging
import org.apache.calcite.plan._
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.{BuiltInMetadata, Metadata, MetadataDef, MetadataHandler, MetadataHandlerProvider, RelMetadataQuery}
import org.apache.calcite.rel.rules._

import java.lang.{Double => JDouble}

/** Result of cost-based optimization. */
final case class CostOptimizationResult(
  optimizedPlan: RelNode,
  estimatedCost: UDPSCost,
  joinStrategies: Map[String, String],
  planDescription: String
)

/** Metadata handler that supplies row-count estimates from the StatisticsProvider.
  *
  * Registered with Calcite's metadata system so the VolcanoPlanner can query
  * cardinality estimates when computing costs.
  */
final class UDPSRelMdRowCount(
  private val statisticsProvider: StatisticsProvider
) extends MetadataHandler[BuiltInMetadata.RowCount] {

  override def getDef: MetadataDef[BuiltInMetadata.RowCount] =
    BuiltInMetadata.RowCount.DEF

  def getRowCount(rel: RelNode, mq: RelMetadataQuery): JDouble =
    rel match {
      case scan: TableScan =>
        val qualifiedName = scan.getTable.getQualifiedName
        val tableName = qualifiedName.get(qualifiedName.size() - 1)
        val namespace = if (qualifiedName.size() > 1) qualifiedName.get(0) else "default"
        statisticsProvider.getTableStatistics(tableName, namespace) match {
          case Some(stats) => JDouble.valueOf(stats.rowCount.toDouble)
          case None        => null
        }
      case _ =>
        rel.estimateRowCount(mq)
    }
}

/** Metadata handler that supplies column-distinctness estimates. */
final class UDPSRelMdColumnUniqueness(
  private val statisticsProvider: StatisticsProvider
) extends MetadataHandler[BuiltInMetadata.ColumnUniqueness] {

  override def getDef: MetadataDef[BuiltInMetadata.ColumnUniqueness] =
    BuiltInMetadata.ColumnUniqueness.DEF

  def areColumnsUnique(
    rel: RelNode,
    mq: RelMetadataQuery,
    columns: org.apache.calcite.util.ImmutableBitSet,
    ignoreNulls: Boolean
  ): java.lang.Boolean =
    rel match {
      case scan: TableScan =>
        val qualifiedName = scan.getTable.getQualifiedName
        val tableName = qualifiedName.get(qualifiedName.size() - 1)
        val namespace = if (qualifiedName.size() > 1) qualifiedName.get(0) else "default"
        statisticsProvider.getTableStatistics(tableName, namespace) match {
          case Some(tableStats) =>
            val fieldNames = scan.getRowType.getFieldNames
            val allUnique = columns.toList.stream().allMatch { colIdx =>
              val colName = fieldNames.get(colIdx)
              tableStats.columnStats.get(colName) match {
                case Some(cs) => cs.distinctCount == tableStats.rowCount
                case None     => false
              }
            }
            java.lang.Boolean.valueOf(allUnique)
          case None => null
        }
      case _ => null
    }
}

/** Metadata provider that registers UDPS-specific handlers with Calcite. */
final class UDPSMetadataProvider(
  statisticsProvider: StatisticsProvider
) extends MetadataHandlerProvider {

  private val rowCountHandler = new UDPSRelMdRowCount(statisticsProvider)
  private val columnUniquenessHandler = new UDPSRelMdColumnUniqueness(statisticsProvider)

  @SuppressWarnings(Array("unchecked"))
  override def handler[MH <: MetadataHandler[_]](handlerClass: Class[MH]): MH = {
    if (classOf[MetadataHandler[BuiltInMetadata.RowCount]].isAssignableFrom(handlerClass)) {
      rowCountHandler.asInstanceOf[MH]
    } else if (classOf[MetadataHandler[BuiltInMetadata.ColumnUniqueness]].isAssignableFrom(handlerClass)) {
      columnUniquenessHandler.asInstanceOf[MH]
    } else {
      throw new IllegalArgumentException(
        s"Unsupported metadata handler class: ${handlerClass.getSimpleName}"
      )
    }
  }

  /** The set of metadata definitions this provider handles. */
  def handledDefs: Set[MetadataDef[_ <: Metadata]] =
    Set(BuiltInMetadata.RowCount.DEF, BuiltInMetadata.ColumnUniqueness.DEF)
}

/** Cost-based optimizer that integrates the UDPS cost model and statistics
  * with Calcite's VolcanoPlanner.
  *
  * Thread-safety: Each invocation of `optimize` creates its own planner
  * instance, so multiple threads can optimize concurrently without interference.
  */
final class CostBasedOptimizer(
  statisticsProvider: StatisticsProvider,
  costWeights: CostWeights = CostWeights.Default
) extends LazyLogging {

  private val costFactory: UDPSCostFactory = UDPSCostFactory(costWeights)

  /** Metadata provider that should be registered with the RelOptCluster
    * to supply UDPS statistics to Calcite's cost model.
    */
  val metadataProvider: UDPSMetadataProvider = new UDPSMetadataProvider(statisticsProvider)

  /** The cost factory used by this optimizer, exposed for cluster configuration. */
  def getCostFactory: UDPSCostFactory = costFactory

  /** Optimize a logical plan using Calcite's VolcanoPlanner with the UDPS cost model.
    *
    * @param logicalPlan a logical RelNode tree (output of the logical optimizer)
    * @return the optimization result including the physical plan, estimated cost,
    *         and the join strategies selected
    */
  def optimize(logicalPlan: RelNode): CostOptimizationResult = {
    val cluster = logicalPlan.getCluster
    val planner = createPlanner(cluster)

    planner.setRoot(logicalPlan)
    val optimizedPlan = planner.findBestExp()

    val estimatedCost = extractCost(optimizedPlan, planner)
    val joinStrategies = collectJoinStrategies(optimizedPlan)
    val planDescription = RelOptUtil.toString(optimizedPlan)

    logger.info("Cost-based optimization complete. Estimated cost: {}", estimatedCost)
    logger.debug("Optimized plan:\n{}", planDescription)

    CostOptimizationResult(
      optimizedPlan = optimizedPlan,
      estimatedCost = estimatedCost,
      joinStrategies = joinStrategies,
      planDescription = planDescription
    )
  }

  private def createPlanner(cluster: RelOptCluster): VolcanoPlanner = {
    val planner = new VolcanoPlanner(costFactory, cluster.getPlanner.getContext)

    registerRules(planner)

    planner
  }

  /** Register transformation and implementation rules with the planner. */
  private def registerRules(planner: VolcanoPlanner): Unit = {
    planner.addRule(CoreRules.FILTER_INTO_JOIN)
    planner.addRule(CoreRules.JOIN_COMMUTE)
    planner.addRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES)
    planner.addRule(CoreRules.PROJECT_MERGE)
    planner.addRule(CoreRules.FILTER_MERGE)
    planner.addRule(CoreRules.AGGREGATE_REMOVE)
    planner.addRule(CoreRules.SORT_REMOVE)
    planner.addRule(CoreRules.JOIN_ASSOCIATE)
    planner.addRule(CoreRules.JOIN_CONDITION_PUSH)
    planner.addRule(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
    planner.addRule(CoreRules.PROJECT_FILTER_TRANSPOSE)
    val _ = planner.addRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE)
  }

  /** Extract the cost of the optimized plan as a UDPSCost. */
  private def extractCost(plan: RelNode, planner: VolcanoPlanner): UDPSCost = {
    implicit val w: CostWeights = costWeights
    val calciteCost = planner.getCost(plan, plan.getCluster.getMetadataQuery)
    calciteCost match {
      case udps: UDPSCost => udps
      case other if other != null =>
        UDPSCost(io = other.getIo, cpu = other.getCpu, memory = 0.0)
      case _ =>
        UDPSCost(io = Double.PositiveInfinity, cpu = Double.PositiveInfinity, memory = Double.PositiveInfinity)
    }
  }

  /** Walk the optimized plan tree and collect join strategies for reporting. */
  private def collectJoinStrategies(plan: RelNode): Map[String, String] = {
    val strategies = scala.collection.mutable.Map.empty[String, String]
    collectJoinStrategiesRec(plan, strategies)
    strategies.toMap
  }

  private def collectJoinStrategiesRec(
    node: RelNode,
    acc: scala.collection.mutable.Map[String, String]
  ): Unit = {
    node match {
      case join: Join =>
        val joinId = s"join_${acc.size}"
        val strategy = inferJoinStrategy(join)
        val _ = acc.put(joinId, strategy)
      case _ => ()
    }
    val inputs = node.getInputs
    val iter = inputs.iterator()
    while (iter.hasNext) {
      collectJoinStrategiesRec(iter.next(), acc)
    }
  }

  /** Infer the join strategy from the physical join operator type. */
  private def inferJoinStrategy(join: Join): String = {
    val className = join.getClass.getSimpleName.toLowerCase
    if (className.contains("hash")) "HashJoin"
    else if (className.contains("merge") || className.contains("sort")) "MergeJoin"
    else if (className.contains("nestedloop") || className.contains("nested")) "NestedLoopJoin"
    else if (className.contains("correlate")) "CorrelatedJoin"
    else s"Join(${join.getClass.getSimpleName})"
  }

  /** Recommend a join strategy based on cost estimates and statistics.
    *
    * This method can be used by custom physical rules or upstream components
    * to make join strategy decisions outside of the Calcite planner.
    */
  def recommendJoinStrategy(
    leftTableName: String,
    leftNamespace: String,
    rightTableName: String,
    rightNamespace: String
  ): String = {
    implicit val w: CostWeights = costWeights

    val leftStats = statisticsProvider.getTableStatistics(leftTableName, leftNamespace)
    val rightStats = statisticsProvider.getTableStatistics(rightTableName, rightNamespace)

    (leftStats, rightStats) match {
      case (Some(left), Some(right)) =>
        val (buildSide, probeSide) =
          if (left.rowCount <= right.rowCount) (left, right)
          else (right, left)

        if (CostEstimates.preferHashJoin(
              buildSide.rowCount, buildSide.avgRowSize,
              probeSide.rowCount, probeSide.avgRowSize
            )) "HashJoin"
        else "NestedLoopJoin"

      case _ => "HashJoin"
    }
  }

  /** Recommend a scan strategy based on cost estimates and selectivity.
    *
    * @param selectivity estimated fraction of rows matching the predicate (0.0 to 1.0)
    * @return "IndexScan" or "TableScan"
    */
  def recommendScanStrategy(
    tableName: String,
    namespace: String,
    selectivity: Double
  ): String = {
    implicit val w: CostWeights = costWeights

    statisticsProvider.getTableStatistics(tableName, namespace) match {
      case Some(stats) =>
        if (CostEstimates.preferIndexScan(stats.rowCount, stats.avgRowSize, selectivity))
          "IndexScan"
        else
          "TableScan"
      case None => "TableScan"
    }
  }
}

object CostBasedOptimizer {

  /** Create a CostBasedOptimizer with default cost weights. */
  def apply(statisticsProvider: StatisticsProvider): CostBasedOptimizer =
    new CostBasedOptimizer(statisticsProvider)

  /** Create a CostBasedOptimizer with custom cost weights. */
  def apply(
    statisticsProvider: StatisticsProvider,
    costWeights: CostWeights
  ): CostBasedOptimizer =
    new CostBasedOptimizer(statisticsProvider, costWeights)
}
