package io.gbmm.udps.query.optimizer

import org.apache.calcite.plan.{RelOptCost, RelOptCostFactory}

/** Configurable weights for the three cost dimensions.
  *
  * The weighted sum `io * ioWeight + cpu * cpuWeight + memory * memoryWeight`
  * is used for comparing alternative physical plans.
  */
final case class CostWeights(
  ioWeight: Double,
  cpuWeight: Double,
  memoryWeight: Double
)

object CostWeights {
  val Default: CostWeights = CostWeights(ioWeight = 1.0, cpuWeight = 0.1, memoryWeight = 0.5)
}

/** Three-dimensional cost representation for the UDPS query engine.
  *
  * @param io     I/O cost: bytes scanned from storage
  * @param cpu    CPU cost: rows processed, comparisons performed
  * @param memory Memory cost: hash table size for joins, sort buffers
  */
final case class UDPSCost(
  io: Double,
  cpu: Double,
  memory: Double
)(implicit weights: CostWeights) extends RelOptCost {

  private def weightedSum: Double =
    io * weights.ioWeight + cpu * weights.cpuWeight + memory * weights.memoryWeight

  override def getRows: Double = cpu
  override def getCpu: Double = cpu
  override def getIo: Double = io

  override def isInfinite: Boolean =
    io == Double.PositiveInfinity ||
      cpu == Double.PositiveInfinity ||
      memory == Double.PositiveInfinity

  override def isEqWithEpsilon(other: RelOptCost): Boolean = other match {
    case that: UDPSCost =>
      val EPSILON = 1e-6
      Math.abs(this.weightedSum - that.weightedSum) < EPSILON
    case _ => Math.abs(this.weightedSum - (other.getIo + other.getCpu)) < 1e-6
  }

  override def isLe(other: RelOptCost): Boolean = other match {
    case that: UDPSCost => this.weightedSum <= that.weightedSum
    case _              => this.weightedSum <= (other.getIo + other.getCpu)
  }

  override def isLt(other: RelOptCost): Boolean = other match {
    case that: UDPSCost => this.weightedSum < that.weightedSum
    case _              => this.weightedSum < (other.getIo + other.getCpu)
  }

  override def plus(other: RelOptCost): RelOptCost = other match {
    case that: UDPSCost =>
      UDPSCost(this.io + that.io, this.cpu + that.cpu, this.memory + that.memory)
    case _ =>
      UDPSCost(this.io + other.getIo, this.cpu + other.getCpu, this.memory)
  }

  override def minus(other: RelOptCost): RelOptCost = other match {
    case that: UDPSCost =>
      UDPSCost(this.io - that.io, this.cpu - that.cpu, this.memory - that.memory)
    case _ =>
      UDPSCost(this.io - other.getIo, this.cpu - other.getCpu, this.memory)
  }

  override def multiplyBy(factor: Double): RelOptCost =
    UDPSCost(io * factor, cpu * factor, memory * factor)

  override def divideBy(cost: RelOptCost): Double = cost match {
    case that: UDPSCost if that.weightedSum != 0.0 => this.weightedSum / that.weightedSum
    case _ if (cost.getIo + cost.getCpu) != 0.0    => this.weightedSum / (cost.getIo + cost.getCpu)
    case _                                          => Double.PositiveInfinity
  }

  override def toString: String =
    f"UDPSCost(io=$io%.2f, cpu=$cpu%.2f, mem=$memory%.2f, weighted=$weightedSum%.2f)"

  def equals(other: RelOptCost): Boolean = other match {
    case that: UDPSCost => this.io == that.io && this.cpu == that.cpu && this.memory == that.memory
    case _              => false
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: UDPSCost => this.io == that.io && this.cpu == that.cpu && this.memory == that.memory
    case _              => false
  }

  override def hashCode(): Int = {
    val HASH_PRIME = 31
    var h = java.lang.Double.hashCode(io)
    h = HASH_PRIME * h + java.lang.Double.hashCode(cpu)
    h = HASH_PRIME * h + java.lang.Double.hashCode(memory)
    h
  }
}

/** Factory for creating UDPSCost instances, registered with Calcite's VolcanoPlanner. */
final class UDPSCostFactory(val weights: CostWeights) extends RelOptCostFactory {

  private implicit val implicitWeights: CostWeights = weights

  override def makeCost(rowCount: Double, cpu: Double, io: Double): RelOptCost =
    UDPSCost(io = io, cpu = cpu, memory = 0.0)

  override def makeHugeCost(): RelOptCost =
    UDPSCost(
      io = Double.MaxValue / 4.0,
      cpu = Double.MaxValue / 4.0,
      memory = Double.MaxValue / 4.0
    )

  override def makeInfiniteCost(): RelOptCost =
    UDPSCost(
      io = Double.PositiveInfinity,
      cpu = Double.PositiveInfinity,
      memory = Double.PositiveInfinity
    )

  override def makeTinyCost(): RelOptCost =
    UDPSCost(io = 1.0, cpu = 1.0, memory = 0.0)

  override def makeZeroCost(): RelOptCost =
    UDPSCost(io = 0.0, cpu = 0.0, memory = 0.0)

  /** Create a cost with all three dimensions specified explicitly. */
  def makeFullCost(io: Double, cpu: Double, memory: Double): UDPSCost =
    UDPSCost(io = io, cpu = cpu, memory = memory)
}

object UDPSCostFactory {
  def apply(): UDPSCostFactory = new UDPSCostFactory(CostWeights.Default)
  def apply(weights: CostWeights): UDPSCostFactory = new UDPSCostFactory(weights)
}

/** Pre-built cost estimation formulas for standard relational operators. */
object CostEstimates {

  /** Cost of a full sequential table scan. */
  def fullTableScan(rowCount: Long, avgRowSize: Long)(implicit w: CostWeights): UDPSCost =
    UDPSCost(
      io = rowCount.toDouble * avgRowSize.toDouble,
      cpu = rowCount.toDouble,
      memory = 0.0
    )

  /** Cost of an index scan with a given selectivity in [0.0, 1.0]. */
  def indexScan(
    rowCount: Long,
    avgRowSize: Long,
    selectivity: Double
  )(implicit w: CostWeights): UDPSCost = {
    val selectedRows = rowCount.toDouble * selectivity
    UDPSCost(
      io = selectedRows * avgRowSize.toDouble,
      cpu = selectedRows,
      memory = 0.0
    )
  }

  /** Cost of a hash join.
    *
    * The build side is materialized into a hash table (memory),
    * both sides are fully scanned (IO), and each row from both
    * sides is processed (CPU).
    */
  def hashJoin(
    buildRows: Long,
    buildAvgRowSize: Long,
    probeRows: Long,
    probeAvgRowSize: Long
  )(implicit w: CostWeights): UDPSCost =
    UDPSCost(
      io = buildRows.toDouble * buildAvgRowSize.toDouble + probeRows.toDouble * probeAvgRowSize.toDouble,
      cpu = buildRows.toDouble + probeRows.toDouble,
      memory = buildRows.toDouble * buildAvgRowSize.toDouble
    )

  /** Cost of a nested-loop join.
    *
    * Every row in the outer relation is compared against every
    * row in the inner relation, producing O(n*m) CPU cost.
    */
  def nestedLoopJoin(
    outerRows: Long,
    outerAvgRowSize: Long,
    innerRows: Long,
    innerAvgRowSize: Long
  )(implicit w: CostWeights): UDPSCost =
    UDPSCost(
      io = outerRows.toDouble * outerAvgRowSize.toDouble + innerRows.toDouble * innerAvgRowSize.toDouble,
      cpu = outerRows.toDouble * innerRows.toDouble,
      memory = 0.0
    )

  /** Cost of an in-memory sort using O(n log n) comparisons. */
  def sort(rowCount: Long, avgRowSize: Long)(implicit w: CostWeights): UDPSCost = {
    val n = rowCount.toDouble
    val logN = if (n > 1.0) Math.log(n) / Math.log(2.0) else 1.0
    UDPSCost(
      io = 0.0,
      cpu = n * logN,
      memory = n * avgRowSize.toDouble
    )
  }

  /** Determine whether a hash join is preferred over a nested-loop join
    * based on cost comparison.
    */
  def preferHashJoin(
    buildRows: Long,
    buildAvgRowSize: Long,
    probeRows: Long,
    probeAvgRowSize: Long
  )(implicit w: CostWeights): Boolean = {
    val hashCost = hashJoin(buildRows, buildAvgRowSize, probeRows, probeAvgRowSize)
    val nlCost = nestedLoopJoin(buildRows, buildAvgRowSize, probeRows, probeAvgRowSize)
    hashCost.isLt(nlCost)
  }

  /** Determine whether an index scan is preferred over a full table scan. */
  def preferIndexScan(
    rowCount: Long,
    avgRowSize: Long,
    selectivity: Double
  )(implicit w: CostWeights): Boolean = {
    val INDEX_SELECTIVITY_THRESHOLD = 0.3
    selectivity < INDEX_SELECTIVITY_THRESHOLD && {
      val idxCost = indexScan(rowCount, avgRowSize, selectivity)
      val fullCost = fullTableScan(rowCount, avgRowSize)
      idxCost.isLt(fullCost)
    }
  }
}
