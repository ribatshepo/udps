package io.gbmm.udps.query.optimizer

import cats.effect.{Clock, IO}
import com.typesafe.scalalogging.LazyLogging
import org.apache.calcite.config.{CalciteConnectionConfigImpl, Lex}
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.{RelOptCluster, RelOptUtil}
import org.apache.calcite.plan.hep.{HepMatchOrder, HepPlanner, HepProgramBuilder}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataTypeFactory, RelDataTypeSystem}
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.`type`.SqlTypeFactoryImpl
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.validate.{SqlValidator, SqlValidatorUtil}
import org.apache.calcite.sql2rel.{SqlToRelConverter, StandardConvertletTable}
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, RuleSet}

import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._

/** Result produced by the [[LogicalOptimizer]] after applying rule-based
  * transformations to a relational plan.
  *
  * @param originalPlan   the un-optimised relational plan converted from SQL
  * @param optimizedPlan  the plan after heuristic and cost-based optimization
  * @param appliedRules   names of the rule sets that were applied
  * @param planningTimeMs wall-clock milliseconds spent on optimization
  */
final case class OptimizationResult(
    originalPlan: RelNode,
    optimizedPlan: RelNode,
    appliedRules: Seq[String],
    planningTimeMs: Long
)

/** Configuration knobs for [[LogicalOptimizer]].
  *
  * @param enablePredicatePushdown  apply predicate pushdown heuristics
  * @param enableProjectionPushdown apply projection pushdown heuristics
  * @param enableConstantFolding    fold constant expressions
  * @param enableSimplification     merge / remove redundant operators
  * @param enableSubqueryDecorrelation decorrelate correlated sub-queries
  * @param enableJoinReorder        join reordering (commutativity / associativity)
  * @param maxPlanningIterations    upper bound on planner iterations for join reordering
  */
final case class OptimizerConfig(
    enablePredicatePushdown: Boolean = true,
    enableProjectionPushdown: Boolean = true,
    enableConstantFolding: Boolean = true,
    enableSimplification: Boolean = true,
    enableSubqueryDecorrelation: Boolean = true,
    enableJoinReorder: Boolean = true,
    maxPlanningIterations: Int = 5000
)

object OptimizerConfig {
  val Default: OptimizerConfig = OptimizerConfig()
}

private[optimizer] final case class PhaseResult(plan: RelNode, ruleNames: Seq[String])

/** Logical query optimizer that converts a validated [[SqlNode]] into an
  * optimised [[RelNode]] using Apache Calcite's planning infrastructure.
  *
  * The pipeline is:
  *   1. SQL validation
  *   2. SqlNode to RelNode conversion (SqlToRelConverter)
  *   3. Heuristic phase (HepPlanner, bottom-up)
  *   4. Join reordering phase (HepPlanner with commutativity / associativity)
  *
  * Instances are thread-safe; all mutable planner state is created per
  * invocation.
  */
final class LogicalOptimizer private (
    frameworkConfig: FrameworkConfig,
    config: OptimizerConfig
) extends LazyLogging {

  private val typeFactory: RelDataTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)
  private val rexBuilder: RexBuilder = new RexBuilder(typeFactory)

  // ---- public API ----------------------------------------------------------

  /** Optimise a parsed [[SqlNode]].
    *
    * The method validates the SQL, converts it to a relational algebra tree,
    * then applies the configured heuristic and cost-based rule sets.
    *
    * @return an [[OptimizationResult]] wrapped in [[IO]], or a failed [[IO]]
    *         if validation / conversion fails.
    */
  def optimize(sqlNode: SqlNode): IO[OptimizationResult] =
    for {
      startNanos <- Clock[IO].monotonic.map(_.toNanos)
      validator  <- IO.delay(createValidator())
      validated  <- IO.delay(validator.validate(sqlNode))
      converter  <- IO.delay(createSqlToRelConverter(validator))
      rawRel     <- IO.delay(converter.convertQuery(validated, false, true).rel)
      _          <- IO.delay(logger.debug(s"Original plan:\n${RelOptUtil.toString(rawRel)}"))
      heurResult <- applyHeuristicPhase(rawRel)
      costResult <- applyCostBasedPhase(heurResult.plan)
      endNanos   <- Clock[IO].monotonic.map(_.toNanos)
      elapsed     = NANOSECONDS.toMillis(endNanos - startNanos)
      appliedSets = heurResult.ruleNames ++ costResult.ruleNames
      _          <- IO.delay(logger.info(s"Optimization completed in ${elapsed}ms, applied: ${appliedSets.mkString(", ")}"))
    } yield OptimizationResult(
      originalPlan = rawRel,
      optimizedPlan = costResult.plan,
      appliedRules = appliedSets,
      planningTimeMs = elapsed
    )

  /** Produce a human-readable explain plan for the given [[RelNode]]. */
  def explain(relNode: RelNode): String =
    RelOptUtil.toString(relNode)

  /** Produce a human-readable explain plan for the given [[SqlNode]] after
    * full optimization.
    */
  def explainOptimized(sqlNode: SqlNode): IO[String] =
    optimize(sqlNode).map(r => explain(r.optimizedPlan))

  // ---- internals -----------------------------------------------------------


  private def applyHeuristicPhase(rel: RelNode): IO[PhaseResult] = IO.delay {
    val selectedRules = collectHeuristicRules()
    if (selectedRules.isEmpty) {
      PhaseResult(rel, Seq.empty)
    } else {
      val programBuilder = new HepProgramBuilder()
      programBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP)
      selectedRules.foreach { case (_, ruleSet) =>
        ruleSet.asScala.foreach(rule => programBuilder.addRuleInstance(rule))
      }
      val hepPlanner = new HepPlanner(programBuilder.build())
      hepPlanner.setRoot(rel)
      val optimized = hepPlanner.findBestExp()
      val names = selectedRules.map(_._1)
      logger.debug(s"Heuristic phase applied: ${names.mkString(", ")}")
      PhaseResult(optimized, names)
    }
  }

  private def applyCostBasedPhase(rel: RelNode): IO[PhaseResult] = IO.delay {
    if (!config.enableJoinReorder) {
      PhaseResult(rel, Seq.empty)
    } else {
      val programBuilder = new HepProgramBuilder()
      programBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP)
      programBuilder.addMatchLimit(config.maxPlanningIterations)
      OptimizationRules.CostBasedRules.asScala.foreach { rule =>
        programBuilder.addRuleInstance(rule)
      }
      val hepPlanner = new HepPlanner(programBuilder.build())
      hepPlanner.setRoot(rel)
      val optimized = hepPlanner.findBestExp()
      logger.debug("Cost-based phase applied: JoinReorderRules")
      PhaseResult(optimized, Seq("JoinReorderRules"))
    }
  }

  private def collectHeuristicRules(): Seq[(String, RuleSet)] = {
    val builder = Seq.newBuilder[(String, RuleSet)]
    if (config.enablePredicatePushdown)
      builder += "PredicatePushdownRules" -> OptimizationRules.PredicatePushdownRules
    if (config.enableProjectionPushdown)
      builder += "ProjectionPushdownRules" -> OptimizationRules.ProjectionPushdownRules
    if (config.enableConstantFolding)
      builder += "ConstantFoldingRules" -> OptimizationRules.ConstantFoldingRules
    if (config.enableSubqueryDecorrelation)
      builder += "SubqueryDecorrelationRules" -> OptimizationRules.SubqueryDecorrelationRules
    if (config.enableSimplification)
      builder += "SimplificationRules" -> OptimizationRules.SimplificationRules
    builder.result()
  }

  private def createValidator(): SqlValidator = {
    val schemaPlus = frameworkConfig.getDefaultSchema
    val calciteSchema = CalciteSchema.from(schemaPlus)
    val connectionProperties = new Properties()
    val calciteConfig = new CalciteConnectionConfigImpl(connectionProperties)
    val catalogReader = new CalciteCatalogReader(
      calciteSchema,
      Collections.singletonList(schemaPlus.getName),
      typeFactory,
      calciteConfig
    )
    SqlValidatorUtil.newValidator(
      SqlStdOperatorTable.instance(),
      catalogReader,
      typeFactory,
      SqlValidator.Config.DEFAULT
        .withIdentifierExpansion(true)
    )
  }

  private def createSqlToRelConverter(validator: SqlValidator): SqlToRelConverter = {
    val schemaPlus = frameworkConfig.getDefaultSchema
    val calciteSchema = CalciteSchema.from(schemaPlus)
    val connectionProperties = new Properties()
    val calciteConfig = new CalciteConnectionConfigImpl(connectionProperties)
    val catalogReader = new CalciteCatalogReader(
      calciteSchema,
      Collections.singletonList(schemaPlus.getName),
      typeFactory,
      calciteConfig
    )

    val cluster = RelOptCluster.create(
      new VolcanoPlanner(),
      rexBuilder
    )
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE)

    new SqlToRelConverter(
      null,           // view expander (not needed for direct SQL)
      validator,
      catalogReader,
      cluster,
      StandardConvertletTable.INSTANCE,
      SqlToRelConverter.config()
        .withExpand(true)
        .withTrimUnusedFields(true)
    )
  }

  private val NANOSECONDS = java.util.concurrent.TimeUnit.NANOSECONDS
}

object LogicalOptimizer {

  /** Create a [[LogicalOptimizer]] using the given Calcite schema.
    *
    * @param schemaPlus the root schema containing table definitions
    * @param config     optimizer configuration (defaults apply if omitted)
    */
  def create(schemaPlus: SchemaPlus, config: OptimizerConfig = OptimizerConfig.Default): LogicalOptimizer = {
    val frameworkConfig: FrameworkConfig = Frameworks
      .newConfigBuilder()
      .defaultSchema(schemaPlus)
      .parserConfig(
        SqlParser.config()
          .withLex(Lex.ORACLE)
          .withCaseSensitive(false)
      )
      .build()
    new LogicalOptimizer(frameworkConfig, config)
  }

  /** Create a [[LogicalOptimizer]] from an existing [[FrameworkConfig]]. */
  def create(frameworkConfig: FrameworkConfig): LogicalOptimizer =
    new LogicalOptimizer(frameworkConfig, OptimizerConfig.Default)

  /** Create a [[LogicalOptimizer]] from an existing [[FrameworkConfig]] with
    * custom configuration.
    */
  def create(frameworkConfig: FrameworkConfig, config: OptimizerConfig): LogicalOptimizer =
    new LogicalOptimizer(frameworkConfig, config)
}
