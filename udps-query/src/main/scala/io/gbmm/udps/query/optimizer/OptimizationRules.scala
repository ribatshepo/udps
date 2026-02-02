package io.gbmm.udps.query.optimizer

import org.apache.calcite.rel.rules.CoreRules
import org.apache.calcite.tools.{RuleSet, RuleSets}

/** Named rule sets for the UDPS logical query optimizer.
  *
  * Each set groups related Calcite built-in optimisation rules so the
  * [[LogicalOptimizer]] can apply them in well-defined phases.
  */
object OptimizationRules {

  /** Push filter predicates below joins, projections, and into table scans. */
  val PredicatePushdownRules: RuleSet = RuleSets.ofList(
    CoreRules.FILTER_INTO_JOIN,
    CoreRules.FILTER_PROJECT_TRANSPOSE,
    CoreRules.FILTER_SCAN,
    CoreRules.JOIN_CONDITION_PUSH
  )

  /** Push projections down to reduce the number of columns flowing through
    * the plan tree as early as possible.
    */
  val ProjectionPushdownRules: RuleSet = RuleSets.ofList(
    CoreRules.PROJECT_FILTER_TRANSPOSE,
    CoreRules.PROJECT_JOIN_TRANSPOSE
  )

  /** Cost-based join reordering: commutativity and associativity. */
  val JoinReorderRules: RuleSet = RuleSets.ofList(
    CoreRules.JOIN_COMMUTE,
    CoreRules.JOIN_ASSOCIATE
  )

  /** Fold / reduce constant expressions in filters, projections, calcs, and
    * join conditions.
    */
  val ConstantFoldingRules: RuleSet = RuleSets.ofList(
    CoreRules.FILTER_REDUCE_EXPRESSIONS,
    CoreRules.PROJECT_REDUCE_EXPRESSIONS,
    CoreRules.CALC_REDUCE_EXPRESSIONS,
    CoreRules.JOIN_REDUCE_EXPRESSIONS
  )

  /** Decorrelate correlated sub-queries so they can be evaluated as joins. */
  val SubqueryDecorrelationRules: RuleSet = RuleSets.ofList(
    CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
    CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
    CoreRules.JOIN_SUB_QUERY_TO_CORRELATE
  )

  /** Simplify the plan by merging adjacent operators and removing
    * redundant nodes.
    */
  val SimplificationRules: RuleSet = RuleSets.ofList(
    CoreRules.FILTER_MERGE,
    CoreRules.PROJECT_MERGE,
    CoreRules.AGGREGATE_REMOVE,
    CoreRules.PROJECT_REMOVE,
    CoreRules.SORT_REMOVE,
    CoreRules.UNION_MERGE
  )

  /** All heuristic rules combined for the [[LogicalOptimizer]] HepPlanner
    * phase.
    */
  val HeuristicRules: RuleSet = RuleSets.ofList(
    java.util.stream.Stream
      .of(
        PredicatePushdownRules,
        ProjectionPushdownRules,
        ConstantFoldingRules,
        SubqueryDecorrelationRules,
        SimplificationRules
      )
      .flatMap(rs => java.util.stream.StreamSupport.stream(rs.spliterator(), false))
      .collect(java.util.stream.Collectors.toList[org.apache.calcite.plan.RelOptRule])
  )

  /** Cost-based rules for the VolcanoPlanner phase. */
  val CostBasedRules: RuleSet = JoinReorderRules
}
