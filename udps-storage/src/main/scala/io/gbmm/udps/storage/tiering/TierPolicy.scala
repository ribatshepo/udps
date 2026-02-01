package io.gbmm.udps.storage.tiering

import io.gbmm.udps.core.domain.StorageTier

import java.time.Instant
import java.time.temporal.ChronoUnit

/** Metadata about a data file used for tier evaluation. */
final case class DataFileInfo(
  filePath: String,
  sizeBytes: Long,
  createdAt: Instant,
  lastAccessedAt: Instant,
  accessCountLastMonth: Int,
  currentTier: StorageTier
)

/** A rule that determines whether a file should transition to a different tier. */
sealed trait TierRule extends Product with Serializable

object TierRule {

  /** Transition files older than `maxAgeDays` to `targetTier`. */
  final case class AgeBasedRule(maxAgeDays: Int, targetTier: StorageTier) extends TierRule

  /** Transition files accessed fewer than `maxAccessesPerMonth` times to `targetTier`. */
  final case class AccessFrequencyRule(maxAccessesPerMonth: Int, targetTier: StorageTier) extends TierRule

  /** Transition files larger than `maxSizeBytes` to `targetTier`. */
  final case class SizeBasedRule(maxSizeBytes: Long, targetTier: StorageTier) extends TierRule

  /** Administrative override that forces a file to `targetTier`. */
  final case class ManualOverride(targetTier: StorageTier) extends TierRule
}

/**
 * Evaluates an ordered sequence of tier rules against file metadata to determine
 * the appropriate storage tier.
 *
 * Rules are grouped by type and evaluated in priority order:
 * ManualOverride > AgeBasedRule > AccessFrequencyRule > SizeBasedRule.
 *
 * Within each group the first matching rule wins. Across groups the highest-priority
 * (lowest numeric priority on StorageTier) match from the highest-priority rule type
 * is selected. If no rule matches the file stays in its current tier.
 */
final class TierPolicy(val rules: Seq[TierRule]) {

  import TierRule._

  /** Evaluate all rules and return the target tier for the given file. */
  def evaluate(info: DataFileInfo, now: Instant): StorageTier = {
    val manualResult    = evaluateManualOverrides(info)
    val ageResult       = evaluateAgeRules(info, now)
    val frequencyResult = evaluateFrequencyRules(info)
    val sizeResult      = evaluateSizeRules(info)

    manualResult
      .orElse(ageResult)
      .orElse(frequencyResult)
      .orElse(sizeResult)
      .getOrElse(info.currentTier)
  }

  private def evaluateManualOverrides(info: DataFileInfo): Option[StorageTier] = {
    val _ = info // present for interface consistency
    rules.collectFirst { case ManualOverride(tier) => tier }
  }

  private def evaluateAgeRules(info: DataFileInfo, now: Instant): Option[StorageTier] = {
    val ageDays = ChronoUnit.DAYS.between(info.createdAt, now)
    rules
      .collect { case r: AgeBasedRule if ageDays > r.maxAgeDays => r }
      .sortBy(_.maxAgeDays)(Ordering[Int].reverse) // longest age threshold first for most specific match
      .headOption
      .map(_.targetTier)
  }

  private def evaluateFrequencyRules(info: DataFileInfo): Option[StorageTier] =
    rules
      .collect { case r: AccessFrequencyRule if info.accessCountLastMonth < r.maxAccessesPerMonth => r }
      .sortBy(_.maxAccessesPerMonth) // strictest threshold first
      .headOption
      .map(_.targetTier)

  private def evaluateSizeRules(info: DataFileInfo): Option[StorageTier] =
    rules
      .collect { case r: SizeBasedRule if info.sizeBytes > r.maxSizeBytes => r }
      .sortBy(_.maxSizeBytes)(Ordering[Long].reverse) // largest threshold first
      .headOption
      .map(_.targetTier)
}

object TierPolicy {

  private val SevenDays: Int  = 7
  private val ThirtyDays: Int = 30
  private val NinetyDays: Int = 90
  private val LowAccessThreshold: Int = 10
  private val LargeFileSizeBytes: Long = 10L * 1024L * 1024L * 1024L // 10 GB

  /** Default policy:
   *  - age > 90d => Archive
   *  - age > 30d => Cold
   *  - age > 7d  => Warm
   *  - access < 10/month => Cold
   *  - size > 10 GB => Archive
   */
  val default: TierPolicy = new TierPolicy(Seq(
    TierRule.AgeBasedRule(NinetyDays, StorageTier.Archive),
    TierRule.AgeBasedRule(ThirtyDays, StorageTier.Cold),
    TierRule.AgeBasedRule(SevenDays, StorageTier.Warm),
    TierRule.AccessFrequencyRule(LowAccessThreshold, StorageTier.Cold),
    TierRule.SizeBasedRule(LargeFileSizeBytes, StorageTier.Archive)
  ))
}
