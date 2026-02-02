package io.gbmm.udps.catalog.profiling

object NumericProfiler {

  private val HistogramBucketCount = 10
  private val IqrMultiplier = 1.5

  def profile(values: Seq[Double]): NumericProfile = {
    if (values.isEmpty) {
      return NumericProfile(
        min = 0.0,
        max = 0.0,
        mean = 0.0,
        median = 0.0,
        stddev = 0.0,
        quartiles = (0.0, 0.0, 0.0),
        histogram = Seq.empty,
        nullCount = 0L,
        distinctCount = 0L,
        outlierCount = 0L
      )
    }

    val sorted = values.sorted
    val count = sorted.size
    val minVal = sorted.head
    val maxVal = sorted.last
    val sum = sorted.sum
    val meanVal = sum / count

    val varianceSum = sorted.foldLeft(0.0) { (acc, v) =>
      val diff = v - meanVal
      acc + diff * diff
    }
    val stddevVal = if (count > 1) math.sqrt(varianceSum / (count - 1)) else 0.0

    val medianVal = percentile(sorted, 0.5)
    val q1 = percentile(sorted, 0.25)
    val q2 = medianVal
    val q3 = percentile(sorted, 0.75)

    val iqr = q3 - q1
    val lowerFence = q1 - IqrMultiplier * iqr
    val upperFence = q3 + IqrMultiplier * iqr
    val outliers = sorted.count(v => v < lowerFence || v > upperFence)

    val hist = buildHistogram(sorted, minVal, maxVal)
    val distinctCount = sorted.distinct.size.toLong

    NumericProfile(
      min = minVal,
      max = maxVal,
      mean = meanVal,
      median = medianVal,
      stddev = stddevVal,
      quartiles = (q1, q2, q3),
      histogram = hist,
      nullCount = 0L,
      distinctCount = distinctCount,
      outlierCount = outliers.toLong
    )
  }

  private def percentile(sorted: Seq[Double], p: Double): Double = {
    val n = sorted.size
    if (n == 1) return sorted.head
    val rank = p * (n - 1)
    val lower = rank.toInt
    val upper = math.min(lower + 1, n - 1)
    val fraction = rank - lower
    sorted(lower) + fraction * (sorted(upper) - sorted(lower))
  }

  private def buildHistogram(sorted: Seq[Double], minVal: Double, maxVal: Double): Seq[HistogramBucket] = {
    if (minVal == maxVal) {
      return Seq(HistogramBucket(minVal, maxVal, sorted.size.toLong))
    }
    val bucketWidth = (maxVal - minVal) / HistogramBucketCount
    (0 until HistogramBucketCount).map { i =>
      val lower = minVal + i * bucketWidth
      val upper = if (i == HistogramBucketCount - 1) maxVal else minVal + (i + 1) * bucketWidth
      val cnt = if (i == HistogramBucketCount - 1) {
        sorted.count(v => v >= lower && v <= upper)
      } else {
        sorted.count(v => v >= lower && v < upper)
      }
      HistogramBucket(lower, upper, cnt.toLong)
    }
  }
}
