package io.gbmm.udps.governance.classification

import com.typesafe.scalalogging.LazyLogging

/**
 * Result from the ML-based classifier.
 *
 * @param category     predicted PII category
 * @param confidence   prediction confidence between 0.0 and 1.0
 * @param modelVersion version identifier for the model/heuristic used
 */
final case class MLClassificationResult(
    category: PIICategory,
    confidence: Double,
    modelVersion: String
)

/**
 * Feature vector extracted from column data for heuristic classification.
 *
 * @param avgLength       average string length of sample values
 * @param maxLength       maximum string length observed
 * @param digitRatio      ratio of digit characters to total characters
 * @param alphaRatio      ratio of alphabetic characters to total characters
 * @param specialCharRatio ratio of non-alphanumeric characters to total characters
 * @param hasDelimiters   whether values contain common delimiters (-, /, .)
 * @param uniqueRatio     ratio of unique values to total values
 */
final case class FeatureVector(
    avgLength: Double,
    maxLength: Int,
    digitRatio: Double,
    alphaRatio: Double,
    specialCharRatio: Double,
    hasDelimiters: Boolean,
    uniqueRatio: Double
)

/**
 * Feature-based heuristic classifier that scores columns against PII categories
 * using extracted statistical features from column names and sample data.
 *
 * @param confidenceThreshold minimum confidence to return a prediction
 */
final class MLClassifier(
    confidenceThreshold: Double = MLClassifier.defaultConfidenceThreshold
) extends LazyLogging {

  val modelVersion: String = "heuristic-v1"

  /**
   * Predict the most likely PII category for a column based on features.
   *
   * @param columnName   column name for name-based feature extraction
   * @param sampleValues sample data values for statistical analysis
   * @return prediction if confidence exceeds threshold, None otherwise
   */
  def predict(
      columnName: String,
      sampleValues: Seq[String]
  ): Option[MLClassificationResult] = {
    if (sampleValues.isEmpty) return None

    val features = extractFeatures(sampleValues)
    val nameTokens = tokenizeColumnName(columnName)
    val scores = PIICategory.all.map { category =>
      val score = scoreCategory(category, features, nameTokens)
      (category, score)
    }

    val (bestCategory, bestScore) = scores.maxBy(_._2)
    if (bestScore >= confidenceThreshold) {
      Some(MLClassificationResult(
        category = bestCategory,
        confidence = math.min(bestScore, 1.0),
        modelVersion = modelVersion
      ))
    } else {
      None
    }
  }

  /**
   * Predict with fallback to regex classifier when ML confidence is below threshold.
   *
   * @param columnName       column name
   * @param sampleValues     sample data values
   * @param regexClassifier  fallback regex-based classifier
   * @return classification results from ML or regex fallback
   */
  def predictWithFallback(
      columnName: String,
      sampleValues: Seq[String],
      regexClassifier: PIIClassifier
  ): Seq[ClassificationResult] =
    predict(columnName, sampleValues) match {
      case Some(mlResult) =>
        Seq(ClassificationResult(
          columnName = columnName,
          category = mlResult.category,
          confidence = mlResult.confidence,
          source = s"ml_${mlResult.modelVersion}",
          sampleMatches = sampleValues.length
        ))
      case None =>
        regexClassifier.classifyColumn(columnName, sampleValues)
    }

  /** Extract a feature vector from sample values. */
  private[classification] def extractFeatures(
      samples: Seq[String]
  ): FeatureVector = {
    val nonEmpty = samples.filter(s => s != null && s.nonEmpty)
    if (nonEmpty.isEmpty) {
      return FeatureVector(
        avgLength = 0.0,
        maxLength = 0,
        digitRatio = 0.0,
        alphaRatio = 0.0,
        specialCharRatio = 0.0,
        hasDelimiters = false,
        uniqueRatio = 0.0
      )
    }

    val lengths = nonEmpty.map(_.length.toDouble)
    val totalChars = nonEmpty.map(_.length).sum.toDouble
    val digitCount = nonEmpty.map(_.count(_.isDigit)).sum.toDouble
    val alphaCount = nonEmpty.map(_.count(_.isLetter)).sum.toDouble
    val delimiterChars = Set('-', '/', '.', ' ', '_')
    val hasDelims = nonEmpty.exists(s => s.exists(delimiterChars.contains))
    val uniqueCount = nonEmpty.distinct.length.toDouble

    val safeTotal = if (totalChars > 0) totalChars else 1.0

    FeatureVector(
      avgLength = lengths.sum / lengths.length,
      maxLength = lengths.max.toInt,
      digitRatio = digitCount / safeTotal,
      alphaRatio = alphaCount / safeTotal,
      specialCharRatio = (safeTotal - digitCount - alphaCount).max(0) / safeTotal,
      hasDelimiters = hasDelims,
      uniqueRatio = uniqueCount / nonEmpty.length.toDouble
    )
  }

  /** Tokenize a column name into lowercase tokens. */
  private def tokenizeColumnName(name: String): Set[String] = {
    val withSeparators = name.replaceAll("([a-z])([A-Z])", "$1_$2")
    withSeparators
      .toLowerCase
      .split("[^a-z0-9]+")
      .filter(_.nonEmpty)
      .toSet
  }

  /** Score how well features match a given PII category using weighted heuristics. */
  private def scoreCategory(
      category: PIICategory,
      features: FeatureVector,
      nameTokens: Set[String]
  ): Double = {
    val nameScore = scoreName(category, nameTokens)
    val featureScore = scoreFeatures(category, features)
    val nameWeight = 0.4
    val featureWeight = 0.6
    nameScore * nameWeight + featureScore * featureWeight
  }

  /** Score column name tokens against category keywords. */
  private def scoreName(
      category: PIICategory,
      tokens: Set[String]
  ): Double = {
    val keywords = categoryKeywords(category)
    if (keywords.isEmpty) return 0.0
    val matchCount = keywords.count(kw => tokens.exists(_.contains(kw)))
    matchCount.toDouble / keywords.size.toDouble
  }

  /** Score feature vector against expected characteristics of a PII category. */
  private def scoreFeatures(
      category: PIICategory,
      features: FeatureVector
  ): Double =
    category match {
      case PIICategory.SSN =>
        digitHeavyScore(features, expectedLength = 11.0, lengthTolerance = 2.0) *
          delimiterBonus(features)
      case PIICategory.CreditCard =>
        digitHeavyScore(features, expectedLength = 19.0, lengthTolerance = 4.0) *
          delimiterBonus(features)
      case PIICategory.Email =>
        val atSignScore = if (features.specialCharRatio > 0.02) 0.8 else 0.2
        val lengthScore = lengthProximity(features.avgLength, 24.0, 15.0)
        atSignScore * 0.6 + lengthScore * 0.4
      case PIICategory.Phone =>
        digitHeavyScore(features, expectedLength = 12.0, lengthTolerance = 4.0)
      case PIICategory.IPAddress =>
        val digitScore = if (features.digitRatio > 0.5) 0.7 else 0.2
        val delimScore = if (features.hasDelimiters) 0.8 else 0.3
        val lengthScore = lengthProximity(features.avgLength, 13.0, 5.0)
        digitScore * 0.4 + delimScore * 0.3 + lengthScore * 0.3
      case PIICategory.Passport =>
        val mixedScore = if (features.digitRatio > 0.5 && features.alphaRatio > 0.05) 0.7 else 0.2
        val lengthScore = lengthProximity(features.avgLength, 9.0, 3.0)
        mixedScore * 0.5 + lengthScore * 0.5
      case PIICategory.DriverLicense =>
        val mixedScore = if (features.digitRatio > 0.4 && features.alphaRatio > 0.05) 0.6 else 0.2
        val lengthScore = lengthProximity(features.avgLength, 10.0, 5.0)
        mixedScore * 0.5 + lengthScore * 0.5
      case PIICategory.DateOfBirth =>
        digitHeavyScore(features, expectedLength = 10.0, lengthTolerance = 2.0) *
          delimiterBonus(features)
      case PIICategory.HomeAddress =>
        val alphaScore = if (features.alphaRatio > 0.5) 0.7 else 0.3
        val lengthScore = if (features.avgLength > 15.0) 0.7 else 0.3
        val uniqueScore = if (features.uniqueRatio > 0.8) 0.7 else 0.3
        alphaScore * 0.3 + lengthScore * 0.4 + uniqueScore * 0.3
      case PIICategory.BiometricData =>
        val binaryScore = if (features.specialCharRatio > 0.3) 0.6 else 0.2
        val lengthScore = if (features.avgLength > 50.0) 0.6 else 0.2
        binaryScore * 0.5 + lengthScore * 0.5
      case PIICategory.FinancialAccount =>
        digitHeavyScore(features, expectedLength = 12.0, lengthTolerance = 5.0)
      case PIICategory.MedicalRecord =>
        val mixedScore = if (features.digitRatio > 0.3 && features.alphaRatio > 0.1) 0.5 else 0.2
        val lengthScore = lengthProximity(features.avgLength, 10.0, 5.0)
        mixedScore * 0.5 + lengthScore * 0.5
      case PIICategory.Username =>
        val alphaScore = if (features.alphaRatio > 0.6) 0.6 else 0.3
        val lengthScore = lengthProximity(features.avgLength, 12.0, 8.0)
        val uniqueScore = if (features.uniqueRatio > 0.9) 0.7 else 0.3
        alphaScore * 0.3 + lengthScore * 0.3 + uniqueScore * 0.4
      case PIICategory.Password =>
        val mixedScore = if (features.specialCharRatio > 0.1 && features.digitRatio > 0.1) 0.7 else 0.2
        val uniqueScore = if (features.uniqueRatio > 0.95) 0.7 else 0.3
        mixedScore * 0.5 + uniqueScore * 0.5
      case PIICategory.APIKey =>
        val lengthScore = if (features.avgLength > 20.0) 0.7 else 0.2
        val mixedScore = if (features.alphaRatio > 0.3 && features.digitRatio > 0.1) 0.6 else 0.2
        val uniqueScore = if (features.uniqueRatio > 0.95) 0.7 else 0.3
        lengthScore * 0.4 + mixedScore * 0.3 + uniqueScore * 0.3
      case PIICategory.JWTToken =>
        val lengthScore = if (features.avgLength > 50.0) 0.8 else 0.1
        val dotScore = if (features.hasDelimiters) 0.7 else 0.1
        val alphaScore = if (features.alphaRatio > 0.5) 0.6 else 0.2
        lengthScore * 0.4 + dotScore * 0.3 + alphaScore * 0.3
    }

  /** Score for digit-heavy values with expected length. */
  private def digitHeavyScore(
      features: FeatureVector,
      expectedLength: Double,
      lengthTolerance: Double
  ): Double = {
    val digitScore = if (features.digitRatio > 0.6) 0.8 else if (features.digitRatio > 0.3) 0.4 else 0.1
    val lengthScore = lengthProximity(features.avgLength, expectedLength, lengthTolerance)
    digitScore * 0.5 + lengthScore * 0.5
  }

  /** Bonus multiplier when delimiters are present. */
  private def delimiterBonus(features: FeatureVector): Double =
    if (features.hasDelimiters) 1.0 else 0.7

  /** Gaussian-like proximity score for average length. */
  private def lengthProximity(
      actual: Double,
      expected: Double,
      tolerance: Double
  ): Double = {
    val diff = math.abs(actual - expected)
    val safeTolerance = if (tolerance > 0) tolerance else 1.0
    math.exp(-(diff * diff) / (2.0 * safeTolerance * safeTolerance))
  }

  /** Keywords associated with each PII category for name-based scoring. */
  private def categoryKeywords(category: PIICategory): Set[String] =
    category match {
      case PIICategory.SSN => Set("ssn", "social", "security")
      case PIICategory.CreditCard => Set("credit", "card", "cc", "pan")
      case PIICategory.Email => Set("email", "mail", "address")
      case PIICategory.Phone => Set("phone", "tel", "mobile", "cell", "fax")
      case PIICategory.IPAddress => Set("ip", "address", "addr", "host")
      case PIICategory.Passport => Set("passport", "travel", "document")
      case PIICategory.DriverLicense => Set("driver", "license", "dl", "licence")
      case PIICategory.DateOfBirth => Set("birth", "dob", "date", "born")
      case PIICategory.HomeAddress => Set("address", "street", "home", "mailing", "postal")
      case PIICategory.BiometricData => Set("biometric", "fingerprint", "retina", "iris", "face")
      case PIICategory.FinancialAccount => Set("account", "bank", "routing", "iban", "swift")
      case PIICategory.MedicalRecord => Set("medical", "mrn", "patient", "health", "record")
      case PIICategory.Username => Set("user", "username", "login", "account")
      case PIICategory.Password => Set("password", "passwd", "pwd", "secret", "credential")
      case PIICategory.APIKey => Set("api", "key", "access", "token", "secret")
      case PIICategory.JWTToken => Set("jwt", "token", "bearer", "auth")
    }
}

object MLClassifier {
  private val defaultConfidenceThreshold = 0.8

  /** Create a classifier with the default confidence threshold. */
  def default: MLClassifier = new MLClassifier()

  /** Create a classifier with a custom confidence threshold. */
  def withThreshold(threshold: Double): MLClassifier =
    new MLClassifier(confidenceThreshold = threshold)
}
