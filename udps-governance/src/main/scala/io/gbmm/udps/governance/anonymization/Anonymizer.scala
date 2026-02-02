package io.gbmm.udps.governance.anonymization

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.syntax._
import io.gbmm.udps.governance.anonymization.AnonymizationTechnique._

final case class AnonymizationPolicy(
    columnName: String,
    technique: AnonymizationTechnique,
    description: Option[String]
)

object AnonymizationPolicy {

  implicit val encodePolicy: Encoder[AnonymizationPolicy] = Encoder.instance { p =>
    Json.obj(
      "column_name" -> p.columnName.asJson,
      "technique" -> p.technique.asJson,
      "description" -> p.description.asJson
    )
  }

  implicit val decodePolicy: Decoder[AnonymizationPolicy] = Decoder.instance { c =>
    for {
      col <- c.downField("column_name").as[String]
      tech <- c.downField("technique").as[AnonymizationTechnique]
      desc <- c.downField("description").as[Option[String]]
    } yield AnonymizationPolicy(col, tech, desc)
  }
}

class Anonymizer extends LazyLogging {

  private val defaultSensitivity: Double = 1.0

  def anonymize(value: String, technique: AnonymizationTechnique): String =
    technique match {
      case Masking(maskChar, preserveLength)  => applyMasking(value, maskChar, preserveLength)
      case Hashing(algorithm, salt)           => applyHashing(value, algorithm, salt)
      case Tokenization(tokenPrefix)          => applyTokenization(value, tokenPrefix)
      case Generalization(ranges)             => applyGeneralization(value, ranges)
      case Pseudonymization(seed)             => applyPseudonymization(value, seed)
      case Perturbation(noiseScale)           => applyPerturbation(value, noiseScale)
      case Suppression()                      => applySuppression()
      case KAnonymity(k, _)                  => applyKAnonymitySingle(value, k)
      case DifferentialPrivacy(epsilon)       => applyDifferentialPrivacy(value, epsilon)
      case FormatPreservingEncryption(key)    => applyFormatPreservingEncryption(value, key)
    }

  def anonymizeColumn(
      values: Seq[String],
      technique: AnonymizationTechnique
  ): Seq[String] =
    technique match {
      case ka @ KAnonymity(_, _) => applyKAnonymityBatch(values, ka)
      case other                 => values.map(v => anonymize(v, other))
    }

  def anonymizeWithPolicy(
      row: Map[String, String],
      policies: Seq[AnonymizationPolicy]
  ): Map[String, String] =
    policies.foldLeft(row) { (currentRow, policy) =>
      currentRow.get(policy.columnName) match {
        case Some(value) =>
          logger.debug(
            "Applying {} to column '{}'",
            policy.technique.getClass.getSimpleName,
            policy.columnName
          )
          currentRow.updated(policy.columnName, anonymize(value, policy.technique))
        case None =>
          logger.debug("Column '{}' not found in row, skipping", policy.columnName)
          currentRow
      }
    }

  private def applyMasking(value: String, maskChar: Char, preserveLength: Boolean): String = {
    if (value.length <= 2) {
      val maskLen = if (preserveLength) value.length else 3
      maskChar.toString * maskLen
    } else {
      val middle = maskChar.toString * (if (preserveLength) value.length - 2 else 3)
      s"${value.head}$middle${value.last}"
    }
  }

  private def applyHashing(value: String, algorithm: String, salt: Option[String]): String = {
    val input = salt.fold(value)(s => s + value)
    hashWithAlgorithm(input, algorithm)
  }

  private def applyTokenization(value: String, tokenPrefix: String): String = {
    val hash = sha256(value)
    val uuid = UUID.nameUUIDFromBytes(hash.getBytes(StandardCharsets.UTF_8))
    s"${tokenPrefix}_$uuid"
  }

  private def applyGeneralization(value: String, ranges: Seq[(Double, Double, String)]): String =
    scala.util.Try(value.toDouble).toOption match {
      case Some(numVal) =>
        ranges
          .find { case (lo, hi, _) => numVal >= lo && numVal < hi }
          .map(_._3)
          .getOrElse(value)
      case None => value
    }

  private def applyPseudonymization(value: String, seed: Long): String = {
    val hash = sha256(value).hashCode.toLong
    val combined = seed ^ hash
    val random = new java.util.Random(combined)
    val pseudonymLength = 8
    val chars = "abcdefghijklmnopqrstuvwxyz"
    val sb = new StringBuilder(pseudonymLength)
    var i = 0
    while (i < pseudonymLength) {
      val _ = sb.append(chars.charAt(random.nextInt(chars.length)))
      i += 1
    }
    sb.toString()
  }

  private def applyPerturbation(value: String, noiseScale: Double): String =
    scala.util.Try(value.toDouble).toOption match {
      case Some(numVal) =>
        val random = new java.util.Random(value.hashCode.toLong)
        val noise = random.nextGaussian() * noiseScale
        val perturbed = numVal + noise
        if (value.contains(".")) f"$perturbed%.2f"
        else math.round(perturbed).toString
      case None => value
    }

  private def applySuppression(): String = ""

  private def applyKAnonymitySingle(value: String, k: Int): String =
    scala.util.Try(value.toDouble).toOption match {
      case Some(numVal) =>
        val bucketSize = math.max(k, 1).toDouble
        val lower = (math.floor(numVal / bucketSize) * bucketSize).toLong
        val upper = lower + bucketSize.toLong
        s"$lower-$upper"
      case None =>
        if (value.length > 2) s"${value.take(1)}${"*" * (value.length - 1)}"
        else "*" * math.max(value.length, 1)
    }

  private def applyKAnonymityBatch(
      values: Seq[String],
      ka: KAnonymity
  ): Seq[String] = {
    val numericValues = values.flatMap(v => scala.util.Try(v.toDouble).toOption)
    if (numericValues.nonEmpty) {
      val bucketSize = determineBucketSize(numericValues, ka.k)
      values.map { v =>
        scala.util.Try(v.toDouble).toOption match {
          case Some(numVal) =>
            val lower = (math.floor(numVal / bucketSize) * bucketSize).toLong
            val upper = lower + bucketSize.toLong
            s"$lower-$upper"
          case None =>
            if (v.length > 2) s"${v.take(1)}${"*" * (v.length - 1)}"
            else "*" * math.max(v.length, 1)
        }
      }
    } else {
      values.map(v => applyKAnonymitySingle(v, ka.k))
    }
  }

  private def determineBucketSize(values: Seq[Double], k: Int): Double = {
    val sorted = values.sorted
    val range = sorted.last - sorted.head
    val idealBuckets = math.max(values.size / math.max(k, 1), 1)
    val rawBucket = range / idealBuckets.toDouble
    val magnitude = math.pow(10, math.floor(math.log10(math.max(rawBucket, 1.0))))
    math.max(math.ceil(rawBucket / magnitude) * magnitude, 1.0)
  }

  private def applyDifferentialPrivacy(value: String, epsilon: Double): String =
    scala.util.Try(value.toDouble).toOption match {
      case Some(numVal) =>
        val scale = defaultSensitivity / math.max(epsilon, 1e-10)
        val random = new java.util.Random(value.hashCode.toLong)
        val noise = laplaceSample(scale, random)
        val noisy = numVal + noise
        if (value.contains(".")) f"$noisy%.2f"
        else math.round(noisy).toString
      case None => value
    }

  private def applyFormatPreservingEncryption(value: String, key: String): String = {
    val keyBytes = sha256(key).getBytes(StandardCharsets.UTF_8)
    val result = new Array[Char](value.length)
    var i = 0
    while (i < value.length) {
      val ch = value.charAt(i)
      val keyByte = keyBytes(i % keyBytes.length)
      result(i) = encryptCharPreservingFormat(ch, keyByte)
      i += 1
    }
    new String(result)
  }

  private def encryptCharPreservingFormat(ch: Char, keyByte: Byte): Char = {
    val digitBase = 10
    val letterBase = 26
    val kb = (keyByte & 0xFF) // unsigned
    if (ch >= '0' && ch <= '9') {
      val offset = kb % digitBase
      val shifted = ((ch - '0') + offset) % digitBase
      (shifted + '0').toChar
    } else if (ch >= 'a' && ch <= 'z') {
      val offset = kb % letterBase
      val shifted = ((ch - 'a') + offset) % letterBase
      (shifted + 'a').toChar
    } else if (ch >= 'A' && ch <= 'Z') {
      val offset = kb % letterBase
      val shifted = ((ch - 'A') + offset) % letterBase
      (shifted + 'A').toChar
    } else {
      ch
    }
  }

  private[anonymization] def laplaceSample(
      scale: Double,
      random: java.util.Random
  ): Double = {
    val u = random.nextDouble() - 0.5
    val sign = if (u < 0) -1.0 else 1.0
    -sign * scale * math.log(1.0 - 2.0 * math.abs(u))
  }

  private[anonymization] def sha256(input: String): String =
    hashWithAlgorithm(input, "SHA-256")

  private def hashWithAlgorithm(input: String, algorithm: String): String = {
    val md = MessageDigest.getInstance(algorithm)
    val digest = md.digest(input.getBytes(StandardCharsets.UTF_8))
    digest.map(b => String.format("%02x", Byte.box(b))).mkString
  }
}
