package io.gbmm.udps.governance.anonymization

import io.circe._
import io.circe.syntax._

sealed trait AnonymizationTechnique

object AnonymizationTechnique {

  final case class Masking(
      maskChar: Char = '*',
      preserveLength: Boolean = true
  ) extends AnonymizationTechnique

  final case class Hashing(
      algorithm: String = "SHA-256",
      salt: Option[String] = None
  ) extends AnonymizationTechnique

  final case class Tokenization(
      tokenPrefix: String = "TOK"
  ) extends AnonymizationTechnique

  final case class Generalization(
      ranges: Seq[(Double, Double, String)]
  ) extends AnonymizationTechnique

  final case class Pseudonymization(
      seed: Long
  ) extends AnonymizationTechnique

  final case class Perturbation(
      noiseScale: Double
  ) extends AnonymizationTechnique

  final case class Suppression() extends AnonymizationTechnique

  final case class KAnonymity(
      k: Int = 3,
      quasiIdentifiers: Seq[String]
  ) extends AnonymizationTechnique

  final case class DifferentialPrivacy(
      epsilon: Double = 0.1
  ) extends AnonymizationTechnique

  final case class FormatPreservingEncryption(
      key: String
  ) extends AnonymizationTechnique

  private val techniqueType = "technique_type"

  implicit val encodeTechnique: Encoder[AnonymizationTechnique] = Encoder.instance {
    case Masking(maskChar, preserveLength) =>
      Json.obj(
        techniqueType -> "masking".asJson,
        "mask_char" -> maskChar.toString.asJson,
        "preserve_length" -> preserveLength.asJson
      )
    case Hashing(algorithm, salt) =>
      Json.obj(
        techniqueType -> "hashing".asJson,
        "algorithm" -> algorithm.asJson,
        "salt" -> salt.asJson
      )
    case Tokenization(tokenPrefix) =>
      Json.obj(
        techniqueType -> "tokenization".asJson,
        "token_prefix" -> tokenPrefix.asJson
      )
    case Generalization(ranges) =>
      Json.obj(
        techniqueType -> "generalization".asJson,
        "ranges" -> ranges.map { case (lo, hi, label) =>
          Json.obj("low" -> lo.asJson, "high" -> hi.asJson, "label" -> label.asJson)
        }.asJson
      )
    case Pseudonymization(seed) =>
      Json.obj(
        techniqueType -> "pseudonymization".asJson,
        "seed" -> seed.asJson
      )
    case Perturbation(noiseScale) =>
      Json.obj(
        techniqueType -> "perturbation".asJson,
        "noise_scale" -> noiseScale.asJson
      )
    case Suppression() =>
      Json.obj(
        techniqueType -> "suppression".asJson
      )
    case KAnonymity(k, quasiIdentifiers) =>
      Json.obj(
        techniqueType -> "k_anonymity".asJson,
        "k" -> k.asJson,
        "quasi_identifiers" -> quasiIdentifiers.asJson
      )
    case DifferentialPrivacy(epsilon) =>
      Json.obj(
        techniqueType -> "differential_privacy".asJson,
        "epsilon" -> epsilon.asJson
      )
    case FormatPreservingEncryption(key) =>
      Json.obj(
        techniqueType -> "format_preserving_encryption".asJson,
        "key" -> key.asJson
      )
  }

  implicit val decodeTechnique: Decoder[AnonymizationTechnique] = Decoder.instance { c =>
    c.downField(techniqueType).as[String].flatMap {
      case "masking" =>
        for {
          mc <- c.downField("mask_char").as[String]
          pl <- c.downField("preserve_length").as[Boolean]
        } yield Masking(mc.headOption.getOrElse('*'), pl)
      case "hashing" =>
        for {
          alg <- c.downField("algorithm").as[String]
          salt <- c.downField("salt").as[Option[String]]
        } yield Hashing(alg, salt)
      case "tokenization" =>
        c.downField("token_prefix").as[String].map(Tokenization.apply)
      case "generalization" =>
        c.downField("ranges").as[Seq[Json]].flatMap { jsons =>
          val parsed = jsons.map { j =>
            for {
              lo <- j.hcursor.downField("low").as[Double]
              hi <- j.hcursor.downField("high").as[Double]
              label <- j.hcursor.downField("label").as[String]
            } yield (lo, hi, label)
          }
          val (errors, results) = parsed.partitionMap(identity)
          errors.headOption match {
            case Some(err) => Left(err)
            case None      => Right(Generalization(results))
          }
        }
      case "pseudonymization" =>
        c.downField("seed").as[Long].map(Pseudonymization.apply)
      case "perturbation" =>
        c.downField("noise_scale").as[Double].map(Perturbation.apply)
      case "suppression" =>
        Right(Suppression())
      case "k_anonymity" =>
        for {
          k <- c.downField("k").as[Int]
          qi <- c.downField("quasi_identifiers").as[Seq[String]]
        } yield KAnonymity(k, qi)
      case "differential_privacy" =>
        c.downField("epsilon").as[Double].map(DifferentialPrivacy.apply)
      case "format_preserving_encryption" =>
        c.downField("key").as[String].map(FormatPreservingEncryption.apply)
      case other =>
        Left(DecodingFailure(s"Unknown anonymization technique: $other", c.history))
    }
  }
}
