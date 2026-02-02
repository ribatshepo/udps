package io.gbmm.udps.catalog.quality

import io.circe._
import io.circe.syntax._

sealed trait ValidationType

object ValidationType {
  final case class RangeCheck(min: Double, max: Double) extends ValidationType
  final case class RegexCheck(pattern: String) extends ValidationType
  final case class InSetCheck(values: Set[String]) extends ValidationType

  implicit val encodeValidationType: Encoder[ValidationType] = Encoder.instance {
    case RangeCheck(min, max) =>
      Json.obj(
        "type" -> "range".asJson,
        "min" -> min.asJson,
        "max" -> max.asJson
      )
    case RegexCheck(pattern) =>
      Json.obj(
        "type" -> "regex".asJson,
        "pattern" -> pattern.asJson
      )
    case InSetCheck(values) =>
      Json.obj(
        "type" -> "in_set".asJson,
        "values" -> values.asJson
      )
  }

  implicit val decodeValidationType: Decoder[ValidationType] = Decoder.instance { c =>
    c.downField("type").as[String].flatMap {
      case "range" =>
        for {
          min <- c.downField("min").as[Double]
          max <- c.downField("max").as[Double]
        } yield RangeCheck(min, max)
      case "regex" =>
        c.downField("pattern").as[String].map(RegexCheck.apply)
      case "in_set" =>
        c.downField("values").as[Set[String]].map(InSetCheck.apply)
      case other =>
        Left(DecodingFailure(s"Unknown validation type: $other", c.history))
    }
  }
}

sealed trait QualityRule {
  def ruleName: String
  def columnName: String
}

final case class CompletenessRule(
    columnName: String,
    minNonNullPercent: Double
) extends QualityRule {
  val ruleName: String = s"completeness_$columnName"
}

final case class ValidityRule(
    columnName: String,
    validationType: ValidationType
) extends QualityRule {
  val ruleName: String = s"validity_$columnName"
}

final case class UniquenessRule(
    columnName: String
) extends QualityRule {
  val ruleName: String = s"uniqueness_$columnName"
}

final case class ConsistencyRule(
    sourceColumn: String,
    referenceTable: String,
    referenceColumn: String
) extends QualityRule {
  val ruleName: String = s"consistency_${sourceColumn}_${referenceTable}_$referenceColumn"
  val columnName: String = sourceColumn
}

object QualityRule {
  implicit val encodeQualityRule: Encoder[QualityRule] = Encoder.instance {
    case CompletenessRule(col, minPct) =>
      Json.obj(
        "rule_type" -> "completeness".asJson,
        "column_name" -> col.asJson,
        "min_non_null_percent" -> minPct.asJson
      )
    case ValidityRule(col, vt) =>
      Json.obj(
        "rule_type" -> "validity".asJson,
        "column_name" -> col.asJson,
        "validation_type" -> vt.asJson
      )
    case UniquenessRule(col) =>
      Json.obj(
        "rule_type" -> "uniqueness".asJson,
        "column_name" -> col.asJson
      )
    case ConsistencyRule(src, refTable, refCol) =>
      Json.obj(
        "rule_type" -> "consistency".asJson,
        "source_column" -> src.asJson,
        "reference_table" -> refTable.asJson,
        "reference_column" -> refCol.asJson
      )
  }

  implicit val decodeQualityRule: Decoder[QualityRule] = Decoder.instance { c =>
    c.downField("rule_type").as[String].flatMap {
      case "completeness" =>
        for {
          col <- c.downField("column_name").as[String]
          minPct <- c.downField("min_non_null_percent").as[Double]
        } yield CompletenessRule(col, minPct)
      case "validity" =>
        for {
          col <- c.downField("column_name").as[String]
          vt <- c.downField("validation_type").as[ValidationType]
        } yield ValidityRule(col, vt)
      case "uniqueness" =>
        c.downField("column_name").as[String].map(UniquenessRule.apply)
      case "consistency" =>
        for {
          src <- c.downField("source_column").as[String]
          refTable <- c.downField("reference_table").as[String]
          refCol <- c.downField("reference_column").as[String]
        } yield ConsistencyRule(src, refTable, refCol)
      case other =>
        Left(DecodingFailure(s"Unknown rule type: $other", c.history))
    }
  }
}
