package io.gbmm.udps.governance.security

import java.security.MessageDigest
import java.util.Base64

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging

final case class EnforcementResult(
    allowedColumns: Set[String],
    maskedColumns: Map[String, String],
    deniedColumns: Set[String]
)

class PolicyEnforcer(store: ColumnPolicyStore) extends LazyLogging {

  def enforce(
      tableName: String,
      requestedColumns: Seq[String],
      userId: String,
      userRoles: Seq[String]
  ): IO[EnforcementResult] =
    for {
      policies <- store.listForTable(tableName)
      result = computeEnforcement(policies, requestedColumns, userId, userRoles)
      _ <- IO(logger.debug(
        s"Enforcement for table=$tableName, user=$userId: " +
          s"allowed=${result.allowedColumns.size}, masked=${result.maskedColumns.size}, denied=${result.deniedColumns.size}"
      ))
    } yield result

  def rewriteQuery(
      sql: String,
      userId: String,
      userRoles: Seq[String],
      tableColumns: Map[String, Seq[String]]
  ): IO[String] =
    for {
      allEnforcements <- tableColumns.toList.traverse { case (table, cols) =>
        enforce(table, cols, userId, userRoles).map(table -> _)
      }
      enforcementMap = allEnforcements.toMap
    } yield applyRewrite(sql, enforcementMap, tableColumns)

  def applyMasking(value: String, technique: String): String =
    technique.toLowerCase match {
      case "mask" =>
        maskMiddleCharacters(value)
      case "hash" =>
        hashValue(value)
      case "null" =>
        "NULL"
      case "redact" =>
        "[REDACTED]"
      case "partial" =>
        partialMask(value)
      case other =>
        logger.warn(s"Unknown masking technique: $other, defaulting to full mask")
        maskMiddleCharacters(value)
    }

  private def computeEnforcement(
      policies: Seq[ColumnPolicy],
      requestedColumns: Seq[String],
      userId: String,
      userRoles: Seq[String]
  ): EnforcementResult = {
    val identifiers = userId +: userRoles

    val columnAccess: Map[String, (AccessLevel, Option[String])] = requestedColumns.map { col =>
      val matchingPolicies = policies.filter { p =>
        p.columnName == col && identifiers.contains(p.roleOrUser)
      }
      val bestAccess = if (matchingPolicies.isEmpty) {
        (AccessLevel.NoAccess: AccessLevel, Option.empty[String])
      } else {
        val mostPermissive = matchingPolicies.maxBy(p => AccessLevel.permissiveness(p.accessLevel))
        (mostPermissive.accessLevel, mostPermissive.maskingTechnique)
      }
      col -> bestAccess
    }.toMap

    val allowed = columnAccess.collect { case (col, (AccessLevel.FullAccess, _)) => col }.toSet
    val masked = columnAccess.collect { case (col, (AccessLevel.MaskedAccess, technique)) =>
      col -> technique.getOrElse("mask")
    }
    val denied = columnAccess.collect { case (col, (AccessLevel.NoAccess, _)) => col }.toSet

    EnforcementResult(allowed, masked, denied)
  }

  private def applyRewrite(
      sql: String,
      enforcements: Map[String, EnforcementResult],
      tableColumns: Map[String, Seq[String]]
  ): String = {
    var rewritten = sql
    enforcements.foreach { case (table, result) =>
      val cols = tableColumns.getOrElse(table, Seq.empty)
      cols.foreach { col =>
        if (result.deniedColumns.contains(col)) {
          rewritten = replaceColumnInSelect(rewritten, col, s"NULL AS $col")
        } else if (result.maskedColumns.contains(col)) {
          val technique = result.maskedColumns(col)
          rewritten = replaceColumnInSelect(rewritten, col, s"mask_$technique($col) AS $col")
        }
      }
    }
    rewritten
  }

  private def replaceColumnInSelect(sql: String, column: String, replacement: String): String = {
    val pattern = s"(?i)(?<=SELECT\\s|,\\s|,)\\s*\\b${java.util.regex.Pattern.quote(column)}\\b(?=\\s*,|\\s+FROM)"
    sql.replaceAll(pattern, s" $replacement")
  }

  private val maskVisibleChars = 2

  private def maskMiddleCharacters(value: String): String = {
    if (value.length <= maskVisibleChars * 2) {
      "*" * value.length
    } else {
      val prefix = value.take(maskVisibleChars)
      val suffix = value.takeRight(maskVisibleChars)
      val maskedLength = value.length - (maskVisibleChars * 2)
      s"$prefix${"*" * maskedLength}$suffix"
    }
  }

  private val shaAlgorithm = "SHA-256"

  private def hashValue(value: String): String = {
    val digest = MessageDigest.getInstance(shaAlgorithm)
    Base64.getEncoder.encodeToString(digest.digest(value.getBytes("UTF-8")))
  }

  private val partialVisibleChars = 4

  private def partialMask(value: String): String = {
    if (value.length <= partialVisibleChars) {
      "*" * value.length
    } else {
      value.take(partialVisibleChars) + ("*" * (value.length - partialVisibleChars))
    }
  }
}
