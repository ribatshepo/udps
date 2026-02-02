package io.gbmm.udps.catalog.lineage

import scala.util.matching.Regex

/** Column-level mapping from source to target. */
final case class ColumnMapping(
  sourceTable: String,
  sourceColumn: String,
  targetColumn: String
)

/** Lineage information extracted from a SQL statement. */
final case class SqlLineageInfo(
  sourceTables: Set[String],
  targetTable: Option[String],
  columnMappings: Seq[ColumnMapping]
)

/**
 * Extracts table and column references from SQL strings using regex-based parsing.
 * Handles SELECT, INSERT INTO ... SELECT, and CREATE TABLE AS SELECT patterns.
 */
class LineageExtractor {

  private val InsertIntoPattern: Regex =
    """(?is)\bINSERT\s+INTO\s+(\S+)""".r

  private val CreateTableAsPattern: Regex =
    """(?is)\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)\s+AS\b""".r

  private val FromClausePattern: Regex =
    """(?is)\bFROM\s+([\s\S]+?)(?:\bWHERE\b|\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|\bHAVING\b|\bUNION\b|;|\z)""".r

  private val JoinPattern: Regex =
    """(?is)\bJOIN\s+(\S+)""".r

  private val SelectColumnsPattern: Regex =
    """(?is)\bSELECT\s+([\s\S]+?)\bFROM\b""".r

  private val QualifiedColumnPattern: Regex =
    """(\w+)\.(\w+)""".r

  private val AliasPattern: Regex =
    """(?i)(?:\bAS\s+)?(\w+)\s*$""".r

  def extract(sql: String): SqlLineageInfo = {
    val normalizedSql = normalizeSql(sql)
    val targetTable = extractTargetTable(normalizedSql)
    val sourceTables = extractSourceTables(normalizedSql)
    val columnMappings = extractColumnMappings(normalizedSql, sourceTables)
    SqlLineageInfo(sourceTables, targetTable, columnMappings)
  }

  private def normalizeSql(sql: String): String =
    sql.replaceAll("\\s+", " ").trim

  private def extractTargetTable(sql: String): Option[String] =
    InsertIntoPattern.findFirstMatchIn(sql).map(_.group(1).toLowerCase)
      .orElse(CreateTableAsPattern.findFirstMatchIn(sql).map(_.group(1).toLowerCase))

  private def extractSourceTables(sql: String): Set[String] = {
    val fromTables = FromClausePattern.findAllMatchIn(sql).flatMap { m =>
      val clause = m.group(1).trim
      extractTablesFromClause(clause)
    }.toSet

    val joinTables = JoinPattern.findAllMatchIn(sql).map { m =>
      cleanTableName(m.group(1))
    }.toSet

    (fromTables ++ joinTables).map(_.toLowerCase).filter(isValidTableName)
  }

  private def extractTablesFromClause(clause: String): Seq[String] = {
    val subqueryDepth = new scala.collection.mutable.ArrayBuffer[Int]()
    var depth = 0
    val cleaned = clause.flatMap { c =>
      c match {
        case '(' =>
          depth += 1
          subqueryDepth += depth
          ""
        case ')' =>
          depth -= 1
          ""
        case _ if depth > 0 => ""
        case other => String.valueOf(other)
      }
    }

    cleaned.split(",").toSeq.map(_.trim).flatMap { part =>
      val tokens = part.split("\\s+").toSeq
      tokens.headOption.map(cleanTableName)
    }.filter(isValidTableName)
  }

  private def extractColumnMappings(sql: String, sourceTables: Set[String]): Seq[ColumnMapping] =
    SelectColumnsPattern.findFirstMatchIn(sql) match {
      case Some(m) =>
        val columnsStr = m.group(1).trim
        if (columnsStr == "*") Seq.empty
        else parseColumnList(columnsStr, sourceTables)
      case None => Seq.empty
    }

  private def parseColumnList(columnsStr: String, sourceTables: Set[String]): Seq[ColumnMapping] = {
    val columns = splitColumns(columnsStr)
    columns.flatMap { col =>
      val trimmed = col.trim
      QualifiedColumnPattern.findFirstMatchIn(trimmed) match {
        case Some(qm) =>
          val table = qm.group(1).toLowerCase
          val column = qm.group(2).toLowerCase
          val alias = extractAlias(trimmed).getOrElse(column)
          if (sourceTables.contains(table) || isTableAlias(table)) {
            Some(ColumnMapping(table, column, alias))
          } else {
            None
          }
        case None =>
          None
      }
    }
  }

  private def splitColumns(columnsStr: String): Seq[String] = {
    val result = new scala.collection.mutable.ArrayBuffer[String]()
    val current = new StringBuilder
    var depth = 0
    columnsStr.foreach {
      case '(' => depth += 1; current.append('(')
      case ')' => depth -= 1; current.append(')')
      case ',' if depth == 0 =>
        result += current.toString()
        current.clear()
      case c => current.append(c)
    }
    if (current.nonEmpty) result += current.toString()
    result.toSeq
  }

  private def extractAlias(expr: String): Option[String] = {
    val parts = expr.trim.split("\\s+")
    if (parts.length >= 2) {
      val last = parts.last
      AliasPattern.findFirstMatchIn(last).map(_.group(1).toLowerCase)
    } else {
      None
    }
  }

  private def cleanTableName(name: String): String =
    name.replaceAll("[`\"\\[\\]]", "").trim

  private def isValidTableName(name: String): Boolean =
    name.nonEmpty &&
      !SqlKeywords.contains(name.toUpperCase) &&
      name.matches("[a-zA-Z_][a-zA-Z0-9_.]*")

  private def isTableAlias(name: String): Boolean =
    name.matches("[a-zA-Z_]\\w*")

  private val SqlKeywords: Set[String] = Set(
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "EXISTS",
    "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "CROSS", "ON",
    "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET",
    "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
    "CREATE", "TABLE", "AS", "DISTINCT", "ALL", "UNION",
    "CASE", "WHEN", "THEN", "ELSE", "END", "BETWEEN", "LIKE",
    "IS", "NULL", "TRUE", "FALSE", "ASC", "DESC", "WITH",
    "LATERAL", "NATURAL", "USING", "FETCH", "NEXT", "ROWS", "ONLY"
  )
}
