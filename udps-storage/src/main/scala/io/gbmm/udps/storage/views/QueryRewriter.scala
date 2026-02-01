package io.gbmm.udps.storage.views

import com.typesafe.scalalogging.LazyLogging

import scala.util.matching.Regex

/**
 * Rewrites SQL queries to read from materialized views when a suitable
 * view exists, avoiding expensive recomputation.
 *
 * This is a simplified, regex-based rewriter. Full SQL parsing and
 * logical-plan rewriting would require Apache Calcite (query module).
 */
object QueryRewriter extends LazyLogging {

  // ---- Named constants ---------------------------------------------------

  /** Pattern to extract table references after FROM / JOIN keywords. */
  private val TableReferencePattern: Regex =
    """(?i)(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)""".r

  /** Pattern to extract column names from a SELECT clause. */
  private val SelectColumnsPattern: Regex =
    """(?i)SELECT\s+(.*?)\s+FROM""".r

  /** Pattern to match individual column identifiers (possibly qualified). */
  private val ColumnIdentifierPattern: Regex =
    """([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)""".r

  // ---- Public API --------------------------------------------------------

  /**
   * Attempt to rewrite a SQL query to use a materialized view.
   *
   * Returns [[Some]] with the rewritten query if a valid, matching view
   * is found; [[None]] otherwise.
   *
   * Matching criteria:
   *   1. The view must be valid ([[ViewDefinition.isValid]]).
   *   2. The view's base tables must be a superset of the query's
   *      referenced tables.
   *   3. The view's SQL must textually contain the core of the query
   *      (simple containment heuristic).
   */
  def rewrite(sqlQuery: String, availableViews: Seq[ViewDefinition]): Option[String] = {
    val queryTables = extractTableReferences(sqlQuery)

    if (queryTables.isEmpty) {
      logger.debug("No table references found in query; skipping rewrite")
      return None
    }

    val candidates = availableViews
      .filter(_.isValid)
      .filter(v => queryTables.subsetOf(v.baseTables.map(normalizeIdentifier).toSet))
      .sortBy(_.baseTables.size)

    candidates.headOption.map { view =>
      val rewritten = rewriteQueryToView(sqlQuery, queryTables, view)
      logger.info(
        "Rewrote query to use materialized view: name={}, id={}",
        view.name,
        view.id.toString
      )
      rewritten
    }
  }

  /**
   * Find a materialized view that covers the given table and columns.
   *
   * A view "covers" the request when:
   *   1. It is valid.
   *   2. Its base tables contain the requested table name.
   *   3. Its SQL query textually references all requested columns
   *      (or selects all via `*`).
   */
  def findMatchingView(
      tableName: String,
      columns: Seq[String],
      availableViews: Seq[ViewDefinition]
  ): Option[ViewDefinition] = {
    val normalizedTable = normalizeIdentifier(tableName)
    val normalizedCols  = columns.map(normalizeIdentifier).toSet

    availableViews
      .filter(_.isValid)
      .filter(_.baseTables.map(normalizeIdentifier).contains(normalizedTable))
      .find { view =>
        val viewSqlLower = view.sqlQuery.toLowerCase
        val selectsAll   = viewSqlLower.contains("select *") || viewSqlLower.contains("select  *")
        selectsAll || normalizedCols.forall(col => viewSqlLower.contains(col))
      }
  }

  // ---- Internal ----------------------------------------------------------

  /**
   * Extract all table references from a SQL query using regex.
   * Returns a set of normalised (lower-case) table names.
   */
  private[views] def extractTableReferences(sql: String): Set[String] =
    TableReferencePattern
      .findAllMatchIn(sql)
      .map(m => normalizeIdentifier(m.group(1)))
      .toSet

  /**
   * Extract column names from the SELECT clause of a SQL query.
   * Returns a set of normalised (lower-case) column names.
   */
  private[views] def extractSelectColumns(sql: String): Set[String] =
    SelectColumnsPattern.findFirstMatchIn(sql) match {
      case Some(m) =>
        val columnsRaw = m.group(1).trim
        if (columnsRaw == "*") Set("*")
        else
          ColumnIdentifierPattern
            .findAllMatchIn(columnsRaw)
            .map(cm => normalizeIdentifier(cm.group(1)))
            .toSet
      case None => Set.empty
    }

  /**
   * Rewrite the original query to read from the view's Parquet data path
   * instead of the original base tables.
   *
   * Strategy: replace each referenced table with the view's data path
   * wrapped in a `parquet_scan` function call (a convention for the
   * query engine to load Parquet files directly).
   */
  private def rewriteQueryToView(
      sql: String,
      queryTables: Set[String],
      view: ViewDefinition
  ): String = {
    val viewAlias = s"__mv_${sanitizeIdentifier(view.name)}"
    val parquetScan = s"parquet_scan('${view.dataPath}') AS $viewAlias"

    var rewritten = sql
    queryTables.foreach { table =>
      val tablePattern = s"(?i)(?<=FROM\\s|JOIN\\s)$table(?=\\s|$$|,|;)".r
      val replaced = tablePattern.replaceFirstIn(rewritten, parquetScan)
      if (replaced != rewritten) {
        rewritten = replaced
        // Replace subsequent occurrences of the same table with just the alias
        val subsequentPattern = s"(?i)(?<=FROM\\s|JOIN\\s)$table(?=\\s|$$|,|;)".r
        rewritten = subsequentPattern.replaceAllIn(rewritten, viewAlias)
      }
    }
    rewritten
  }

  /** Lower-case and trim an identifier for comparison. */
  private def normalizeIdentifier(id: String): String =
    id.trim.toLowerCase

  /**
   * Sanitize a view name for use as a SQL alias by replacing
   * non-alphanumeric characters with underscores.
   */
  private def sanitizeIdentifier(name: String): String =
    name.replaceAll("[^a-zA-Z0-9_]", "_")
}
