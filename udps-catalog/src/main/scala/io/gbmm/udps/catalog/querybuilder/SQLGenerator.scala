package io.gbmm.udps.catalog.querybuilder

object SQLGenerator {

  private val IdentifierPattern = "^[a-zA-Z_][a-zA-Z0-9_]*$".r

  def generate(spec: QuerySpec): String = {
    val sb = new StringBuilder()
    sb.append(buildSelect(spec.columns))
    sb.append(buildFrom(spec.tables))
    sb.append(buildJoins(spec.joins))
    sb.append(buildWhere(spec.filters))
    sb.append(buildGroupBy(spec.groupBy))
    sb.append(buildOrderBy(spec.orderBy))
    spec.limit.foreach { n =>
      sb.append(s" LIMIT $n")
    }
    sb.toString()
  }

  private def buildSelect(columns: Seq[ColumnRef]): String = {
    if (columns.isEmpty) return "SELECT *"
    val cols = columns.map(renderColumn)
    s"SELECT ${cols.mkString(", ")}"
  }

  private def renderColumn(col: ColumnRef): String = {
    val qualified = col.table match {
      case Some(t) => s"${quoteIdentifier(t)}.${quoteIdentifier(col.column)}"
      case None    => quoteIdentifier(col.column)
    }
    val withAgg = col.aggregation match {
      case Some(agg) => s"${renderAggregation(agg)}($qualified)"
      case None      => qualified
    }
    col.alias match {
      case Some(a) => s"$withAgg AS ${quoteIdentifier(a)}"
      case None    => withAgg
    }
  }

  private def renderAggregation(agg: AggregationType): String =
    agg match {
      case AggregationType.Count => "COUNT"
      case AggregationType.Sum   => "SUM"
      case AggregationType.Avg   => "AVG"
      case AggregationType.Min   => "MIN"
      case AggregationType.Max   => "MAX"
    }

  private def buildFrom(tables: Seq[TableRef]): String = {
    if (tables.isEmpty) return ""
    val refs = tables.map { t =>
      t.alias match {
        case Some(a) => s"${quoteIdentifier(t.name)} AS ${quoteIdentifier(a)}"
        case None    => quoteIdentifier(t.name)
      }
    }
    s" FROM ${refs.mkString(", ")}"
  }

  private def buildJoins(joins: Seq[JoinSpec]): String = {
    if (joins.isEmpty) return ""
    joins.map { j =>
      val joinKeyword = renderJoinType(j.joinType)
      val tableRef = j.rightTable.alias match {
        case Some(a) => s"${quoteIdentifier(j.rightTable.name)} AS ${quoteIdentifier(a)}"
        case None    => quoteIdentifier(j.rightTable.name)
      }
      j.joinType match {
        case JoinType.Cross =>
          s" $joinKeyword $tableRef"
        case _ =>
          s" $joinKeyword $tableRef ON ${quoteIdentifier(j.leftColumn)} = ${quoteIdentifier(j.rightColumn)}"
      }
    }.mkString("")
  }

  private def renderJoinType(jt: JoinType): String =
    jt match {
      case JoinType.Inner => "INNER JOIN"
      case JoinType.Left  => "LEFT JOIN"
      case JoinType.Right => "RIGHT JOIN"
      case JoinType.Full  => "FULL OUTER JOIN"
      case JoinType.Cross => "CROSS JOIN"
    }

  private def buildWhere(filters: Seq[FilterExpr]): String = {
    if (filters.isEmpty) return ""
    val conditions = filters.map(renderFilter)
    s" WHERE ${conditions.mkString(" AND ")}"
  }

  private def renderFilter(f: FilterExpr): String = {
    val col = quoteIdentifier(f.column)
    f.operator match {
      case FilterOp.Eq        => s"$col = ${escapeLiteral(f.value)}"
      case FilterOp.NotEq     => s"$col <> ${escapeLiteral(f.value)}"
      case FilterOp.Lt        => s"$col < ${escapeLiteral(f.value)}"
      case FilterOp.Gt        => s"$col > ${escapeLiteral(f.value)}"
      case FilterOp.Lte       => s"$col <= ${escapeLiteral(f.value)}"
      case FilterOp.Gte       => s"$col >= ${escapeLiteral(f.value)}"
      case FilterOp.In        => s"$col IN (${renderInValues(f.value)})"
      case FilterOp.Like      => s"$col LIKE ${escapeLiteral(f.value)}"
      case FilterOp.IsNull    => s"$col IS NULL"
      case FilterOp.IsNotNull => s"$col IS NOT NULL"
    }
  }

  private def renderInValues(value: String): String =
    value.split(",").map(v => escapeLiteral(v.trim)).mkString(", ")

  private def buildGroupBy(groupBy: Seq[String]): String = {
    if (groupBy.isEmpty) return ""
    s" GROUP BY ${groupBy.map(quoteIdentifier).mkString(", ")}"
  }

  private def buildOrderBy(orderBy: Seq[OrderBySpec]): String = {
    if (orderBy.isEmpty) return ""
    val specs = orderBy.map { o =>
      val dir = if (o.ascending) "ASC" else "DESC"
      s"${quoteIdentifier(o.column)} $dir"
    }
    s" ORDER BY ${specs.mkString(", ")}"
  }

  private[querybuilder] def quoteIdentifier(name: String): String =
    IdentifierPattern.findFirstIn(name) match {
      case Some(_) => s""""$name""""
      case None    => s""""${name.replace("\"", "\"\"")}""""
    }

  private[querybuilder] def escapeLiteral(value: String): String = {
    val sanitized = value.replace("'", "''")
    s"'$sanitized'"
  }
}
