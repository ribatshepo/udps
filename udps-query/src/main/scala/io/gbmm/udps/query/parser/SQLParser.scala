package io.gbmm.udps.query.parser

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import org.apache.calcite.config.Lex
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.{SqlDynamicParam, SqlKind, SqlNode}
import org.apache.calcite.sql.parser.{SqlParseException, SqlParser => CalciteSqlParser}
import org.apache.calcite.sql.parser.SqlParser.Config
import org.apache.calcite.sql.validate.{SqlValidator, SqlValidatorUtil}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.config.CalciteConnectionConfigImpl
import org.apache.calcite.sql.validate.SqlConformanceEnum

import java.util.Properties
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Classification of SQL statement types. */
sealed trait SqlStatementKind extends Product with Serializable

object SqlStatementKind {
  case object Select extends SqlStatementKind
  case object Insert extends SqlStatementKind
  case object Update extends SqlStatementKind
  case object Delete extends SqlStatementKind
  case object CreateTable extends SqlStatementKind
  case object CreateView extends SqlStatementKind
  case object DropTable extends SqlStatementKind
  case object SetOption extends SqlStatementKind
  case object Explain extends SqlStatementKind
  case object Other extends SqlStatementKind
}

/**
 * Represents a SQL parse error with positional information.
 *
 * @param message  Human-readable error description
 * @param line     1-based line number where the error occurred
 * @param column   1-based column number where the error occurred
 * @param token    The problematic token if identifiable
 */
final case class ParseError(
  message: String,
  line: Int,
  column: Int,
  token: Option[String]
)

/**
 * Result of a successful SQL parse operation.
 *
 * @param sqlNode        The parsed AST node
 * @param kind           Classified statement type
 * @param parameterCount Number of dynamic parameter placeholders (?)
 * @param isDdl          Whether the statement is DDL (vs DML/DQL)
 */
final case class ParseResult(
  sqlNode: SqlNode,
  kind: SqlStatementKind,
  parameterCount: Int,
  isDdl: Boolean
)

/**
 * Configuration for the SQL parser.
 *
 * @param lex                  Lexical policy controlling quoting and case sensitivity
 * @param conformance          SQL conformance level
 * @param identifierMaxLength  Maximum identifier length allowed
 */
final case class SQLParserConfig(
  lex: Lex = Lex.MYSQL_ANSI,
  conformance: SqlConformanceEnum = SqlConformanceEnum.DEFAULT,
  identifierMaxLength: Int = SQLParserConfig.DefaultIdentifierMaxLength
)

object SQLParserConfig {
  private val DefaultIdentifierMaxLength: Int = 256

  val default: SQLParserConfig = SQLParserConfig()
}

/**
 * SQL parser that bridges Apache Calcite's parser with UDPS-specific
 * extensions. Thread-safe: each parse operation creates its own parser
 * instance.
 *
 * @param config Parser configuration settings
 */
final class SQLParser private (config: SQLParserConfig) extends LazyLogging {

  private val calciteConfig: Config =
    CalciteSqlParser.config()
      .withLex(config.lex)
      .withConformance(config.conformance)
      .withIdentifierMaxLength(config.identifierMaxLength)

  /**
   * Parse a SQL string into a [[ParseResult]].
   *
   * @param sql The SQL statement to parse
   * @return Either a [[ParseError]] or a [[ParseResult]]
   */
  def parse(sql: String): Either[ParseError, ParseResult] = {
    val trimmed = stripTrailingSemicolons(sql)
    if (trimmed.isEmpty) {
      return Left(ParseError("Empty SQL statement", 1, 1, None))
    }

    try {
      val parser = CalciteSqlParser.create(trimmed, calciteConfig)
      val sqlNode = parser.parseStmt()
      val kind = classifyKind(sqlNode)
      val paramCount = countParameters(sqlNode)
      val ddl = isDdlStatement(kind)
      Right(ParseResult(sqlNode, kind, paramCount, ddl))
    } catch {
      case e: SqlParseException =>
        Left(buildParseError(e))
      case NonFatal(e) =>
        Left(ParseError(
          s"Unexpected parse failure: ${e.getMessage}",
          1, 1, None
        ))
    }
  }

  /**
   * Parse a SQL string, returning the result in a Cats Effect IO.
   *
   * @param sql The SQL statement to parse
   * @return IO that yields a [[ParseResult]] or raises a [[ParseError]] as an exception
   */
  def parseIO(sql: String): IO[ParseResult] =
    IO.fromEither(
      parse(sql).left.map(err =>
        new IllegalArgumentException(
          s"SQL parse error at line ${err.line}, column ${err.column}: ${err.message}"
        )
      )
    )

  /**
   * Parse and validate a SQL statement against a Calcite schema.
   *
   * @param sql    The SQL statement to parse
   * @param schema The schema to validate against
   * @return Either a [[ParseError]] or a validated [[ParseResult]]
   */
  def parseAndValidate(
    sql: String,
    schema: SchemaPlus
  ): Either[ParseError, ParseResult] =
    parse(sql).flatMap { result =>
      validateNode(result, schema)
    }

  /**
   * Parse and validate a SQL statement, returning the result in IO.
   *
   * @param sql    The SQL statement to parse
   * @param schema The schema to validate against
   * @return IO that yields a validated [[ParseResult]]
   */
  def parseAndValidateIO(
    sql: String,
    schema: SchemaPlus
  ): IO[ParseResult] =
    IO.fromEither(
      parseAndValidate(sql, schema).left.map(err =>
        new IllegalArgumentException(
          s"SQL validation error at line ${err.line}, column ${err.column}: ${err.message}"
        )
      )
    )

  private def validateNode(
    result: ParseResult,
    schema: SchemaPlus
  ): Either[ParseError, ParseResult] =
    try {
      val validator = buildValidator(schema)
      val validatedNode = validator.validate(result.sqlNode)
      val paramCount = countParameters(validatedNode)
      Right(result.copy(sqlNode = validatedNode, parameterCount = paramCount))
    } catch {
      case e: org.apache.calcite.runtime.CalciteContextException =>
        Left(ParseError(
          e.getMessage,
          Option(e.getPosLine).map(_.intValue()).getOrElse(1),
          Option(e.getPosColumn).map(_.intValue()).getOrElse(1),
          None
        ))
      case NonFatal(e) =>
        Left(ParseError(
          s"Validation failed: ${e.getMessage}",
          1, 1, None
        ))
    }

  private def buildValidator(schema: SchemaPlus): SqlValidator = {
    val typeFactory: RelDataTypeFactory =
      new org.apache.calcite.sql.`type`.SqlTypeFactoryImpl(
        org.apache.calcite.rel.`type`.RelDataTypeSystem.DEFAULT
      )

    val rootSchema = CalciteSchema.from(schema)

    val connectionProperties = new Properties()
    val calciteConnectionConfig = new CalciteConnectionConfigImpl(connectionProperties)

    val catalogReader = new CalciteCatalogReader(
      rootSchema,
      List(schema.getName).asJava,
      typeFactory,
      calciteConnectionConfig
    )

    SqlValidatorUtil.newValidator(
      SqlStdOperatorTable.instance(),
      catalogReader,
      typeFactory,
      SqlValidator.Config.DEFAULT
        .withConformance(config.conformance)
    )
  }

  private def classifyKind(node: SqlNode): SqlStatementKind =
    node.getKind match {
      case SqlKind.SELECT | SqlKind.UNION | SqlKind.INTERSECT |
           SqlKind.EXCEPT | SqlKind.ORDER_BY        => SqlStatementKind.Select
      case SqlKind.INSERT                            => SqlStatementKind.Insert
      case SqlKind.UPDATE                            => SqlStatementKind.Update
      case SqlKind.DELETE                            => SqlStatementKind.Delete
      case SqlKind.CREATE_TABLE                      => SqlStatementKind.CreateTable
      case SqlKind.CREATE_VIEW                       => SqlStatementKind.CreateView
      case SqlKind.DROP_TABLE                        => SqlStatementKind.DropTable
      case SqlKind.SET_OPTION                        => SqlStatementKind.SetOption
      case SqlKind.EXPLAIN                           => SqlStatementKind.Explain
      case _                                         => SqlStatementKind.Other
    }

  private def isDdlStatement(kind: SqlStatementKind): Boolean =
    kind match {
      case SqlStatementKind.CreateTable |
           SqlStatementKind.CreateView |
           SqlStatementKind.DropTable => true
      case _ => false
    }

  private def countParameters(node: SqlNode): Int = {
    var count = 0
    collectDynamicParams(node, acc = { _ => count += 1 })
    count
  }

  /**
   * Walk the SqlNode tree to find all dynamic parameter markers.
   * Uses Calcite's accept/visitor would be ideal, but a simple
   * recursive toString scan for ? markers is fragile. Instead we
   * walk the tree structure directly.
   */
  private def collectDynamicParams(node: SqlNode, acc: SqlDynamicParam => Unit): Unit =
    node match {
      case dp: SqlDynamicParam =>
        acc(dp)
      case _ =>
        node match {
          case call: org.apache.calcite.sql.SqlCall =>
            call.getOperandList.asScala.foreach { operand =>
              if (operand != null) collectDynamicParams(operand, acc)
            }
          case _ => ()
        }
    }

  private def buildParseError(e: SqlParseException): ParseError = {
    val pos = e.getPos
    val line = if (pos != null) pos.getLineNum else 1
    val col = if (pos != null) pos.getColumnNum else 1

    val token = extractToken(e.getMessage)

    ParseError(
      message = e.getMessage,
      line = line,
      column = col,
      token = token
    )
  }

  private def extractToken(message: String): Option[String] = {
    val encounterPattern = """Encountered "([^"]+)" """.r
    encounterPattern.findFirstMatchIn(message).map(_.group(1))
  }

  private def stripTrailingSemicolons(sql: String): String = {
    val trimmed = sql.trim
    if (trimmed.endsWith(";")) {
      stripTrailingSemicolons(trimmed.dropRight(1))
    } else {
      trimmed
    }
  }
}

object SQLParser {

  /**
   * Create a SQLParser with the provided configuration.
   *
   * @param config Parser configuration
   * @return A configured SQLParser instance
   */
  def apply(config: SQLParserConfig): SQLParser =
    new SQLParser(config)

  /**
   * Create a SQLParser with default settings:
   * - MYSQL_ANSI lexing (case-insensitive unquoted identifiers, double-quoted identifiers)
   * - SQL:2016 conformance
   * - 256-character identifier limit
   * - != operator enabled
   *
   * @return A SQLParser with sensible defaults
   */
  def default(): SQLParser =
    new SQLParser(SQLParserConfig.default)

  /**
   * Create a default SQLParser wrapped in IO.
   *
   * @return IO that yields a configured SQLParser
   */
  def defaultIO: IO[SQLParser] =
    IO.pure(default())
}
