package io.gbmm.udps.storage.fts

import cats.effect.{IO, Resource}
import com.typesafe.scalalogging.LazyLogging
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index.{DirectoryReader, Term}
import org.apache.lucene.search.{
  BooleanClause,
  IndexSearcher,
  ScoreDoc,
  TopDocs,
  BooleanQuery => LBooleanQuery,
  FuzzyQuery => LFuzzyQuery,
  PrefixQuery => LPrefixQuery,
  WildcardQuery => LWildcardQuery
}
import org.apache.lucene.search.similarities.{BM25Similarity, ClassicSimilarity}
import org.apache.lucene.search.{PhraseQuery => LPhraseQuery, Query, TermQuery}
import org.apache.lucene.store.FSDirectory

import java.nio.file.Paths

/**
 * A single search result with score and highlighted snippets.
 *
 * @param documentId the document identifier
 * @param score      the relevance score assigned by the similarity model
 * @param highlights map of field name to highlighted text snippet
 */
final case class SearchResult(
    documentId: String,
    score: Float,
    highlights: Map[String, String]
)

/** Supported query types for full-text search. */
sealed trait QueryType extends Product with Serializable

object QueryType {

  /**
   * Boolean query combining must, should, and must-not clauses.
   * Each clause string is parsed as a term against the content field.
   */
  final case class BooleanQuery(
      must: Seq[String] = Seq.empty,
      should: Seq[String] = Seq.empty,
      mustNot: Seq[String] = Seq.empty
  ) extends QueryType

  /**
   * Phrase query that matches an exact sequence of terms
   * with optional slop (number of positions terms may be moved).
   */
  final case class PhraseQuery(phrase: String, slop: Int = 0) extends QueryType

  /**
   * Fuzzy query that matches terms within a specified edit distance.
   *
   * @param term     the search term
   * @param maxEdits maximum Levenshtein edit distance (0, 1, or 2)
   */
  final case class FuzzyQuery(term: String, maxEdits: Int = 2) extends QueryType

  /**
   * Wildcard query supporting `*` (zero or more characters) and
   * `?` (exactly one character) wildcards.
   */
  final case class WildcardQuery(pattern: String) extends QueryType

  /** Prefix query that matches all terms starting with the given prefix. */
  final case class PrefixQuery(prefix: String) extends QueryType
}

/**
 * Lucene-based full-text searcher supporting multiple query types
 * and scoring models (BM25 and TF-IDF).
 *
 * Use [[LuceneSearcher.resource]] for safe lifecycle management.
 */
final class LuceneSearcher private (
    reader: DirectoryReader,
    analyzer: Analyzer,
    indexPath: String
) extends LazyLogging {

  private val ContentField = "content"
  private val IdField      = "id"
  private val ColumnField  = "column"

  /**
   * Searches the index using BM25 scoring (Lucene 9.x default).
   *
   * @param columnName the column to restrict the search to
   * @param queryType  the query specification
   * @param maxResults maximum number of results to return
   * @return matching documents ordered by relevance
   */
  def search(columnName: String, queryType: QueryType, maxResults: Int = 100): IO[Seq[SearchResult]] =
    IO.blocking {
      val searcher = new IndexSearcher(reader)
      searcher.setSimilarity(new BM25Similarity())
      executeSearch(searcher, columnName, queryType, maxResults)
    }

  /**
   * Searches the index using TF-IDF (ClassicSimilarity) scoring.
   *
   * @param columnName the column to restrict the search to
   * @param queryType  the query specification
   * @param maxResults maximum number of results to return
   * @return matching documents ordered by TF-IDF relevance
   */
  def searchWithTFIDF(columnName: String, queryType: QueryType, maxResults: Int = 100): IO[Seq[SearchResult]] =
    IO.blocking {
      val searcher = new IndexSearcher(reader)
      searcher.setSimilarity(new ClassicSimilarity())
      executeSearch(searcher, columnName, queryType, maxResults)
    }

  /**
   * Counts matching documents without fetching them.
   *
   * @param columnName the column to restrict the search to
   * @param queryType  the query specification
   * @return the total number of matching documents
   */
  def count(columnName: String, queryType: QueryType): IO[Long] =
    IO.blocking {
      val searcher = new IndexSearcher(reader)
      val query = buildColumnScopedQuery(columnName, queryType)
      searcher.count(query).toLong
    }

  /** Closes the underlying directory reader. */
  def close(): IO[Unit] =
    IO.blocking {
      reader.close()
      logger.info("LuceneSearcher closed for path '{}'", indexPath)
    }

  private def executeSearch(
      searcher: IndexSearcher,
      columnName: String,
      queryType: QueryType,
      maxResults: Int
  ): Seq[SearchResult] = {
    val query = buildColumnScopedQuery(columnName, queryType)
    val topDocs: TopDocs = searcher.search(query, maxResults)

    topDocs.scoreDocs.toSeq.map { scoreDoc: ScoreDoc =>
      val doc: Document = searcher.storedFields().document(scoreDoc.doc)
      val docId = doc.get(IdField)
      val content = doc.get(ContentField)

      val highlights: Map[String, String] =
        if (content != null) Map(ContentField -> content)
        else Map.empty

      SearchResult(
        documentId = docId,
        score = scoreDoc.score,
        highlights = highlights
      )
    }
  }

  /**
   * Wraps the content query with a column-name filter so results
   * are restricted to the specified column.
   */
  private def buildColumnScopedQuery(columnName: String, queryType: QueryType): Query = {
    val contentQuery = buildQuery(queryType)
    val columnFilter = new TermQuery(new Term(ColumnField, columnName))

    val builder = new LBooleanQuery.Builder()
    builder.add(columnFilter, BooleanClause.Occur.FILTER)
    builder.add(contentQuery, BooleanClause.Occur.MUST)
    builder.build()
  }

  private def buildQuery(queryType: QueryType): Query = queryType match {
    case QueryType.BooleanQuery(must, should, mustNot) =>
      buildBooleanQuery(must, should, mustNot)

    case QueryType.PhraseQuery(phrase, slop) =>
      buildPhraseQuery(phrase, slop)

    case QueryType.FuzzyQuery(term, maxEdits) =>
      val clampedEdits = math.min(math.max(maxEdits, 0), 2)
      new LFuzzyQuery(new Term(ContentField, term.toLowerCase), clampedEdits)

    case QueryType.WildcardQuery(pattern) =>
      new LWildcardQuery(new Term(ContentField, pattern.toLowerCase))

    case QueryType.PrefixQuery(prefix) =>
      new LPrefixQuery(new Term(ContentField, prefix.toLowerCase))
  }

  private def buildBooleanQuery(
      must: Seq[String],
      should: Seq[String],
      mustNot: Seq[String]
  ): Query = {
    val builder = new LBooleanQuery.Builder()

    must.foreach { term =>
      builder.add(
        new TermQuery(new Term(ContentField, term.toLowerCase)),
        BooleanClause.Occur.MUST
      )
    }

    should.foreach { term =>
      builder.add(
        new TermQuery(new Term(ContentField, term.toLowerCase)),
        BooleanClause.Occur.SHOULD
      )
    }

    mustNot.foreach { term =>
      builder.add(
        new TermQuery(new Term(ContentField, term.toLowerCase)),
        BooleanClause.Occur.MUST_NOT
      )
    }

    builder.build()
  }

  private def buildPhraseQuery(phrase: String, slop: Int): Query = {
    val terms = phrase.toLowerCase.split("\\s+").filter(_.nonEmpty)
    val builder = new LPhraseQuery.Builder()
    builder.setSlop(slop)
    terms.foreach { t =>
      builder.add(new Term(ContentField, t))
    }
    builder.build()
  }
}

object LuceneSearcher extends LazyLogging {

  /**
   * Creates a [[LuceneSearcher]] wrapped in a cats-effect [[Resource]]
   * that guarantees the underlying [[DirectoryReader]] is closed on release.
   *
   * @param indexPath     filesystem path to the Lucene index directory
   * @param analyzerType  analyzer configuration (used for query parsing)
   * @return a resource-managed LuceneSearcher
   */
  def resource(indexPath: String, analyzerType: AnalyzerType = AnalyzerType.Standard): Resource[IO, LuceneSearcher] =
    Resource.make(
      IO.blocking {
        val directory = FSDirectory.open(Paths.get(indexPath))
        val reader = DirectoryReader.open(directory)
        val analyzer = AnalyzerFactory.create(analyzerType)
        logger.info("Opened LuceneSearcher at '{}' with {} documents", indexPath, reader.numDocs().toString)
        new LuceneSearcher(reader, analyzer, indexPath)
      }
    )(searcher => searcher.close())
}
