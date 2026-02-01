package io.gbmm.udps.storage.fts

import cats.effect.{IO, Resource}
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector.VarCharVector
import org.apache.lucene.document.{Document, Field, StringField, TextField}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.store.FSDirectory

import java.nio.file.Paths

/**
 * Configuration for full-text search indexing.
 *
 * @param indexPath         filesystem path for the Lucene index directory
 * @param analyzerType      which analyzer to use for tokenization
 * @param commitIntervalMs  auto-commit interval in milliseconds
 */
final case class FTSConfig(
    indexPath: String,
    analyzerType: AnalyzerType = AnalyzerType.Standard,
    commitIntervalMs: Long = 1000L
)

/**
 * Lucene-based full-text indexer that stores text columns from Apache Arrow
 * vectors as searchable Lucene documents.
 *
 * Each indexed row becomes a Lucene document with three fields:
 *  - "id"      — the document identifier (stored, not tokenized)
 *  - "column"  — the column name (stored, not tokenized)
 *  - "content" — the text value (stored and tokenized for search)
 *
 * Use [[LuceneIndexer.resource]] for safe lifecycle management via
 * cats-effect [[Resource]].
 */
final class LuceneIndexer private (
    writer: IndexWriter,
    config: FTSConfig
) extends LazyLogging {

  private val IdField      = "id"
  private val ColumnField  = "column"
  private val ContentField = "content"

  /**
   * Indexes a text column from an Arrow [[VarCharVector]].
   *
   * @param columnName  the logical column name stored with each document
   * @param vector      the Arrow VarChar vector containing text values
   * @param documentIds sequence of document IDs, one per row in the vector
   * @return the number of documents indexed
   */
  def indexColumn(columnName: String, vector: VarCharVector, documentIds: Seq[String]): IO[Long] =
    IO.blocking {
      require(
        vector.getValueCount == documentIds.size,
        s"Vector value count (${vector.getValueCount}) must match documentIds size (${documentIds.size})"
      )

      var indexed = 0L
      var idx = 0
      val count = vector.getValueCount

      while (idx < count) {
        if (!vector.isNull(idx)) {
          val text = new String(vector.get(idx), java.nio.charset.StandardCharsets.UTF_8)
          val doc = createDocument(documentIds(idx), columnName, text)
          writer.addDocument(doc)
          indexed += 1L
        }
        idx += 1
      }

      logger.debug("Indexed {} documents from column '{}' ({} nulls skipped)",
        indexed.toString, columnName, (count - indexed).toString)
      indexed
    }

  /**
   * Indexes a batch of (id, text) pairs.
   *
   * @param columnName the logical column name stored with each document
   * @param documents  sequence of (document ID, text content) tuples
   * @return the number of documents indexed
   */
  def indexBatch(columnName: String, documents: Seq[(String, String)]): IO[Long] =
    IO.blocking {
      var indexed = 0L
      val iter = documents.iterator

      while (iter.hasNext) {
        val (id, text) = iter.next()
        val doc = createDocument(id, columnName, text)
        writer.addDocument(doc)
        indexed += 1L
      }

      logger.debug("Indexed batch of {} documents for column '{}'", indexed.toString, columnName)
      indexed
    }

  /**
   * Deletes documents by their IDs.
   *
   * @param ids the document IDs to delete
   * @return the number of documents targeted for deletion
   */
  def deleteByIds(ids: Seq[String]): IO[Long] =
    IO.blocking {
      val terms = ids.map(id => new Term(IdField, id)).toArray
      writer.deleteDocuments(terms: _*)
      logger.debug("Scheduled deletion of {} documents", ids.size.toString)
      ids.size.toLong
    }

  /** Commits all pending index changes to durable storage. */
  def commit(): IO[Unit] =
    IO.blocking {
      writer.commit()
      logger.debug("Index committed")
    }

  /** Closes the index writer, flushing any buffered changes. */
  def close(): IO[Unit] =
    IO.blocking {
      writer.close()
      logger.info("LuceneIndexer closed for path '{}'", config.indexPath)
    }

  private def createDocument(id: String, columnName: String, text: String): Document = {
    val doc = new Document()
    doc.add(new StringField(IdField, id, Field.Store.YES))
    doc.add(new StringField(ColumnField, columnName, Field.Store.YES))
    doc.add(new TextField(ContentField, text, Field.Store.YES))
    doc
  }
}

object LuceneIndexer extends LazyLogging {

  /**
   * Creates a [[LuceneIndexer]] wrapped in a cats-effect [[Resource]]
   * that guarantees the underlying [[IndexWriter]] is closed on release.
   *
   * @param config full-text search configuration
   * @return a resource-managed LuceneIndexer
   */
  def resource(config: FTSConfig): Resource[IO, LuceneIndexer] =
    Resource.make(
      IO.blocking {
        val directory = FSDirectory.open(Paths.get(config.indexPath))
        val analyzer = AnalyzerFactory.create(config.analyzerType)
        val writerConfig = new IndexWriterConfig(analyzer)
        writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
        writerConfig.setCommitOnClose(true)

        val writer = new IndexWriter(directory, writerConfig)
        logger.info("Opened LuceneIndexer at '{}' with analyzer {}", config.indexPath, config.analyzerType.toString)
        new LuceneIndexer(writer, config)
      }
    )(indexer => indexer.close())
}
