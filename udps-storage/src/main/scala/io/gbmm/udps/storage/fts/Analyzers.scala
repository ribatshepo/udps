package io.gbmm.udps.storage.fts

import org.apache.lucene.analysis.{Analyzer, TokenStream, Tokenizer}
import org.apache.lucene.analysis.core.LowerCaseFilter
import org.apache.lucene.analysis.phonetic.PhoneticFilter
import org.apache.lucene.analysis.standard.{StandardAnalyzer, StandardTokenizer}
import org.apache.lucene.analysis.ngram.NGramTokenizer
import org.apache.lucene.analysis.de.GermanAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.es.SpanishAnalyzer
import org.apache.lucene.analysis.fr.FrenchAnalyzer
import org.apache.lucene.analysis.it.ItalianAnalyzer
import org.apache.lucene.analysis.nl.DutchAnalyzer
import org.apache.lucene.analysis.pt.PortugueseAnalyzer
import org.apache.lucene.analysis.ru.RussianAnalyzer
import org.apache.commons.codec.language.Metaphone

sealed trait AnalyzerType extends Product with Serializable

object AnalyzerType {

  /** Standard Lucene analyzer with stop-word removal. */
  case object Standard extends AnalyzerType

  /**
   * N-gram analyzer that produces tokens of length between
   * [[minGram]] and [[maxGram]] characters.
   */
  final case class NGram(minGram: Int = 2, maxGram: Int = 3) extends AnalyzerType

  /** Phonetic analyzer using the Metaphone algorithm. */
  case object Phonetic extends AnalyzerType

  /**
   * Language-specific analyzer. Supported languages:
   * "english", "spanish", "french", "german", "italian",
   * "dutch", "portuguese", "russian".
   */
  final case class LanguageSpecific(language: String) extends AnalyzerType
}

/**
 * Factory for constructing Lucene [[Analyzer]] instances from
 * an [[AnalyzerType]] descriptor.
 */
object AnalyzerFactory {

  /**
   * Creates a configured Lucene [[Analyzer]] for the given type.
   *
   * @param analyzerType the analyzer specification
   * @return a ready-to-use Lucene Analyzer
   * @throws IllegalArgumentException if the language is not supported
   */
  def create(analyzerType: AnalyzerType): Analyzer = analyzerType match {
    case AnalyzerType.Standard =>
      new StandardAnalyzer()

    case AnalyzerType.NGram(minGram, maxGram) =>
      createNGramAnalyzer(minGram, maxGram)

    case AnalyzerType.Phonetic =>
      createPhoneticAnalyzer()

    case AnalyzerType.LanguageSpecific(language) =>
      createLanguageAnalyzer(language)
  }

  private def createNGramAnalyzer(minGram: Int, maxGram: Int): Analyzer =
    new Analyzer {
      override protected def createComponents(fieldName: String): Analyzer.TokenStreamComponents = {
        val tokenizer = new NGramTokenizer(minGram, maxGram)
        val lowerCase = new LowerCaseFilter(tokenizer)
        new Analyzer.TokenStreamComponents(tokenizer, lowerCase)
      }
    }

  private def createPhoneticAnalyzer(): Analyzer =
    new Analyzer {
      override protected def createComponents(fieldName: String): Analyzer.TokenStreamComponents = {
        val tokenizer: Tokenizer = new StandardTokenizer()
        val lowerCase: TokenStream = new LowerCaseFilter(tokenizer)
        val phonetic: TokenStream = new PhoneticFilter(lowerCase, new Metaphone(), false)
        new Analyzer.TokenStreamComponents(tokenizer, phonetic)
      }
    }

  private val SupportedLanguages: Map[String, () => Analyzer] = Map(
    "english"    -> (() => new EnglishAnalyzer()),
    "spanish"    -> (() => new SpanishAnalyzer()),
    "french"     -> (() => new FrenchAnalyzer()),
    "german"     -> (() => new GermanAnalyzer()),
    "italian"    -> (() => new ItalianAnalyzer()),
    "dutch"      -> (() => new DutchAnalyzer()),
    "portuguese" -> (() => new PortugueseAnalyzer()),
    "russian"    -> (() => new RussianAnalyzer())
  )

  private def createLanguageAnalyzer(language: String): Analyzer = {
    val normalizedLang = language.toLowerCase.trim
    SupportedLanguages.get(normalizedLang) match {
      case Some(factory) => factory()
      case None =>
        throw new IllegalArgumentException(
          s"Unsupported language: '$language'. Supported languages: ${SupportedLanguages.keys.mkString(", ")}"
        )
    }
  }
}
