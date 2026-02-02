package io.gbmm.udps.query.cache

import cats.effect.{IO, Ref, Resource, Temporal}
import cats.syntax.all._
import dev.profunktor.redis4cats.RedisCommands
import io.circe._
import io.circe.syntax._
import io.circe.parser.{decode => circeDecode}
import com.typesafe.scalalogging.LazyLogging

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import scala.concurrent.duration.FiniteDuration

// ---------------------------------------------------------------------------
// Domain models
// ---------------------------------------------------------------------------

final case class QueryResult(
  columns: Seq[String],
  rows: Seq[Map[String, Any]],
  rowCount: Long,
  executionTimeMs: Long,
  stagesExecuted: Seq[String]
)

final case class CachedQueryResult(
  columns: Seq[String],
  rows: Seq[Map[String, Any]],
  rowCount: Long,
  executionTimeMs: Long,
  stagesExecuted: Seq[String],
  cacheHit: Boolean,
  cacheKey: String
)

object CachedQueryResult {

  def fromQueryResult(qr: QueryResult, cacheHit: Boolean, cacheKey: String): CachedQueryResult =
    CachedQueryResult(
      columns = qr.columns,
      rows = qr.rows,
      rowCount = qr.rowCount,
      executionTimeMs = qr.executionTimeMs,
      stagesExecuted = qr.stagesExecuted,
      cacheHit = cacheHit,
      cacheKey = cacheKey
    )
}

final case class CacheStats(
  hitCount: Long,
  missCount: Long,
  hitRatio: Double,
  entryCount: Long
)

// ---------------------------------------------------------------------------
// Circe codecs for QueryResult serialization
// ---------------------------------------------------------------------------

object QueryResultCodecs {

  // Map[String, Any] needs manual encoding since Any is not natively supported.
  // We encode values as JSON primitives: String, Number, Boolean, Null.
  implicit val anyEncoder: Encoder[Any] = Encoder.instance {
    case s: String      => Json.fromString(s)
    case i: Int         => Json.fromInt(i)
    case l: Long        => Json.fromLong(l)
    case d: Double      => Json.fromDoubleOrNull(d)
    case f: Float       => Json.fromFloatOrNull(f)
    case b: Boolean     => Json.fromBoolean(b)
    case bd: BigDecimal => Json.fromBigDecimal(bd)
    case bi: BigInt     => Json.fromBigInt(bi)
    case null           => Json.Null
    case None           => Json.Null
    case Some(v)        => anyEncoder(v)
    case other          => Json.fromString(other.toString)
  }

  // When decoding, we recover typed values from the JSON scalar types.
  implicit val anyDecoder: Decoder[Any] = Decoder.instance { cursor =>
    cursor.focus match {
      case Some(json) =>
        if (json.isNull) Right(null)
        else if (json.isBoolean) json.asBoolean.toRight(DecodingFailure("Expected boolean", cursor.history))
        else if (json.isNumber) {
          val num = json.asNumber.get
          num.toLong match {
            case Some(l) => Right(l)
            case None    => Right(num.toDouble)
          }
        }
        else if (json.isString) json.asString.toRight(DecodingFailure("Expected string", cursor.history))
        else Left(DecodingFailure("Unsupported JSON value type", cursor.history))
      case None =>
        Left(DecodingFailure("Empty cursor", cursor.history))
    }
  }

  implicit val mapEncoder: Encoder[Map[String, Any]] = Encoder.instance { m =>
    Json.obj(m.map { case (k, v) => k -> anyEncoder(v) }.toSeq: _*)
  }

  implicit val mapDecoder: Decoder[Map[String, Any]] = Decoder.instance { cursor =>
    cursor.as[JsonObject].map { obj =>
      obj.toMap.map { case (k, v) =>
        k -> (anyDecoder.decodeJson(v) match {
          case Right(decoded) => decoded
          case Left(_)        => v.noSpaces
        })
      }
    }
  }

  implicit val queryResultEncoder: Encoder[QueryResult] = Encoder.instance { qr =>
    Json.obj(
      "columns" -> qr.columns.asJson,
      "rows" -> qr.rows.map(mapEncoder.apply).asJson,
      "rowCount" -> qr.rowCount.asJson,
      "executionTimeMs" -> qr.executionTimeMs.asJson,
      "stagesExecuted" -> qr.stagesExecuted.asJson
    )
  }

  implicit val queryResultDecoder: Decoder[QueryResult] = Decoder.instance { cursor =>
    for {
      columns        <- cursor.downField("columns").as[Seq[String]]
      rows           <- cursor.downField("rows").as[Seq[Map[String, Any]]](Decoder.decodeSeq(mapDecoder))
      rowCount       <- cursor.downField("rowCount").as[Long]
      executionTimeMs <- cursor.downField("executionTimeMs").as[Long]
      stagesExecuted <- cursor.downField("stagesExecuted").as[Seq[String]]
    } yield QueryResult(columns, rows, rowCount, executionTimeMs, stagesExecuted)
  }
}

// ---------------------------------------------------------------------------
// SQL normalization and table extraction
// ---------------------------------------------------------------------------

object SqlNormalizer {

  private val SingleLineComment = """--[^\n]*""".r
  private val MultiLineComment  = """/\*[\s\S]*?\*/""".r
  private val ExtraWhitespace   = """\s+""".r

  /** Normalize SQL for cache key generation: remove comments, lowercase, collapse whitespace, trim. */
  def normalize(sql: String): String = {
    val withoutSingleLine = SingleLineComment.replaceAllIn(sql, " ")
    val withoutMultiLine  = MultiLineComment.replaceAllIn(withoutSingleLine, " ")
    val collapsed         = ExtraWhitespace.replaceAllIn(withoutMultiLine, " ")
    collapsed.trim.toLowerCase
  }

  /** Generate a SHA-256 cache key from normalized SQL and sorted parameters. */
  def cacheKey(sql: String, params: Seq[Any]): String = {
    val normalizedSql   = normalize(sql)
    val sortedParamStr  = params.map(p => if (p == null) "null" else p.toString).mkString("|")
    val input           = s"$normalizedSql:$sortedParamStr"
    val digest          = MessageDigest.getInstance("SHA-256")
    val hashBytes       = digest.digest(input.getBytes(StandardCharsets.UTF_8))
    hashBytes.map("%02x".format(_)).mkString
  }

  // Regex patterns for extracting table names from FROM and JOIN clauses.
  // Matches: FROM table, FROM schema.table, JOIN table, etc.
  private val FromPattern = """(?i)\bfrom\s+([a-z_][a-z0-9_.]*(?:\s*,\s*[a-z_][a-z0-9_.]*)*)""".r
  private val JoinPattern = """(?i)\bjoin\s+([a-z_][a-z0-9_.]*)""".r
  private val TableNameInList = """([a-z_][a-z0-9_.]*)""".r

  /** Extract table names referenced in a SQL query (simple regex-based). */
  def extractTables(sql: String): Set[String] = {
    val normalized = normalize(sql)

    val fromTables = FromPattern.findAllMatchIn(normalized).flatMap { m =>
      val tableList = m.group(1)
      TableNameInList.findAllMatchIn(tableList).map(_.group(1))
    }.toSet

    val joinTables = JoinPattern.findAllMatchIn(normalized).map(_.group(1)).toSet

    (fromTables ++ joinTables).map { name =>
      // Extract just the table name if schema-qualified (schema.table -> table)
      val parts = name.split('.')
      parts.last
    }
  }
}

// ---------------------------------------------------------------------------
// CacheBackend abstraction
// ---------------------------------------------------------------------------

trait CacheBackend[F[_]] {
  def get(key: String): F[Option[String]]
  def put(key: String, value: String, ttl: FiniteDuration): F[Unit]
  def delete(key: String): F[Unit]
  def deleteByPattern(pattern: String): F[Long]
  def exists(key: String): F[Boolean]
}

// ---------------------------------------------------------------------------
// Redis cache backend
// ---------------------------------------------------------------------------

final class RedisCacheBackend(
  redis: RedisCommands[IO, String, String],
  keyPrefix: String
) extends CacheBackend[IO] with LazyLogging {

  private def prefixed(key: String): String = s"$keyPrefix:$key"

  override def get(key: String): IO[Option[String]] =
    redis.get(prefixed(key))

  override def put(key: String, value: String, ttl: FiniteDuration): IO[Unit] =
    redis.setEx(prefixed(key), value, ttl)

  override def delete(key: String): IO[Unit] =
    redis.del(prefixed(key)).void

  override def deleteByPattern(pattern: String): IO[Long] =
    for {
      keys    <- redis.keys(s"$keyPrefix:$pattern")
      deleted <- if (keys.isEmpty) IO.pure(0L)
                 else keys.toList.traverse(k => redis.del(k)).map(_.map(_.toLong).sum)
    } yield deleted

  override def exists(key: String): IO[Boolean] =
    redis.exists(prefixed(key))
}

object RedisCacheBackend {

  val DefaultKeyPrefix: String = "udps:query:cache"

  def make(
    redis: RedisCommands[IO, String, String],
    keyPrefix: String = DefaultKeyPrefix
  ): Resource[IO, RedisCacheBackend] =
    Resource.pure(new RedisCacheBackend(redis, keyPrefix))
}

// ---------------------------------------------------------------------------
// In-memory cache backend (for testing / single-node)
// ---------------------------------------------------------------------------

final case class CacheEntry(value: String, expiresAt: Instant)

final class InMemoryCacheBackend private (
  store: Ref[IO, Map[String, CacheEntry]]
) extends CacheBackend[IO] {

  override def get(key: String): IO[Option[String]] =
    for {
      now     <- IO.realTimeInstant
      entries <- store.get
      result = entries.get(key).filter(_.expiresAt.isAfter(now)).map(_.value)
    } yield result

  override def put(key: String, value: String, ttl: FiniteDuration): IO[Unit] =
    for {
      now <- IO.realTimeInstant
      expiresAt = now.plusNanos(ttl.toNanos)
      _ <- store.update(_ + (key -> CacheEntry(value, expiresAt)))
    } yield ()

  override def delete(key: String): IO[Unit] =
    store.update(_ - key)

  override def deleteByPattern(pattern: String): IO[Long] = {
    // Convert glob pattern to regex: * -> .*, ? -> .
    val regex = ("^" + pattern.replace("*", ".*").replace("?", ".") + "$").r
    store.modify { entries =>
      val matching = entries.keys.filter(k => regex.findFirstIn(k).isDefined).toSet
      val updated  = entries -- matching
      (updated, matching.size.toLong)
    }
  }

  override def exists(key: String): IO[Boolean] =
    for {
      now     <- IO.realTimeInstant
      entries <- store.get
    } yield entries.get(key).exists(_.expiresAt.isAfter(now))

  /** Remove all expired entries. */
  def evictExpired: IO[Int] =
    for {
      now <- IO.realTimeInstant
      evicted <- store.modify { entries =>
        val (alive, expired) = entries.partition { case (_, e) => e.expiresAt.isAfter(now) }
        (alive, expired.size)
      }
    } yield evicted
}

object InMemoryCacheBackend {

  private val DefaultCleanupInterval: FiniteDuration = {
    import scala.concurrent.duration._
    60.seconds
  }

  /** Create an InMemoryCacheBackend with a background cleanup fiber. */
  def make(
    cleanupInterval: FiniteDuration = DefaultCleanupInterval
  ): Resource[IO, InMemoryCacheBackend] =
    for {
      store   <- Resource.eval(Ref.of[IO, Map[String, CacheEntry]](Map.empty))
      backend  = new InMemoryCacheBackend(store)
      _       <- Resource.make(
                   startCleanupLoop(backend, cleanupInterval).start
                 )(_.cancel)
    } yield backend

  private def startCleanupLoop(
    backend: InMemoryCacheBackend,
    interval: FiniteDuration
  ): IO[Unit] =
    (Temporal[IO].sleep(interval) *> backend.evictExpired).foreverM
}

// ---------------------------------------------------------------------------
// QueryCache
// ---------------------------------------------------------------------------

final class QueryCache private (
  backend: CacheBackend[IO],
  hitCount: Ref[IO, Long],
  missCount: Ref[IO, Long],
  tableIndex: Ref[IO, Map[String, Set[String]]]  // tableName -> Set[cacheKey]
) extends LazyLogging {

  import QueryResultCodecs._

  /**
   * Get a cached result or execute the query and cache the result.
   *
   * @param sql     the SQL query text
   * @param params  query parameters (used in cache key generation)
   * @param ttl     time-to-live for the cache entry
   * @param execute the query execution thunk, invoked only on cache miss
   * @return the query result with cache metadata
   */
  def getOrExecute(
    sql: String,
    params: Seq[Any],
    ttl: FiniteDuration
  )(execute: => IO[QueryResult]): IO[CachedQueryResult] = {
    val key = SqlNormalizer.cacheKey(sql, params)

    backend.get(key).flatMap {
      case Some(json) =>
        circeDecode[QueryResult](json) match {
          case Right(qr) =>
            hitCount.update(_ + 1).as(
              CachedQueryResult.fromQueryResult(qr, cacheHit = true, cacheKey = key)
            )
          case Left(err) =>
            // Corrupted cache entry: log, delete, and re-execute.
            IO(logger.warn(s"Cache decode failure for key=$key: ${err.getMessage}")) *>
              backend.delete(key) *>
              executeFreshAndCache(sql, params, key, ttl, execute)
        }
      case None =>
        executeFreshAndCache(sql, params, key, ttl, execute)
    }
  }

  private def executeFreshAndCache(
    sql: String,
    params: Seq[Any],
    key: String,
    ttl: FiniteDuration,
    execute: => IO[QueryResult]
  ): IO[CachedQueryResult] =
    for {
      qr  <- execute
      json = qr.asJson.noSpaces
      _   <- backend.put(key, json, ttl)
      _   <- missCount.update(_ + 1)
      _   <- registerTables(sql, key)
    } yield CachedQueryResult.fromQueryResult(qr, cacheHit = false, cacheKey = key)

  /** Register the association between referenced tables and a cache key. */
  private def registerTables(sql: String, key: String): IO[Unit] = {
    val tables = SqlNormalizer.extractTables(sql)
    tableIndex.update { idx =>
      tables.foldLeft(idx) { (acc, table) =>
        acc.updated(table, acc.getOrElse(table, Set.empty) + key)
      }
    }
  }

  /** Invalidate all cached queries that reference the given table. */
  def invalidateTable(tableName: String): IO[Long] = {
    val normalizedTable = tableName.toLowerCase.trim
    for {
      keys <- tableIndex.modify { idx =>
        val matchingKeys = idx.getOrElse(normalizedTable, Set.empty)
        val updated      = idx - normalizedTable
        (updated, matchingKeys)
      }
      _ <- keys.toList.traverse_(backend.delete)
    } yield keys.size.toLong
  }

  /** Invalidate all cache entries. */
  def invalidateAll(): IO[Unit] =
    for {
      keys <- tableIndex.modify(idx => (Map.empty, idx.values.flatten.toSet))
      _    <- keys.toList.traverse_(backend.delete)
      _    <- hitCount.set(0L)
      _    <- missCount.set(0L)
    } yield ()

  /** Retrieve cache statistics. */
  def stats: IO[CacheStats] =
    for {
      hits   <- hitCount.get
      misses <- missCount.get
      idx    <- tableIndex.get
      total   = hits + misses
      ratio   = if (total > 0L) hits.toDouble / total.toDouble else 0.0
      entries = idx.values.flatten.toSet.size.toLong
    } yield CacheStats(
      hitCount = hits,
      missCount = misses,
      hitRatio = ratio,
      entryCount = entries
    )

  /** Invalidate a single cache entry by its key. */
  def invalidateKey(key: String): IO[Unit] =
    for {
      _ <- backend.delete(key)
      _ <- tableIndex.update(_.map { case (t, ks) => t -> (ks - key) })
    } yield ()
}

object QueryCache {

  def make(backend: CacheBackend[IO]): Resource[IO, QueryCache] =
    Resource.eval(
      for {
        hitRef   <- Ref.of[IO, Long](0L)
        missRef  <- Ref.of[IO, Long](0L)
        tableRef <- Ref.of[IO, Map[String, Set[String]]](Map.empty)
      } yield new QueryCache(backend, hitRef, missRef, tableRef)
    )
}
