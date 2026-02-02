package io.gbmm.udps.integration.ustrp

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.http4s.{MediaType, Method, Request, Uri}
import org.http4s.circe._
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.headers.{Accept, `Content-Type`}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import java.nio.ByteBuffer
import java.time.Instant
import scala.concurrent.duration._

/** Configuration for connecting to a Confluent Schema Registry.
  *
  * @param baseUrl      the base URL of the schema registry (e.g. "http://schema-registry:8081")
  * @param cacheTtl     how long to cache schemas before re-fetching
  * @param maxCacheSize maximum number of schemas to hold in the cache
  */
final case class SchemaRegistryConfig(
    baseUrl: String,
    cacheTtl: FiniteDuration,
    maxCacheSize: Int
)

object SchemaRegistryConfig {
  implicit val reader: ConfigReader[SchemaRegistryConfig] =
    deriveReader[SchemaRegistryConfig]
}

/** Schema metadata returned by the schema registry.
  *
  * @param id         the globally unique schema ID
  * @param schema     the schema definition string (Avro JSON, Protobuf IDL, etc.)
  * @param schemaType the type of schema ("AVRO", "PROTOBUF", "JSON")
  * @param version    the version of the schema under its subject
  */
final case class SchemaInfo(
    id: Int,
    schema: String,
    schemaType: String,
    version: Int
)

object SchemaInfo {
  implicit val decoder: Decoder[SchemaInfo] = deriveDecoder[SchemaInfo]
  implicit val encoder: Encoder[SchemaInfo] = deriveEncoder[SchemaInfo]
}

/** Wrapper holding a cached schema and the time it was fetched.
  *
  * @param schema    the cached schema information
  * @param fetchedAt the instant the schema was fetched from the registry
  */
final case class CachedSchema(
    schema: SchemaInfo,
    fetchedAt: Instant
)

/** Response from the schema registry when fetching a schema by ID.
  * The registry does not return version in the /schemas/ids/:id endpoint,
  * so we default version to 0 and populate it when available.
  */
private[ustrp] final case class SchemaByIdResponse(
    id: Int,
    schema: String,
    schemaType: Option[String]
)

private[ustrp] object SchemaByIdResponse {
  implicit val decoder: Decoder[SchemaByIdResponse] = deriveDecoder[SchemaByIdResponse]
}

/** Response from the schema registry for subject version endpoints. */
private[ustrp] final case class SubjectVersionResponse(
    id: Int,
    schema: String,
    schemaType: Option[String],
    version: Int,
    subject: String
)

private[ustrp] object SubjectVersionResponse {
  implicit val decoder: Decoder[SubjectVersionResponse] = deriveDecoder[SubjectVersionResponse]
}

/** Request body for registering a new schema. */
private[ustrp] final case class RegisterSchemaRequest(
    schema: String,
    schemaType: String
)

private[ustrp] object RegisterSchemaRequest {
  implicit val encoder: Encoder[RegisterSchemaRequest] = deriveEncoder[RegisterSchemaRequest]
}

/** Response body after registering a schema. */
private[ustrp] final case class RegisterSchemaResponse(id: Int)

private[ustrp] object RegisterSchemaResponse {
  implicit val decoder: Decoder[RegisterSchemaResponse] = deriveDecoder[RegisterSchemaResponse]
}

/** HTTP-based implementation of [[SchemaRegistryClient]] that communicates with
  * a Confluent Schema Registry via its REST API.
  *
  * Schemas fetched by ID are cached in-memory with a configurable TTL. The cache
  * size is bounded; when the limit is reached, the oldest entry is evicted.
  *
  * @param httpClient the http4s client for making HTTP requests
  * @param config     schema registry configuration
  * @param cache      concurrent reference holding the in-memory schema cache
  */
final class HttpSchemaRegistryClient private (
    httpClient: Client[IO],
    config: SchemaRegistryConfig,
    cache: Ref[IO, Map[Int, CachedSchema]]
) extends SchemaRegistryClient
    with LazyLogging {

  private val ConfluentMagicByte: Byte = 0x0
  private val SchemaIdByteCount: Int = 4
  private val WireFormatHeaderSize: Int = 1 + SchemaIdByteCount

  private val schemaRegistryAccept: Accept = Accept(
    MediaType.application.json
  )

  private val schemaRegistryContentType: `Content-Type` = `Content-Type`(
    MediaType.unsafeParse("application/vnd.schemaregistry.v1+json")
  )

  /** Decode Confluent wire-format bytes by extracting the schema ID and
    * returning the raw payload bytes after the 5-byte header.
    *
    * The Confluent wire format is:
    *   - Byte 0: magic byte (0x0)
    *   - Bytes 1-4: 4-byte big-endian schema ID
    *   - Bytes 5+: serialized payload
    *
    * This method validates the magic byte, extracts the schema ID (triggering
    * a cache-or-fetch of the schema metadata for validation), and returns
    * the payload bytes.
    */
  override def decode(topic: String, data: Array[Byte]): IO[Array[Byte]] =
    for {
      _ <- IO.raiseWhen(data.length < WireFormatHeaderSize)(
        new IllegalArgumentException(
          s"Data too short for Confluent wire format: expected at least $WireFormatHeaderSize bytes, got ${data.length}"
        )
      )
      _ <- IO.raiseWhen(data(0) != ConfluentMagicByte)(
        new IllegalArgumentException(
          s"Invalid Confluent wire format magic byte: expected 0x0, got 0x${String.format("%02X", Byte.box(data(0)))}"
        )
      )
      schemaId <- IO.delay {
        ByteBuffer.wrap(data, 1, SchemaIdByteCount).getInt
      }
      _ <- getSchema(schemaId)
      payload <- IO.delay {
        val payloadLength = data.length - WireFormatHeaderSize
        val payload = new Array[Byte](payloadLength)
        System.arraycopy(data, WireFormatHeaderSize, payload, 0, payloadLength)
        payload
      }
      _ <- IO.delay(
        logger.debug(
          s"Decoded wire-format message from topic=$topic schemaId=$schemaId payloadSize=${payload.length}"
        )
      )
    } yield payload

  /** Fetch schema metadata by ID, using the in-memory cache when available.
    *
    * If the cached entry has expired (older than `cacheTtl`), it is evicted
    * and a fresh copy is fetched from the registry.
    *
    * @param id the schema ID to look up
    * @return the schema information
    */
  def getSchema(id: Int): IO[SchemaInfo] =
    for {
      now <- IO.delay(Instant.now())
      currentCache <- cache.get
      cachedEntry = currentCache.get(id)
      schemaInfo <- cachedEntry match {
        case Some(cached) if !isExpired(cached, now) =>
          IO.pure(cached.schema)
        case _ =>
          fetchSchemaById(id).flatMap { info =>
            putInCache(id, info, now).as(info)
          }
      }
    } yield schemaInfo

  /** Register a new schema under the given subject.
    *
    * @param subject    the subject name (e.g. "my-topic-value")
    * @param schema     the schema definition string
    * @param schemaType the schema type ("AVRO", "PROTOBUF", or "JSON"), defaults to "AVRO"
    * @return the ID assigned to the newly registered schema
    */
  def registerSchema(
      subject: String,
      schema: String,
      schemaType: String = "AVRO"
  ): IO[Int] =
    for {
      baseUri <- parseBaseUri
      uri = baseUri / "subjects" / subject / "versions"
      requestBody = RegisterSchemaRequest(schema, schemaType)
      request = Request[IO](Method.POST, uri)
        .withHeaders(schemaRegistryAccept, schemaRegistryContentType)
        .withEntity(requestBody)(jsonEncoderOf[IO, RegisterSchemaRequest])
      response <- httpClient.expect[RegisterSchemaResponse](request)(
        jsonOf[IO, RegisterSchemaResponse]
      )
      _ <- IO.delay(
        logger.info(
          s"Registered schema for subject=$subject type=$schemaType, assigned id=${response.id}"
        )
      )
    } yield response.id

  /** Fetch the latest version of a schema for the given subject.
    *
    * @param subject the subject name
    * @return the latest schema information
    */
  def getLatestSchema(subject: String): IO[SchemaInfo] =
    for {
      baseUri <- parseBaseUri
      uri = baseUri / "subjects" / subject / "versions" / "latest"
      request = Request[IO](Method.GET, uri)
        .withHeaders(schemaRegistryAccept)
      response <- httpClient.expect[SubjectVersionResponse](request)(
        jsonOf[IO, SubjectVersionResponse]
      )
      info = SchemaInfo(
        id = response.id,
        schema = response.schema,
        schemaType = response.schemaType.getOrElse("AVRO"),
        version = response.version
      )
      _ <- putInCache(info.id, info, Instant.now())
      _ <- IO.delay(
        logger.info(
          s"Fetched latest schema for subject=$subject version=${info.version} id=${info.id}"
        )
      )
    } yield info

  private def fetchSchemaById(id: Int): IO[SchemaInfo] =
    for {
      baseUri <- parseBaseUri
      uri = baseUri / "schemas" / "ids" / id.toString
      request = Request[IO](Method.GET, uri)
        .withHeaders(schemaRegistryAccept)
      response <- httpClient.expect[SchemaByIdResponse](request)(
        jsonOf[IO, SchemaByIdResponse]
      )
      _ <- IO.delay(
        logger.debug(s"Fetched schema id=$id from registry")
      )
    } yield SchemaInfo(
      id = response.id,
      schema = response.schema,
      schemaType = response.schemaType.getOrElse("AVRO"),
      version = 0
    )

  private def putInCache(
      id: Int,
      info: SchemaInfo,
      now: Instant
  ): IO[Unit] =
    cache.update { current =>
      val withEviction = evictExpired(current, now)
      val updated = withEviction.updated(id, CachedSchema(info, now))
      if (updated.size > config.maxCacheSize) {
        evictOldest(updated)
      } else {
        updated
      }
    }

  private def isExpired(cached: CachedSchema, now: Instant): Boolean = {
    val age = java.time.Duration.between(cached.fetchedAt, now)
    age.toMillis > config.cacheTtl.toMillis
  }

  private def evictExpired(
      entries: Map[Int, CachedSchema],
      now: Instant
  ): Map[Int, CachedSchema] =
    entries.filter { case (_, cached) => !isExpired(cached, now) }

  private def evictOldest(
      entries: Map[Int, CachedSchema]
  ): Map[Int, CachedSchema] =
    if (entries.isEmpty) entries
    else {
      val oldestKey = entries.minBy(_._2.fetchedAt.toEpochMilli)._1
      entries.removed(oldestKey)
    }

  private def parseBaseUri: IO[Uri] =
    IO.fromEither(
      Uri
        .fromString(config.baseUrl)
        .leftMap(failure =>
          new IllegalArgumentException(
            s"Invalid schema registry base URL '${config.baseUrl}': ${failure.sanitized}"
          )
        )
    )
}

object HttpSchemaRegistryClient extends LazyLogging {

  /** Create a managed [[HttpSchemaRegistryClient]] that owns its own http4s
    * Ember HTTP client. Both the HTTP client and the schema registry client
    * are released when the resource is finalized.
    *
    * @param config the schema registry configuration
    * @return a resource yielding a configured schema registry client
    */
  def resource(
      config: SchemaRegistryConfig
  ): Resource[IO, HttpSchemaRegistryClient] =
    for {
      httpClient <- EmberClientBuilder
        .default[IO]
        .build
      cacheRef <- Resource.eval(Ref.of[IO, Map[Int, CachedSchema]](Map.empty))
      client <- Resource.make(
        IO.delay {
          logger.info(
            s"Creating HttpSchemaRegistryClient baseUrl=${config.baseUrl} cacheTtl=${config.cacheTtl} maxCacheSize=${config.maxCacheSize}"
          )
          new HttpSchemaRegistryClient(httpClient, config, cacheRef)
        }
      )(_ =>
        IO.delay(
          logger.info("HttpSchemaRegistryClient resource released")
        )
      )
    } yield client

  /** Create a [[HttpSchemaRegistryClient]] from an existing http4s client.
    * Useful when the HTTP client lifecycle is managed externally.
    *
    * @param httpClient the http4s client to use
    * @param config     the schema registry configuration
    * @return a resource yielding the schema registry client
    */
  def fromClient(
      httpClient: Client[IO],
      config: SchemaRegistryConfig
  ): Resource[IO, HttpSchemaRegistryClient] =
    for {
      cacheRef <- Resource.eval(Ref.of[IO, Map[Int, CachedSchema]](Map.empty))
      client <- Resource.make(
        IO.delay {
          logger.info(
            s"Creating HttpSchemaRegistryClient (external client) baseUrl=${config.baseUrl}"
          )
          new HttpSchemaRegistryClient(httpClient, config, cacheRef)
        }
      )(_ =>
        IO.delay(
          logger.info("HttpSchemaRegistryClient resource released")
        )
      )
    } yield client
}
