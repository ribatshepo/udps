package io.gbmm.udps.query.federation.adapters

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.core.domain.DataType
import io.gbmm.udps.query.federation._
import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}
import org.apache.calcite.schema.{ScannableTable, Schema, Statistic, Statistics}

import java.io.{BufferedReader, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.util
import scala.util.{Try, Using}

/** Federation adapter for Elasticsearch.
  *
  * Connects via HTTP REST API, discovers indices as tables, and maps Elasticsearch
  * field mappings to UDPS DataType. Creates Calcite ScannableTable implementations
  * that query Elasticsearch via the _search endpoint.
  */
final class ElasticsearchAdapter extends FederationAdapter with LazyLogging {

  private val ES_DEFAULT_PORT = 9200

  override val sourceType: DataSourceType = DataSourceType.Elasticsearch

  override val pushdownCapabilities: PushdownCapabilities = PushdownCapabilities(
    predicates = true,
    projections = true,
    limits = true,
    aggregations = false,
    joins = false
  )

  override def connect(config: DataSourceConfig): IO[FederatedConnection] = IO {
    val effectivePort = if (config.port > 0) config.port else ES_DEFAULT_PORT
    val scheme = config.properties.getOrElse("scheme", "http")
    val baseUrl = s"$scheme://${config.host}:$effectivePort"

    val clusterInfo = httpGet(s"$baseUrl/", config.username, config.password)
    logger.info(s"Elasticsearch connection established to $baseUrl")

    new ElasticsearchFederatedConnection(
      baseUrl = baseUrl,
      indexPattern = config.database,
      username = config.username,
      password = config.password,
      clusterInfo = clusterInfo
    )
  }

  override def discoverTables(connection: FederatedConnection): IO[Seq[FederatedTableInfo]] = IO {
    val esConn = connection.asInstanceOf[ElasticsearchFederatedConnection]
    val mappingsJson = httpGet(
      s"${esConn.baseUrl}/${esConn.indexPattern}/_mapping",
      esConn.username,
      esConn.password
    )

    val indices = parseIndicesFromMappingResponse(mappingsJson)
    indices.map { case (indexName, fields) =>
      val columns = fields.map { case (fieldName, esType) =>
        FederatedColumnInfo(fieldName, mapEsType(esType))
      }
      val countJson = httpGet(
        s"${esConn.baseUrl}/$indexName/_count",
        esConn.username,
        esConn.password
      )
      val rowCount = extractCount(countJson)
      FederatedTableInfo(indexName, columns, rowCount, DataSourceType.Elasticsearch)
    }.toSeq
  }

  override def createCalciteSchema(connection: FederatedConnection): IO[Schema] = IO {
    val esConn = connection.asInstanceOf[ElasticsearchFederatedConnection]
    new ElasticsearchCalciteSchema(esConn)
  }

  private[adapters] def mapEsType(esType: String): DataType =
    esType.toLowerCase match {
      case "keyword" | "text" | "wildcard" | "constant_keyword" => DataType.Utf8
      case "long"                 => DataType.Int64
      case "integer"              => DataType.Int32
      case "short"                => DataType.Int16
      case "byte"                 => DataType.Int8
      case "double"               => DataType.Float64
      case "float" | "half_float" => DataType.Float32
      case "scaled_float"         => DataType.Float64
      case "boolean"              => DataType.Boolean
      case "date" | "date_nanos"  => DataType.TimestampMillis
      case "binary"               => DataType.Binary
      case "ip"                   => DataType.Utf8
      case "nested" | "object"    => DataType.Utf8
      case "geo_point" | "geo_shape" => DataType.Utf8
      case "unsigned_long"        => DataType.UInt64
      case _                      => DataType.Utf8
    }

  private[adapters] def httpGet(
      url: String,
      username: Option[String],
      password: Option[String]
  ): String = {
    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    try {
      connection.setRequestMethod("GET")
      connection.setRequestProperty("Accept", "application/json")
      connection.setConnectTimeout(10000)
      connection.setReadTimeout(30000)

      (username, password) match {
        case (Some(user), Some(pass)) =>
          val credentials = java.util.Base64.getEncoder
            .encodeToString(s"$user:$pass".getBytes(StandardCharsets.UTF_8))
          connection.setRequestProperty("Authorization", s"Basic $credentials")
        case _ => ()
      }

      val responseCode = connection.getResponseCode
      val stream = if (responseCode >= 200 && responseCode < 300) {
        connection.getInputStream
      } else {
        connection.getErrorStream
      }

      Using.resource(new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) { reader =>
        val sb = new StringBuilder
        var line: String = reader.readLine()
        while (line != null) {
          sb.append(line)
          line = reader.readLine()
        }
        sb.toString()
      }
    } finally {
      connection.disconnect()
    }
  }

  private[adapters] def httpPost(
      url: String,
      body: String,
      username: Option[String],
      password: Option[String]
  ): String = {
    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    try {
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Content-Type", "application/json")
      connection.setRequestProperty("Accept", "application/json")
      connection.setDoOutput(true)
      connection.setConnectTimeout(10000)
      connection.setReadTimeout(30000)

      (username, password) match {
        case (Some(user), Some(pass)) =>
          val credentials = java.util.Base64.getEncoder
            .encodeToString(s"$user:$pass".getBytes(StandardCharsets.UTF_8))
          connection.setRequestProperty("Authorization", s"Basic $credentials")
        case _ => ()
      }

      Using.resource(connection.getOutputStream) { os =>
        os.write(body.getBytes(StandardCharsets.UTF_8))
        os.flush()
      }

      val responseCode = connection.getResponseCode
      val stream = if (responseCode >= 200 && responseCode < 300) {
        connection.getInputStream
      } else {
        connection.getErrorStream
      }

      Using.resource(new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) { reader =>
        val sb = new StringBuilder
        var line: String = reader.readLine()
        while (line != null) {
          sb.append(line)
          line = reader.readLine()
        }
        sb.toString()
      }
    } finally {
      connection.disconnect()
    }
  }

  /** Parse the ES _mapping response to extract index names and their field-to-type mappings.
    * Uses Circe for JSON parsing.
    */
  private def parseIndicesFromMappingResponse(json: String): Map[String, Seq[(String, String)]] = {
    import io.circe.parser._
    parse(json).toOption match {
      case Some(root) =>
        val obj = root.asObject.getOrElse(io.circe.JsonObject.empty)
        obj.toMap.flatMap { case (indexName, indexJson) =>
          val mappings = indexJson.hcursor
            .downField("mappings")
            .downField("properties")
            .focus
            .flatMap(_.asObject)

          mappings.map { props =>
            val fields = props.toMap.flatMap { case (fieldName, fieldJson) =>
              fieldJson.hcursor.downField("type").as[String].toOption.map(fieldName -> _)
            }.toSeq
            indexName -> fields
          }
        }
      case None =>
        logger.warn(s"Failed to parse Elasticsearch mapping response")
        Map.empty
    }
  }

  private def extractCount(json: String): Long = {
    import io.circe.parser._
    parse(json).toOption
      .flatMap(_.hcursor.downField("count").as[Long].toOption)
      .getOrElse(-1L)
  }
}

object ElasticsearchAdapter {
  def apply(): ElasticsearchAdapter = new ElasticsearchAdapter()
}

/** FederatedConnection for Elasticsearch via REST. */
private[adapters] final class ElasticsearchFederatedConnection(
    val baseUrl: String,
    val indexPattern: String,
    val username: Option[String],
    val password: Option[String],
    clusterInfo: String
) extends FederatedConnection {

  override val sourceType: DataSourceType = DataSourceType.Elasticsearch

  override def isConnected: IO[Boolean] = IO {
    Try {
      val adapter = new ElasticsearchAdapter()
      val response = adapter.httpGet(s"$baseUrl/_cluster/health", username, password)
      response.contains("cluster_name")
    }.getOrElse(false)
  }

  override def close: IO[Unit] = IO.unit

  override def metadata: Map[String, String] =
    Map(
      "baseUrl"      -> baseUrl,
      "indexPattern"  -> indexPattern,
      "sourceType"    -> "Elasticsearch"
    )
}

/** Calcite Schema backed by Elasticsearch indices. */
private[adapters] final class ElasticsearchCalciteSchema(
    esConn: ElasticsearchFederatedConnection
) extends AbstractSchema with LazyLogging {

  override def getTableMap: util.Map[String, org.apache.calcite.schema.Table] = {
    val adapter = new ElasticsearchAdapter()
    val mappingsJson = adapter.httpGet(
      s"${esConn.baseUrl}/${esConn.indexPattern}/_mapping",
      esConn.username,
      esConn.password
    )

    import io.circe.parser._
    val result = new util.HashMap[String, org.apache.calcite.schema.Table]()

    parse(mappingsJson).toOption.foreach { root =>
      root.asObject.foreach { obj =>
        obj.keys.foreach { indexName =>
          result.put(indexName, new ElasticsearchScannableTable(esConn, indexName, adapter))
        }
      }
    }
    result
  }
}

/** Calcite ScannableTable that reads documents from an Elasticsearch index via _search. */
private[adapters] final class ElasticsearchScannableTable(
    esConn: ElasticsearchFederatedConnection,
    indexName: String,
    adapter: ElasticsearchAdapter
) extends AbstractTable with ScannableTable with LazyLogging {

  private val DEFAULT_FETCH_SIZE = 1000

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val mappingJson = adapter.httpGet(
      s"${esConn.baseUrl}/$indexName/_mapping",
      esConn.username,
      esConn.password
    )

    import io.circe.parser._
    val builder = typeFactory.builder()
    builder.add("_id", typeFactory.createJavaType(classOf[String]))

    parse(mappingJson).toOption.foreach { root =>
      root.hcursor
        .downField(indexName)
        .downField("mappings")
        .downField("properties")
        .focus
        .flatMap(_.asObject)
        .foreach { props =>
          props.toMap.foreach { case (fieldName, fieldJson) =>
            fieldJson.hcursor.downField("type").as[String].toOption.foreach { _ =>
              builder.add(
                fieldName,
                typeFactory.createTypeWithNullability(
                  typeFactory.createJavaType(classOf[String]),
                  true
                )
              )
            }
          }
        }
    }
    builder.build()
  }

  override def scan(root: DataContext): Enumerable[Array[AnyRef]] = {
    val searchBody = s"""{"size":$DEFAULT_FETCH_SIZE,"query":{"match_all":{}}}"""
    val responseJson = adapter.httpPost(
      s"${esConn.baseUrl}/$indexName/_search",
      searchBody,
      esConn.username,
      esConn.password
    )

    import io.circe.parser._
    val hits = parse(responseJson).toOption
      .flatMap(_.hcursor.downField("hits").downField("hits").focus)
      .flatMap(_.asArray)
      .getOrElse(Vector.empty)

    val fieldOrder = getFieldOrder

    new AbstractEnumerable[Array[AnyRef]] {
      override def enumerator(): Enumerator[Array[AnyRef]] = {
        val iter = hits.iterator
        new Enumerator[Array[AnyRef]] {
          private var currentRow: Array[AnyRef] = _

          override def current(): Array[AnyRef] = currentRow

          override def moveNext(): Boolean = {
            if (iter.hasNext) {
              val hit = iter.next()
              val id = hit.hcursor.downField("_id").as[String].getOrElse("")
              val source = hit.hcursor.downField("_source").focus
                .flatMap(_.asObject)
                .getOrElse(io.circe.JsonObject.empty)

              currentRow = fieldOrder.map {
                case "_id" => id.asInstanceOf[AnyRef]
                case field =>
                  source(field)
                    .map(_.noSpaces.stripPrefix("\"").stripSuffix("\""))
                    .orNull
                    .asInstanceOf[AnyRef]
              }.toArray
              true
            } else {
              false
            }
          }

          override def reset(): Unit =
            throw new UnsupportedOperationException("Reset not supported on ES enumerator")

          override def close(): Unit = ()
        }
      }
    }
  }

  override def getStatistic: Statistic = Statistics.UNKNOWN

  private def getFieldOrder: Seq[String] = {
    val mappingJson = adapter.httpGet(
      s"${esConn.baseUrl}/$indexName/_mapping",
      esConn.username,
      esConn.password
    )
    import io.circe.parser._
    val fields = Seq("_id") ++ parse(mappingJson).toOption
      .flatMap(_.hcursor.downField(indexName).downField("mappings").downField("properties").focus)
      .flatMap(_.asObject)
      .map(_.keys.toSeq)
      .getOrElse(Seq.empty)
    fields
  }
}
