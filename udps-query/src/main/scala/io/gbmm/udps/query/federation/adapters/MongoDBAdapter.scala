package io.gbmm.udps.query.federation.adapters

import cats.effect.IO
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.core.domain.DataType
import io.gbmm.udps.query.federation._
import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}
import org.apache.calcite.schema.{ScannableTable, Schema, Statistic, Statistics}
import org.bson.{BsonType, Document}

import java.util
import scala.jdk.CollectionConverters._

/** Federation adapter for MongoDB.
  *
  * Discovers collections as tables and infers schema by sampling documents.
  * Creates a Calcite AbstractSchema with ScannableTable implementations that
  * iterate over collection documents.
  */
final class MongoDBAdapter extends FederationAdapter with LazyLogging {

  private val MONGO_DEFAULT_PORT = 27017
  private val SCHEMA_SAMPLE_SIZE = 100

  override val sourceType: DataSourceType = DataSourceType.MongoDB

  override val pushdownCapabilities: PushdownCapabilities = PushdownCapabilities(
    predicates = true,
    projections = true,
    limits = true,
    aggregations = false,
    joins = false
  )

  override def connect(config: DataSourceConfig): IO[FederatedConnection] = IO {
    val effectivePort = if (config.port > 0) config.port else MONGO_DEFAULT_PORT
    val connectionString = buildConnectionString(config.copy(port = effectivePort))
    val client = MongoClients.create(connectionString)
    val database = client.getDatabase(config.database)
    logger.info(s"MongoDB connection established to ${config.host}:$effectivePort/${config.database}")
    new MongoFederatedConnection(client, database)
  }

  override def discoverTables(connection: FederatedConnection): IO[Seq[FederatedTableInfo]] = IO {
    val mongoConn = connection.asInstanceOf[MongoFederatedConnection]
    val db = mongoConn.database
    val collectionNames = db.listCollectionNames().into(new java.util.ArrayList[String]()).asScala.toSeq

    collectionNames.map { collName =>
      val collection = db.getCollection(collName)
      val sampleDocs = collection.find().limit(SCHEMA_SAMPLE_SIZE)
        .into(new java.util.ArrayList[Document]()).asScala.toSeq
      val columns = inferSchemaFromDocuments(sampleDocs)
      val estimatedRows = collection.estimatedDocumentCount()
      FederatedTableInfo(collName, columns, estimatedRows, DataSourceType.MongoDB)
    }
  }

  override def createCalciteSchema(connection: FederatedConnection): IO[Schema] = IO {
    val mongoConn = connection.asInstanceOf[MongoFederatedConnection]
    new MongoCalciteSchema(mongoConn.database)
  }

  private def buildConnectionString(config: DataSourceConfig): String = {
    val credentials = (config.username, config.password) match {
      case (Some(user), Some(pass)) =>
        val encodedUser = java.net.URLEncoder.encode(user, "UTF-8")
        val encodedPass = java.net.URLEncoder.encode(pass, "UTF-8")
        s"$encodedUser:$encodedPass@"
      case _ => ""
    }
    val authSource = config.properties.getOrElse("authSource", "admin")
    val extraParams = config.properties
      .filterNot(_._1 == "authSource")
      .map { case (k, v) => s"$k=$v" }
      .mkString("&")
    val paramString = if (credentials.nonEmpty) {
      val base = s"authSource=$authSource"
      if (extraParams.nonEmpty) s"$base&$extraParams" else base
    } else {
      extraParams
    }
    val queryPart = if (paramString.nonEmpty) s"?$paramString" else ""
    s"mongodb://$credentials${config.host}:${config.port}/${config.database}$queryPart"
  }

  private def inferSchemaFromDocuments(docs: Seq[Document]): Seq[FederatedColumnInfo] = {
    if (docs.isEmpty) return Seq(FederatedColumnInfo("_id", DataType.Utf8))

    val fieldTypes = new scala.collection.mutable.LinkedHashMap[String, DataType]()
    fieldTypes.put("_id", DataType.Utf8)

    docs.foreach { doc =>
      doc.entrySet().asScala.foreach { entry =>
        val key = entry.getKey
        if (!fieldTypes.contains(key)) {
          val bsonValue = doc.toBsonDocument.get(key)
          val dataType = mapBsonType(bsonValue.getBsonType)
          fieldTypes.put(key, dataType)
        }
      }
    }

    fieldTypes.map { case (name, dt) => FederatedColumnInfo(name, dt) }.toSeq
  }

  private def mapBsonType(bsonType: BsonType): DataType =
    bsonType match {
      case BsonType.STRING         => DataType.Utf8
      case BsonType.INT32          => DataType.Int32
      case BsonType.INT64          => DataType.Int64
      case BsonType.DOUBLE         => DataType.Float64
      case BsonType.BOOLEAN        => DataType.Boolean
      case BsonType.DATE_TIME      => DataType.TimestampMillis
      case BsonType.TIMESTAMP      => DataType.TimestampMillis
      case BsonType.OBJECT_ID      => DataType.Utf8
      case BsonType.BINARY         => DataType.Binary
      case BsonType.DECIMAL128     => DataType.Decimal(34, 15)
      case BsonType.ARRAY          => DataType.List(DataType.Utf8)
      case BsonType.DOCUMENT       => DataType.Utf8
      case BsonType.NULL           => DataType.Null
      case BsonType.UNDEFINED      => DataType.Null
      case BsonType.REGULAR_EXPRESSION => DataType.Utf8
      case _                       => DataType.Utf8
    }
}

object MongoDBAdapter {
  def apply(): MongoDBAdapter = new MongoDBAdapter()
}

/** FederatedConnection wrapping a MongoDB client and database handle. */
private[adapters] final class MongoFederatedConnection(
    val client: MongoClient,
    val database: MongoDatabase
) extends FederatedConnection {

  override val sourceType: DataSourceType = DataSourceType.MongoDB

  override def isConnected: IO[Boolean] = IO {
    try {
      database.runCommand(new Document("ping", 1))
      true
    } catch {
      case _: Exception => false
    }
  }

  override def close: IO[Unit] = IO {
    client.close()
  }

  override def metadata: Map[String, String] =
    Map(
      "databaseName" -> database.getName,
      "sourceType"   -> "MongoDB"
    )
}

/** Calcite Schema backed by a MongoDB database. Each collection becomes a ScannableTable. */
private[adapters] final class MongoCalciteSchema(database: MongoDatabase)
    extends AbstractSchema with LazyLogging {

  override def getTableMap: util.Map[String, org.apache.calcite.schema.Table] = {
    val collNames = database.listCollectionNames()
      .into(new java.util.ArrayList[String]()).asScala
    val result = new util.HashMap[String, org.apache.calcite.schema.Table]()
    collNames.foreach { name =>
      result.put(name, new MongoScannableTable(database, name))
    }
    result
  }
}

/** A Calcite ScannableTable that reads all documents from a MongoDB collection. */
private[adapters] final class MongoScannableTable(
    database: MongoDatabase,
    collectionName: String
) extends AbstractTable with ScannableTable with LazyLogging {

  private val SCHEMA_SAMPLE_SIZE = 100

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val collection = database.getCollection(collectionName)
    val sampleDocs = collection.find().limit(SCHEMA_SAMPLE_SIZE)
      .into(new java.util.ArrayList[Document]()).asScala.toSeq

    val builder = typeFactory.builder()
    val fieldTypes = new scala.collection.mutable.LinkedHashMap[String, RelDataType]()

    fieldTypes.put("_id", typeFactory.createJavaType(classOf[String]))

    sampleDocs.foreach { doc =>
      doc.entrySet().asScala.foreach { entry =>
        val key = entry.getKey
        if (!fieldTypes.contains(key)) {
          fieldTypes.put(key, typeFactory.createJavaType(classOf[String]))
        }
      }
    }

    fieldTypes.foreach { case (name, relType) =>
      builder.add(name, typeFactory.createTypeWithNullability(relType, true))
    }
    builder.build()
  }

  override def scan(root: DataContext): Enumerable[Array[AnyRef]] = {
    val collection = database.getCollection(collectionName)
    val fieldOrder = getFieldOrder

    new AbstractEnumerable[Array[AnyRef]] {
      override def enumerator(): Enumerator[Array[AnyRef]] = {
        val cursor = collection.find().iterator()
        new Enumerator[Array[AnyRef]] {
          private var currentRow: Array[AnyRef] = _

          override def current(): Array[AnyRef] = currentRow

          override def moveNext(): Boolean = {
            if (cursor.hasNext) {
              val doc = cursor.next()
              currentRow = fieldOrder.map { fieldName =>
                val value = doc.get(fieldName)
                if (value != null) value.toString.asInstanceOf[AnyRef]
                else null
              }.toArray
              true
            } else {
              false
            }
          }

          override def reset(): Unit =
            throw new UnsupportedOperationException("Reset not supported on MongoDB cursor enumerator")

          override def close(): Unit = ()
        }
      }
    }
  }

  override def getStatistic: Statistic = Statistics.UNKNOWN

  private def getFieldOrder: Seq[String] = {
    val collection = database.getCollection(collectionName)
    val sampleDocs = collection.find().limit(SCHEMA_SAMPLE_SIZE)
      .into(new java.util.ArrayList[Document]()).asScala.toSeq
    val fields = new scala.collection.mutable.LinkedHashSet[String]()
    fields.add("_id")
    sampleDocs.foreach { doc =>
      doc.keySet().asScala.foreach(fields.add)
    }
    fields.toSeq
  }
}
