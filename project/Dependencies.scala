import sbt._

object Dependencies {
  // Versions
  object Versions {
    val scala = "2.13.12"
    val scalapb = "0.11.15"
    val grpc = "1.60.1"
    val cats = "2.10.0"
    val catsEffect = "3.5.3"
    val fs2 = "3.9.4"
    val http4s = "0.23.25"
    val circe = "0.14.6"
    val pureconfig = "0.17.5"
    val doobie = "1.0.0-RC5"
    val minio = "8.5.7"
    val kafka = "3.6.1"
    val redis = "4.6.0"
    val logback = "1.4.14"
    val scalaLogging = "3.9.5"
    val scalatest = "3.2.17"
    val testcontainers = "0.41.2"
    val arrow = "14.0.1"
    val parquet = "1.13.1"
    val hadoop = "3.3.6"
    val lucene = "9.9.2"
    val roaringBitmap = "0.9.45"
  }
  
  // Core dependencies
  val scalapbRuntime = "com.thesamet.scalapb" %% "scalapb-runtime" % Versions.scalapb
  val scalapbRuntimeGrpc = "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % Versions.scalapb
  val grpcNetty = "io.grpc" % "grpc-netty" % Versions.grpc
  val grpcServices = "io.grpc" % "grpc-services" % Versions.grpc
  
  // Cats ecosystem
  val catsCore = "org.typelevel" %% "cats-core" % Versions.cats
  val catsEffect = "org.typelevel" %% "cats-effect" % Versions.catsEffect
  val fs2Core = "co.fs2" %% "fs2-core" % Versions.fs2
  val fs2Io = "co.fs2" %% "fs2-io" % Versions.fs2
  
  // HTTP
  val http4sDsl = "org.http4s" %% "http4s-dsl" % Versions.http4s
  val http4sEmberServer = "org.http4s" %% "http4s-ember-server" % Versions.http4s
  val http4sEmberClient = "org.http4s" %% "http4s-ember-client" % Versions.http4s
  val http4sCirce = "org.http4s" %% "http4s-circe" % Versions.http4s
  
  // JSON
  val circeCore = "io.circe" %% "circe-core" % Versions.circe
  val circeGeneric = "io.circe" %% "circe-generic" % Versions.circe
  val circeParser = "io.circe" %% "circe-parser" % Versions.circe
  
  // Config
  val pureconfig = "com.github.pureconfig" %% "pureconfig" % Versions.pureconfig
  val pureconfigCatsEffect = "com.github.pureconfig" %% "pureconfig-cats-effect" % Versions.pureconfig
  
  // Database
  val doobieCore = "org.tpolecat" %% "doobie-core" % Versions.doobie
  val doobieHikari = "org.tpolecat" %% "doobie-hikari" % Versions.doobie
  val doobiePostgres = "org.tpolecat" %% "doobie-postgres" % Versions.doobie
  
  // Storage
  val minio = "io.minio" % "minio" % Versions.minio
  
  // Messaging
  val kafkaClients = "org.apache.kafka" % "kafka-clients" % Versions.kafka
  val fs2Kafka = "com.github.fd4s" %% "fs2-kafka" % "3.2.0"
  
  // Cache
  val redis4cats = "dev.profunktor" %% "redis4cats-effects" % Versions.redis
  
  // Logging
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging
  val logbackClassic = "ch.qos.logback" % "logback-classic" % Versions.logback
  
  // Apache Arrow
  val arrowVector = "org.apache.arrow" % "arrow-vector" % Versions.arrow
  val arrowMemory = "org.apache.arrow" % "arrow-memory-netty" % Versions.arrow
  val arrowAlgorithm = "org.apache.arrow" % "arrow-algorithm" % Versions.arrow
  
  // Apache Parquet
  val parquetHadoop = "org.apache.parquet" % "parquet-hadoop" % Versions.parquet
  val parquetColumn = "org.apache.parquet" % "parquet-column" % Versions.parquet
  val parquetAvro = "org.apache.parquet" % "parquet-avro" % Versions.parquet
  
  // Hadoop (minimal for Parquet)
  val hadoopCommon = "org.apache.hadoop" % "hadoop-common" % Versions.hadoop exclude("org.slf4j", "slf4j-log4j12")
  
  // Apache Lucene
  val luceneCore = "org.apache.lucene" % "lucene-core" % Versions.lucene
  val luceneQueryParser = "org.apache.lucene" % "lucene-queryparser" % Versions.lucene
  val luceneAnalysisCommon = "org.apache.lucene" % "lucene-analysis-common" % Versions.lucene
  val luceneAnalysisPhonetic = "org.apache.lucene" % "lucene-analysis-phonetic" % Versions.lucene
  
  // RoaringBitmap
  val roaringBitmap = "org.roaringbitmap" % "RoaringBitmap" % Versions.roaringBitmap
  
  // Testing
  val scalatest = "org.scalatest" %% "scalatest" % Versions.scalatest % Test
  val testcontainers = "com.dimafeng" %% "testcontainers-scala-scalatest" % Versions.testcontainers % Test
  val testcontainersPostgres = "com.dimafeng" %% "testcontainers-scala-postgresql" % Versions.testcontainers % Test
  val testcontainersKafka = "com.dimafeng" %% "testcontainers-scala-kafka" % Versions.testcontainers % Test
  
  // Dependency groups
  val coreDeps = Seq(
    catsCore,
    catsEffect,
    fs2Core,
    fs2Io,
    scalaLogging,
    logbackClassic,
    pureconfig,
    pureconfigCatsEffect
  )
  
  val grpcDeps = Seq(
    scalapbRuntime,
    scalapbRuntimeGrpc,
    grpcNetty,
    grpcServices
  )
  
  val httpDeps = Seq(
    http4sDsl,
    http4sEmberServer,
    http4sEmberClient,
    http4sCirce
  )
  
  val jsonDeps = Seq(
    circeCore,
    circeGeneric,
    circeParser
  )
  
  val storageDeps = Seq(
    minio,
    doobieCore,
    doobieHikari,
    doobiePostgres,
    arrowVector,
    arrowMemory,
    arrowAlgorithm,
    parquetHadoop,
    parquetColumn,
    parquetAvro,
    hadoopCommon,
    luceneCore,
    luceneQueryParser,
    luceneAnalysisCommon,
    luceneAnalysisPhonetic,
    roaringBitmap
  )
  
  val messagingDeps = Seq(
    kafkaClients,
    fs2Kafka
  )
  
  val cacheDeps = Seq(
    redis4cats
  )
  
  val testDeps = Seq(
    scalatest,
    testcontainers,
    testcontainersPostgres,
    testcontainersKafka
  )
}
