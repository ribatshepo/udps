import Dependencies._
import scalapb.compiler.Version.scalapbVersion

ThisBuild / scalaVersion := "2.13.12"
ThisBuild / organization := "io.gbmm"
ThisBuild / version := "0.1.0-SNAPSHOT"

// Compiler options
ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Wconf:msg=Block result was adapted via implicit conversion:s"
)

// Root project
lazy val root = (project in file("."))
  .settings(
    name := "udps",
    publish / skip := true
  )
  .aggregate(
    core,
    storage,
    query,
    catalog,
    governance,
    api,
    integration
  )

// Core module - domain models and abstractions
lazy val core = (project in file("udps-core"))
  .settings(
    name := "udps-core",
    libraryDependencies ++= coreDeps ++ testDeps
  )

// Storage module - storage layer implementations
lazy val storage = (project in file("udps-storage"))
  .settings(
    name := "udps-storage",
    libraryDependencies ++= coreDeps ++ storageDeps ++ testDeps
  )
  .dependsOn(core)

// Query module - query processing and optimization
lazy val query = (project in file("udps-query"))
  .settings(
    name := "udps-query",
    libraryDependencies ++= coreDeps ++ queryDeps ++ storageDeps ++ jsonDeps ++ testDeps
  )
  .dependsOn(core, storage)

// Catalog module - metadata catalog management
lazy val catalog = (project in file("udps-catalog"))
  .settings(
    name := "udps-catalog",
    libraryDependencies ++= coreDeps ++ storageDeps ++ messagingDeps ++ jsonDeps ++ testDeps
  )
  .dependsOn(core, storage)

// Governance module - data governance and lineage
lazy val governance = (project in file("udps-governance"))
  .settings(
    name := "udps-governance",
    libraryDependencies ++= coreDeps ++ storageDeps ++ messagingDeps ++ jsonDeps ++ testDeps
  )
  .dependsOn(core, catalog)

// API module - gRPC API layer
lazy val api = (project in file("udps-api"))
  .settings(
    name := "udps-api",
    libraryDependencies ++= coreDeps ++ grpcDeps ++ httpDeps ++ testDeps
  )
  .dependsOn(core, query, catalog, governance, integration)

// Integration module - protocol definitions (USP/UCCP)
lazy val integration = (project in file("udps-integration"))
  .settings(
    name := "udps-integration",
    libraryDependencies ++= coreDeps ++ grpcDeps ++ testDeps ++ Seq(
      "com.google.protobuf" % "protobuf-java" % "3.25.2" % "protobuf"
    ),
    Compile / PB.targets := Seq(
      scalapb.gen(
        flatPackage = false,
        javaConversions = false,
        grpc = true,
        singleLineToProtoString = false,
        asciiFormatToString = false
      ) -> (Compile / sourceManaged).value / "scalapb"
    ),
    Compile / PB.protoSources := Seq(
      (Compile / sourceDirectory).value / "protobuf"
    )
  )
  .dependsOn(core)
