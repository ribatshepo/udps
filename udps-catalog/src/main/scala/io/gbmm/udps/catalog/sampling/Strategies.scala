package io.gbmm.udps.catalog.sampling

import java.time.Instant
import java.util.UUID

import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._

sealed trait SamplingStrategy

object SamplingStrategy {

  final case class RandomSampling(
      sampleSize: Int,
      seed: Long
  ) extends SamplingStrategy

  final case class StratifiedSampling(
      sampleSize: Int,
      stratumColumn: String,
      seed: Long
  ) extends SamplingStrategy

  final case class SystematicSampling(
      every: Int
  ) extends SamplingStrategy

  final case class ClusterSampling(
      clusterColumn: String,
      numClusters: Int,
      seed: Long
  ) extends SamplingStrategy

  final case class TimeBasedSampling(
      startTime: Instant,
      endTime: Instant,
      timeColumn: String
  ) extends SamplingStrategy

  implicit val instantEncoder: Encoder[Instant] =
    Encoder.encodeString.contramap[Instant](_.toString)

  implicit val instantDecoder: Decoder[Instant] =
    Decoder.decodeString.emap { s =>
      try Right(Instant.parse(s))
      catch { case e: Exception => Left(s"Invalid instant: ${e.getMessage}") }
    }

  implicit val randomEncoder: Encoder[RandomSampling] = deriveEncoder[RandomSampling]
  implicit val randomDecoder: Decoder[RandomSampling] = deriveDecoder[RandomSampling]

  implicit val stratifiedEncoder: Encoder[StratifiedSampling] = deriveEncoder[StratifiedSampling]
  implicit val stratifiedDecoder: Decoder[StratifiedSampling] = deriveDecoder[StratifiedSampling]

  implicit val systematicEncoder: Encoder[SystematicSampling] = deriveEncoder[SystematicSampling]
  implicit val systematicDecoder: Decoder[SystematicSampling] = deriveDecoder[SystematicSampling]

  implicit val clusterEncoder: Encoder[ClusterSampling] = deriveEncoder[ClusterSampling]
  implicit val clusterDecoder: Decoder[ClusterSampling] = deriveDecoder[ClusterSampling]

  implicit val timeBasedEncoder: Encoder[TimeBasedSampling] = deriveEncoder[TimeBasedSampling]
  implicit val timeBasedDecoder: Decoder[TimeBasedSampling] = deriveDecoder[TimeBasedSampling]

  implicit val strategyEncoder: Encoder[SamplingStrategy] = Encoder.instance {
    case r: RandomSampling     => Json.obj("type" -> "random".asJson, "config" -> r.asJson)
    case s: StratifiedSampling => Json.obj("type" -> "stratified".asJson, "config" -> s.asJson)
    case s: SystematicSampling => Json.obj("type" -> "systematic".asJson, "config" -> s.asJson)
    case c: ClusterSampling    => Json.obj("type" -> "cluster".asJson, "config" -> c.asJson)
    case t: TimeBasedSampling  => Json.obj("type" -> "timeBased".asJson, "config" -> t.asJson)
  }

  implicit val strategyDecoder: Decoder[SamplingStrategy] = Decoder.instance { cursor =>
    cursor.downField("type").as[String].flatMap {
      case "random"     => cursor.downField("config").as[RandomSampling]
      case "stratified" => cursor.downField("config").as[StratifiedSampling]
      case "systematic" => cursor.downField("config").as[SystematicSampling]
      case "cluster"    => cursor.downField("config").as[ClusterSampling]
      case "timeBased"  => cursor.downField("config").as[TimeBasedSampling]
      case other        => Left(DecodingFailure(s"Unknown strategy type: $other", cursor.history))
    }
  }
}

final case class SampleMetadata(
    id: UUID,
    strategy: SamplingStrategy,
    originalRowCount: Long,
    sampleRowCount: Long,
    seed: Option[Long],
    createdAt: Instant
)

object SampleMetadata {
  import SamplingStrategy.{instantEncoder, instantDecoder, strategyEncoder, strategyDecoder}

  implicit val uuidEncoder: Encoder[UUID] =
    Encoder.encodeString.contramap[UUID](_.toString)

  implicit val uuidDecoder: Decoder[UUID] =
    Decoder.decodeString.emap { s =>
      try Right(UUID.fromString(s))
      catch { case e: Exception => Left(s"Invalid UUID: ${e.getMessage}") }
    }

  implicit val encoder: Encoder[SampleMetadata] = deriveEncoder[SampleMetadata]
  implicit val decoder: Decoder[SampleMetadata] = deriveDecoder[SampleMetadata]
}
