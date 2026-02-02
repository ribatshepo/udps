package io.gbmm.udps.governance.audit

import java.time.Instant
import java.util.{Base64, UUID}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import cats.effect.{Clock, IO}
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import cats.syntax.functor._
import io.circe._

sealed trait AuditAction extends Product with Serializable

object AuditAction {
  case object DataAccess extends AuditAction
  case object SchemaChange extends AuditAction
  case object DataModification extends AuditAction
  case object AdminOperation extends AuditAction
  case object SecurityEvent extends AuditAction

  private val actionMap: Map[String, AuditAction] = Map(
    "DataAccess" -> DataAccess,
    "SchemaChange" -> SchemaChange,
    "DataModification" -> DataModification,
    "AdminOperation" -> AdminOperation,
    "SecurityEvent" -> SecurityEvent
  )

  def asString(action: AuditAction): String = action match {
    case DataAccess       => "DataAccess"
    case SchemaChange     => "SchemaChange"
    case DataModification => "DataModification"
    case AdminOperation   => "AdminOperation"
    case SecurityEvent    => "SecurityEvent"
  }

  def fromString(s: String): AuditAction =
    actionMap.getOrElse(s, throw new IllegalArgumentException(s"Unknown AuditAction: $s"))

  implicit val putAuditAction: Put[AuditAction] =
    Put[String].contramap(asString)
  implicit val getAuditAction: Get[AuditAction] =
    Get[String].map(fromString)

  implicit val encodeAuditAction: Encoder[AuditAction] =
    Encoder.encodeString.contramap[AuditAction](asString)

  implicit val decodeAuditAction: Decoder[AuditAction] = Decoder.decodeString.emap { s =>
    actionMap.get(s).toRight(s"Unknown AuditAction: $s")
  }
}

final case class AuditEntry(
    id: UUID,
    userId: String,
    action: AuditAction,
    resource: String,
    timestamp: Instant,
    ipAddress: Option[String],
    userAgent: Option[String],
    status: String,
    details: Json,
    hmacSignature: String
)

object AuditEntry {

  implicit val putJson: Put[Json] =
    Put[String].contramap(_.noSpaces)
  implicit val getJson: Get[Json] =
    Get[String].map(s => io.circe.parser.parse(s).getOrElse(Json.Null))

  implicit val encodeAuditEntry: Encoder[AuditEntry] = Encoder.forProduct10(
    "id", "userId", "action", "resource", "timestamp",
    "ipAddress", "userAgent", "status", "details", "hmacSignature"
  )(e => (e.id, e.userId, e.action, e.resource, e.timestamp,
    e.ipAddress, e.userAgent, e.status, e.details, e.hmacSignature))

  implicit val decodeAuditEntry: Decoder[AuditEntry] = Decoder.forProduct10(
    "id", "userId", "action", "resource", "timestamp",
    "ipAddress", "userAgent", "status", "details", "hmacSignature"
  )(AuditEntry.apply)
}

class AuditLogger(xa: Transactor[IO], hmacKey: Array[Byte]) extends LazyLogging {

  import AuditEntry._

  private val hmacAlgorithm = "HmacSHA256"

  def log(
      userId: String,
      action: AuditAction,
      resource: String,
      status: String,
      details: Json,
      ipAddress: Option[String] = None,
      userAgent: Option[String] = None
  ): IO[AuditEntry] =
    for {
      id <- IO.delay(UUID.randomUUID())
      now <- Clock[IO].realTimeInstant
      hmac = computeHmac(buildHmacPayload(id, userId, action, resource, now, details), hmacKey)
      entry = AuditEntry(id, userId, action, resource, now, ipAddress, userAgent, status, details, hmac)
      _ <- insertEntry(entry).transact(xa)
      _ <- IO.delay(logger.info(s"Audit logged: action=${AuditAction.asString(action)}, user=$userId, resource=$resource, status=$status"))
    } yield entry

  def verifyIntegrity(entry: AuditEntry): Boolean = {
    val expected = computeHmac(
      buildHmacPayload(entry.id, entry.userId, entry.action, entry.resource, entry.timestamp, entry.details),
      hmacKey
    )
    java.security.MessageDigest.isEqual(expected.getBytes("UTF-8"), entry.hmacSignature.getBytes("UTF-8"))
  }

  def getByResource(resource: String, limit: Int = 100): IO[Seq[AuditEntry]] =
    sql"""SELECT id, user_id, action, resource, timestamp, ip_address, user_agent, status, details, hmac_signature
          FROM audit_log WHERE resource = $resource ORDER BY timestamp DESC LIMIT $limit"""
      .query[AuditEntry].to[List].transact(xa).map(_.toSeq)

  def getByUser(userId: String, limit: Int = 100): IO[Seq[AuditEntry]] =
    sql"""SELECT id, user_id, action, resource, timestamp, ip_address, user_agent, status, details, hmac_signature
          FROM audit_log WHERE user_id = $userId ORDER BY timestamp DESC LIMIT $limit"""
      .query[AuditEntry].to[List].transact(xa).map(_.toSeq)

  def applyRetention(retentionYears: Int = 7): IO[Long] = {
    val cutoff = Instant.now().minusSeconds(retentionYears.toLong * 365L * 24L * 3600L)
    sql"""DELETE FROM audit_log WHERE timestamp < $cutoff"""
      .update.run.transact(xa).map(_.toLong)
  }

  private def insertEntry(entry: AuditEntry): ConnectionIO[Unit] =
    sql"""INSERT INTO audit_log (id, user_id, action, resource, timestamp, ip_address, user_agent, status, details, hmac_signature)
          VALUES (${entry.id}, ${entry.userId}, ${entry.action}, ${entry.resource}, ${entry.timestamp},
                  ${entry.ipAddress}, ${entry.userAgent}, ${entry.status}, ${entry.details}, ${entry.hmacSignature})"""
      .update.run.void

  private def buildHmacPayload(
      id: UUID,
      userId: String,
      action: AuditAction,
      resource: String,
      timestamp: Instant,
      details: Json
  ): String =
    s"${id.toString}|$userId|${AuditAction.asString(action)}|$resource|${timestamp.toString}|${details.noSpaces}"

  private def computeHmac(data: String, key: Array[Byte]): String = {
    val mac = Mac.getInstance(hmacAlgorithm)
    mac.init(new SecretKeySpec(key, hmacAlgorithm))
    Base64.getEncoder.encodeToString(mac.doFinal(data.getBytes("UTF-8")))
  }
}
