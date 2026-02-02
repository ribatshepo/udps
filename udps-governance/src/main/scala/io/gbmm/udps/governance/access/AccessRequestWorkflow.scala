package io.gbmm.udps.governance.access

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

/** The type of access being requested. */
sealed trait AccessType extends Product with Serializable
object AccessType {
  final case class TemporaryAccess(ttlMinutes: Int) extends AccessType
  case object PermanentAccess extends AccessType
  case object ExportRequest extends AccessType
}

/** State machine states for an access request. */
sealed trait RequestState extends Product with Serializable
object RequestState {
  case object Submitted extends RequestState
  case object PendingApproval extends RequestState
  case object Approved extends RequestState
  case object Rejected extends RequestState
  case object Expired extends RequestState
  case object Revoked extends RequestState

  def fromString(s: String): RequestState = s match {
    case "Submitted"       => Submitted
    case "PendingApproval" => PendingApproval
    case "Approved"        => Approved
    case "Rejected"        => Rejected
    case "Expired"         => Expired
    case "Revoked"         => Revoked
    case other             =>
      throw new IllegalArgumentException(s"Unknown request state: $other")
  }

  def asString(state: RequestState): String = state match {
    case Submitted       => "Submitted"
    case PendingApproval => "PendingApproval"
    case Approved        => "Approved"
    case Rejected        => "Rejected"
    case Expired         => "Expired"
    case Revoked         => "Revoked"
  }
}

/** An access request with its full lifecycle data. */
final case class AccessRequest(
    id: UUID,
    requesterId: String,
    resourceName: String,
    accessType: AccessType,
    state: RequestState,
    reason: String,
    approvedBy: Option[String],
    createdAt: Instant,
    updatedAt: Instant,
    expiresAt: Option[Instant]
)

/** Persistence abstraction for access requests. */
trait AccessRequestStore {
  def insert(request: AccessRequest): IO[Unit]
  def findById(id: UUID): IO[Option[AccessRequest]]
  def update(request: AccessRequest): IO[Unit]
  def listByRequester(requesterId: String): IO[Seq[AccessRequest]]
  def listByState(state: RequestState): IO[Seq[AccessRequest]]
  def listExpiredTemporary(now: Instant): IO[Seq[AccessRequest]]
}

/** Doobie-backed implementation of AccessRequestStore. */
final class DoobieAccessRequestStore(xa: Transactor[IO])
    extends AccessRequestStore
    with LazyLogging {

  implicit val requestStatePut: Put[RequestState] =
    Put[String].contramap(RequestState.asString)
  implicit val requestStateGet: Get[RequestState] =
    Get[String].map(RequestState.fromString)

  implicit val accessTypePut: Put[AccessType] =
    Put[String].contramap(encodeAccessType)
  implicit val accessTypeGet: Get[AccessType] =
    Get[String].map(decodeAccessType)

  override def insert(request: AccessRequest): IO[Unit] =
    sql"""INSERT INTO access_requests
            (id, requester_id, resource_name, access_type, state, reason,
             approved_by, created_at, updated_at, expires_at)
          VALUES (${request.id}, ${request.requesterId}, ${request.resourceName},
                  ${request.accessType}, ${request.state}, ${request.reason},
                  ${request.approvedBy}, ${request.createdAt}, ${request.updatedAt},
                  ${request.expiresAt})"""
      .update.run.transact(xa).void

  override def findById(id: UUID): IO[Option[AccessRequest]] =
    sql"""SELECT id, requester_id, resource_name, access_type, state, reason,
                 approved_by, created_at, updated_at, expires_at
          FROM access_requests WHERE id = $id"""
      .query[AccessRequest].option.transact(xa)

  override def update(request: AccessRequest): IO[Unit] =
    sql"""UPDATE access_requests
          SET state = ${request.state}, approved_by = ${request.approvedBy},
              updated_at = ${request.updatedAt}, expires_at = ${request.expiresAt}
          WHERE id = ${request.id}"""
      .update.run.transact(xa).flatMap { count =>
        if (count == 1) IO.unit
        else IO.raiseError(
          new RuntimeException(s"Access request ${request.id} not found for update")
        )
      }

  override def listByRequester(requesterId: String): IO[Seq[AccessRequest]] =
    sql"""SELECT id, requester_id, resource_name, access_type, state, reason,
                 approved_by, created_at, updated_at, expires_at
          FROM access_requests WHERE requester_id = $requesterId
          ORDER BY created_at DESC"""
      .query[AccessRequest].to[List].transact(xa).map(_.toSeq)

  override def listByState(state: RequestState): IO[Seq[AccessRequest]] =
    sql"""SELECT id, requester_id, resource_name, access_type, state, reason,
                 approved_by, created_at, updated_at, expires_at
          FROM access_requests WHERE state = $state
          ORDER BY created_at ASC"""
      .query[AccessRequest].to[List].transact(xa).map(_.toSeq)

  override def listExpiredTemporary(now: Instant): IO[Seq[AccessRequest]] =
    sql"""SELECT id, requester_id, resource_name, access_type, state, reason,
                 approved_by, created_at, updated_at, expires_at
          FROM access_requests
          WHERE state = ${RequestState.Approved: RequestState}
            AND expires_at IS NOT NULL
            AND expires_at < $now
          ORDER BY expires_at ASC"""
      .query[AccessRequest].to[List].transact(xa).map(_.toSeq)

  private def encodeAccessType(at: AccessType): String = at match {
    case AccessType.TemporaryAccess(ttl) => s"temporary:$ttl"
    case AccessType.PermanentAccess      => "permanent"
    case AccessType.ExportRequest        => "export"
  }

  private def decodeAccessType(s: String): AccessType =
    if (s.startsWith("temporary:")) {
      val ttl = s.stripPrefix("temporary:").toInt
      AccessType.TemporaryAccess(ttl)
    } else s match {
      case "permanent" => AccessType.PermanentAccess
      case "export"    => AccessType.ExportRequest
      case other       =>
        throw new IllegalArgumentException(s"Unknown access type: $other")
    }
}
