package io.gbmm.udps.governance.access

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging

/** Records a state transition in the workflow audit trail. */
final case class WorkflowTransition(
    from: RequestState,
    to: RequestState,
    actor: String,
    timestamp: Instant,
    comment: Option[String]
)

/** State machine engine for access request workflows.
  *
  * Valid transitions:
  *   Submitted -> PendingApproval
  *   PendingApproval -> Approved | Rejected
  *   Approved -> Revoked | Expired
  */
final class WorkflowEngine(store: AccessRequestStore) extends LazyLogging {

  private val validTransitions: Map[RequestState, Set[RequestState]] = Map(
    RequestState.Submitted       -> Set(RequestState.PendingApproval),
    RequestState.PendingApproval -> Set(RequestState.Approved, RequestState.Rejected),
    RequestState.Approved        -> Set(RequestState.Revoked, RequestState.Expired)
  )

  def submit(
      requesterId: String,
      resourceName: String,
      accessType: AccessType,
      reason: String
  ): IO[AccessRequest] =
    for {
      _   <- validateNonEmpty("requesterId", requesterId)
      _   <- validateNonEmpty("resourceName", resourceName)
      _   <- validateNonEmpty("reason", reason)
      id  <- IO.delay(UUID.randomUUID())
      now <- IO.delay(Instant.now())
      request = AccessRequest(
        id = id,
        requesterId = requesterId,
        resourceName = resourceName,
        accessType = accessType,
        state = RequestState.Submitted,
        reason = reason,
        approvedBy = None,
        createdAt = now,
        updatedAt = now,
        expiresAt = None
      )
      _ <- store.insert(request)
      _ <- IO.delay(logger.info(s"Access request submitted: id=$id requester=$requesterId"))
      transitioned <- transition(request, RequestState.PendingApproval, requesterId, None)
    } yield transitioned

  def approve(requestId: UUID, approverId: String, comment: Option[String]): IO[AccessRequest] =
    for {
      _       <- validateNonEmpty("approverId", approverId)
      request <- getRequestOrFail(requestId)
      _       <- IO.raiseWhen(request.requesterId == approverId)(
                   new IllegalArgumentException("Approver cannot be the requester")
                 )
      now     <- IO.delay(Instant.now())
      expires  = computeExpiry(request.accessType, now)
      updated  = request.copy(
                   state = RequestState.Approved,
                   approvedBy = Some(approverId),
                   updatedAt = now,
                   expiresAt = expires
                 )
      _       <- validateTransition(request.state, RequestState.Approved)
      _       <- store.update(updated)
      _       <- IO.delay(logger.info(
                   s"Access request approved: id=$requestId approver=$approverId"
                 ))
    } yield updated

  def reject(requestId: UUID, approverId: String, comment: Option[String]): IO[AccessRequest] =
    for {
      _       <- validateNonEmpty("approverId", approverId)
      request <- getRequestOrFail(requestId)
      _       <- validateTransition(request.state, RequestState.Rejected)
      now     <- IO.delay(Instant.now())
      updated  = request.copy(
                   state = RequestState.Rejected,
                   approvedBy = Some(approverId),
                   updatedAt = now
                 )
      _       <- store.update(updated)
      _       <- IO.delay(logger.info(
                   s"Access request rejected: id=$requestId approver=$approverId"
                 ))
    } yield updated

  def revoke(requestId: UUID, revokerId: String): IO[AccessRequest] =
    for {
      _       <- validateNonEmpty("revokerId", revokerId)
      request <- getRequestOrFail(requestId)
      _       <- validateTransition(request.state, RequestState.Revoked)
      now     <- IO.delay(Instant.now())
      updated  = request.copy(
                   state = RequestState.Revoked,
                   updatedAt = now
                 )
      _       <- store.update(updated)
      _       <- IO.delay(logger.info(
                   s"Access request revoked: id=$requestId revoker=$revokerId"
                 ))
    } yield updated

  def expireStale: IO[Int] =
    for {
      now     <- IO.delay(Instant.now())
      expired <- store.listExpiredTemporary(now)
      _       <- expired.toList.traverse_ { request =>
                   val updated = request.copy(
                     state = RequestState.Expired,
                     updatedAt = now
                   )
                   store.update(updated)
                     .handleErrorWith { e =>
                       IO.delay(logger.error(
                         s"Failed to expire access request ${request.id}", e
                       ))
                     }
                 }
      _       <- IO.delay(logger.info(s"Expired ${expired.size} stale access requests"))
    } yield expired.size

  def getRequest(requestId: UUID): IO[Option[AccessRequest]] =
    store.findById(requestId)

  def listByRequester(requesterId: String): IO[Seq[AccessRequest]] =
    for {
      _       <- validateNonEmpty("requesterId", requesterId)
      results <- store.listByRequester(requesterId)
    } yield results

  def listPendingApprovals: IO[Seq[AccessRequest]] =
    store.listByState(RequestState.PendingApproval)

  private def getRequestOrFail(id: UUID): IO[AccessRequest] =
    store.findById(id).flatMap {
      case Some(r) => IO.pure(r)
      case None    => IO.raiseError(
        new NoSuchElementException(s"Access request not found: $id")
      )
    }

  private def validateTransition(from: RequestState, to: RequestState): IO[Unit] = {
    val allowed = validTransitions.getOrElse(from, Set.empty)
    IO.raiseUnless(allowed.contains(to))(
      new IllegalStateException(
        s"Invalid transition: ${RequestState.asString(from)} -> ${RequestState.asString(to)}"
      )
    )
  }

  private def transition(
      request: AccessRequest,
      target: RequestState,
      actor: String,
      comment: Option[String]
  ): IO[AccessRequest] =
    for {
      _   <- validateTransition(request.state, target)
      now <- IO.delay(Instant.now())
      updated = request.copy(state = target, updatedAt = now)
      _ <- store.update(updated)
    } yield updated

  private val secondsPerMinute = 60L

  private def computeExpiry(accessType: AccessType, now: Instant): Option[Instant] =
    accessType match {
      case AccessType.TemporaryAccess(ttlMinutes) =>
        Some(now.plusSeconds(ttlMinutes.toLong * secondsPerMinute))
      case AccessType.PermanentAccess => None
      case AccessType.ExportRequest   => None
    }

  private def validateNonEmpty(fieldName: String, value: String): IO[Unit] =
    IO.raiseWhen(value == null || value.trim.isEmpty)(
      new IllegalArgumentException(s"$fieldName must not be null or empty")
    )
}
