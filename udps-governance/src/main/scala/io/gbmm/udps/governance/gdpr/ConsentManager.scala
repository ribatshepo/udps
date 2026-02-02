package io.gbmm.udps.governance.gdpr

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

/** A record of consent granted by a data subject for a specific purpose. */
final case class ConsentRecord(
    id: UUID,
    subjectId: String,
    purpose: String,
    legalBasis: String,
    granted: Boolean,
    grantedAt: Instant,
    expiresAt: Option[Instant]
)

/** Manages consent lifecycle — granting, revoking, checking, and listing. */
final class ConsentManager(xa: Transactor[IO]) extends LazyLogging {

  def grantConsent(
      subjectId: String,
      purpose: String,
      legalBasis: String,
      expiresAt: Option[Instant]
  ): IO[ConsentRecord] =
    for {
      _ <- validateNonEmpty("subjectId", subjectId)
      _ <- validateNonEmpty("purpose", purpose)
      _ <- validateNonEmpty("legalBasis", legalBasis)
      id <- IO.delay(UUID.randomUUID())
      now <- IO.delay(Instant.now())
      record = ConsentRecord(id, subjectId, purpose, legalBasis, granted = true, now, expiresAt)
      _ <- revokeExistingConsent(subjectId, purpose)
      _ <- insertConsent(record)
      _ <- IO.delay(logger.info(s"Consent granted for subject=$subjectId purpose=$purpose"))
    } yield record

  def revokeConsent(subjectId: String, purpose: String): IO[Unit] =
    for {
      _ <- validateNonEmpty("subjectId", subjectId)
      _ <- validateNonEmpty("purpose", purpose)
      _ <- revokeExistingConsent(subjectId, purpose)
      _ <- IO.delay(logger.info(s"Consent revoked for subject=$subjectId purpose=$purpose"))
    } yield ()

  def checkConsent(subjectId: String, purpose: String): IO[Boolean] =
    for {
      _ <- validateNonEmpty("subjectId", subjectId)
      _ <- validateNonEmpty("purpose", purpose)
      now <- IO.delay(Instant.now())
      result <- sql"""SELECT COUNT(*) FROM consent_records
                      WHERE subject_id = $subjectId
                        AND purpose = $purpose
                        AND granted = true
                        AND (expires_at IS NULL OR expires_at > $now)"""
                 .query[Long].unique.transact(xa)
    } yield result > 0

  def listConsents(subjectId: String): IO[Seq[ConsentRecord]] =
    for {
      _       <- validateNonEmpty("subjectId", subjectId)
      records <- sql"""SELECT id, subject_id, purpose, legal_basis, granted, granted_at, expires_at
                       FROM consent_records
                       WHERE subject_id = $subjectId
                       ORDER BY granted_at DESC"""
                  .query[ConsentRecord].to[List].transact(xa)
    } yield records.toSeq

  private def insertConsent(record: ConsentRecord): IO[Unit] =
    sql"""INSERT INTO consent_records (id, subject_id, purpose, legal_basis, granted, granted_at, expires_at)
          VALUES (${record.id}, ${record.subjectId}, ${record.purpose}, ${record.legalBasis},
                  ${record.granted}, ${record.grantedAt}, ${record.expiresAt})"""
      .update.run.transact(xa).void

  private def revokeExistingConsent(subjectId: String, purpose: String): IO[Unit] =
    sql"""UPDATE consent_records SET granted = false
          WHERE subject_id = $subjectId AND purpose = $purpose AND granted = true"""
      .update.run.transact(xa).void

  private def validateNonEmpty(fieldName: String, value: String): IO[Unit] =
    IO.raiseWhen(value == null || value.trim.isEmpty)(
      new IllegalArgumentException(s"$fieldName must not be null or empty")
    )
}
