package io.gbmm.udps.storage.txn

import cats.effect.{IO, Ref}
import com.typesafe.scalalogging.LazyLogging

import java.time.{Duration, Instant}
import java.util.UUID

sealed trait TransactionStatus
object TransactionStatus {
  case object Active extends TransactionStatus
  case object Preparing extends TransactionStatus
  case object Committed extends TransactionStatus
  case object Aborted extends TransactionStatus
  case object RolledBack extends TransactionStatus
}

final case class TransactionEntry(
  transactionId: UUID,
  startedAt: Instant,
  committedAt: Option[Instant],
  status: TransactionStatus,
  affectedPartitions: Set[String],
  bufferedWrites: Seq[BufferedWrite],
  snapshotId: Option[Long],
  timeoutAt: Instant
)

final case class BufferedWrite(
  tableName: String,
  partitionKey: String,
  dataPath: String,
  rowCount: Long,
  sizeBytes: Long
)

/**
 * In-memory, thread-safe transaction log backed by cats-effect Ref.
 *
 * @param timeout transaction timeout duration (default 5 minutes)
 */
final class TransactionLog private (
  state: Ref[IO, Map[UUID, TransactionEntry]],
  timeout: Duration
) extends LazyLogging {

  def beginTransaction(): IO[TransactionEntry] = IO.realTimeInstant.flatMap { now =>
    val txnId = UUID.randomUUID()
    val entry = TransactionEntry(
      transactionId = txnId,
      startedAt = now,
      committedAt = None,
      status = TransactionStatus.Active,
      affectedPartitions = Set.empty,
      bufferedWrites = Seq.empty,
      snapshotId = Some(now.toEpochMilli),
      timeoutAt = now.plus(timeout)
    )
    state.update(_.updated(txnId, entry)).as {
      logger.info(s"Transaction started: id=$txnId, timeoutAt=${entry.timeoutAt}")
      entry
    }
  }

  def updateStatus(txnId: UUID, status: TransactionStatus): IO[Unit] =
    state.modify { m =>
      m.get(txnId) match {
        case Some(entry) =>
          val committedAt = status match {
            case TransactionStatus.Committed => Some(Instant.now())
            case _                           => entry.committedAt
          }
          val updated = entry.copy(status = status, committedAt = committedAt)
          (m.updated(txnId, updated), IO.unit)
        case None =>
          (m, IO.raiseError(new NoSuchElementException(s"Transaction not found: $txnId")))
      }
    }.flatten

  def addBufferedWrite(txnId: UUID, write: BufferedWrite): IO[Unit] =
    state.modify { m =>
      m.get(txnId) match {
        case Some(entry) =>
          val updated = entry.copy(
            bufferedWrites = entry.bufferedWrites :+ write,
            affectedPartitions = entry.affectedPartitions + s"${write.tableName}.${write.partitionKey}"
          )
          (m.updated(txnId, updated), IO.unit)
        case None =>
          (m, IO.raiseError(new NoSuchElementException(s"Transaction not found: $txnId")))
      }
    }.flatten

  def getTransaction(txnId: UUID): IO[Option[TransactionEntry]] =
    state.get.map(_.get(txnId))

  def getActiveTransactions: IO[Seq[TransactionEntry]] =
    state.get.map(_.values.filter(_.status == TransactionStatus.Active).toSeq)

  def cleanupExpired(): IO[Int] = IO.realTimeInstant.flatMap { now =>
    state.modify { m =>
      val expired = m.values.filter { e =>
        e.status == TransactionStatus.Active && now.isAfter(e.timeoutAt)
      }
      val updated = expired.foldLeft(m) { (acc, e) =>
        acc.updated(e.transactionId, e.copy(status = TransactionStatus.Aborted))
      }
      val count = expired.size
      (updated, IO(count).flatTap { c =>
        IO.whenA(c > 0)(IO(logger.warn(s"Cleaned up $c expired transactions")))
      })
    }.flatten
  }
}

object TransactionLog {

  private val DefaultTimeout: Duration = Duration.ofMinutes(5)

  def create(timeout: Duration = DefaultTimeout): IO[TransactionLog] =
    Ref.of[IO, Map[UUID, TransactionEntry]](Map.empty).map { ref =>
      new TransactionLog(ref, timeout)
    }
}
