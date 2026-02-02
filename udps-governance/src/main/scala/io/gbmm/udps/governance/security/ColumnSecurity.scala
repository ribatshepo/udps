package io.gbmm.udps.governance.security

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import io.circe._

sealed trait AccessLevel extends Product with Serializable

object AccessLevel {
  case object FullAccess extends AccessLevel
  case object MaskedAccess extends AccessLevel
  case object NoAccess extends AccessLevel

  private val levelMap: Map[String, AccessLevel] = Map(
    "FullAccess" -> FullAccess,
    "MaskedAccess" -> MaskedAccess,
    "NoAccess" -> NoAccess
  )

  def asString(level: AccessLevel): String = level match {
    case FullAccess   => "FullAccess"
    case MaskedAccess => "MaskedAccess"
    case NoAccess     => "NoAccess"
  }

  def fromString(s: String): AccessLevel =
    levelMap.getOrElse(s, throw new IllegalArgumentException(s"Unknown AccessLevel: $s"))

  implicit val putAccessLevel: Put[AccessLevel] =
    Put[String].contramap(asString)
  implicit val getAccessLevel: Get[AccessLevel] =
    Get[String].map(fromString)

  implicit val encodeAccessLevel: Encoder[AccessLevel] =
    Encoder.encodeString.contramap[AccessLevel](asString)

  implicit val decodeAccessLevel: Decoder[AccessLevel] = Decoder.decodeString.emap { s =>
    levelMap.get(s).toRight(s"Unknown AccessLevel: $s")
  }

  /** Ordering: FullAccess > MaskedAccess > NoAccess. Higher value = more permissive. */
  def permissiveness(level: AccessLevel): Int = level match {
    case FullAccess   => 2
    case MaskedAccess => 1
    case NoAccess     => 0
  }
}

final case class ColumnPolicy(
    id: UUID,
    tableName: String,
    columnName: String,
    roleOrUser: String,
    accessLevel: AccessLevel,
    maskingTechnique: Option[String],
    createdAt: Instant
)

trait ColumnPolicyStore {
  def create(policy: ColumnPolicy): IO[Unit]
  def get(tableName: String, columnName: String, roleOrUser: String): IO[Option[ColumnPolicy]]
  def listForTable(tableName: String): IO[Seq[ColumnPolicy]]
  def listForUser(roleOrUser: String): IO[Seq[ColumnPolicy]]
  def update(policy: ColumnPolicy): IO[Unit]
  def delete(id: UUID): IO[Unit]
}

class DoobieColumnPolicyStore(xa: Transactor[IO]) extends ColumnPolicyStore with LazyLogging {

  override def create(policy: ColumnPolicy): IO[Unit] =
    sql"""INSERT INTO column_policies (id, table_name, column_name, role_or_user, access_level, masking_technique, created_at)
          VALUES (${policy.id}, ${policy.tableName}, ${policy.columnName}, ${policy.roleOrUser},
                  ${policy.accessLevel}, ${policy.maskingTechnique}, ${policy.createdAt})"""
      .update.run.transact(xa).void

  override def get(tableName: String, columnName: String, roleOrUser: String): IO[Option[ColumnPolicy]] =
    sql"""SELECT id, table_name, column_name, role_or_user, access_level, masking_technique, created_at
          FROM column_policies WHERE table_name = $tableName AND column_name = $columnName AND role_or_user = $roleOrUser"""
      .query[ColumnPolicy].option.transact(xa)

  override def listForTable(tableName: String): IO[Seq[ColumnPolicy]] =
    sql"""SELECT id, table_name, column_name, role_or_user, access_level, masking_technique, created_at
          FROM column_policies WHERE table_name = $tableName ORDER BY column_name, role_or_user"""
      .query[ColumnPolicy].to[List].transact(xa).map(_.toSeq)

  override def listForUser(roleOrUser: String): IO[Seq[ColumnPolicy]] =
    sql"""SELECT id, table_name, column_name, role_or_user, access_level, masking_technique, created_at
          FROM column_policies WHERE role_or_user = $roleOrUser ORDER BY table_name, column_name"""
      .query[ColumnPolicy].to[List].transact(xa).map(_.toSeq)

  override def update(policy: ColumnPolicy): IO[Unit] =
    sql"""UPDATE column_policies SET table_name = ${policy.tableName}, column_name = ${policy.columnName},
          role_or_user = ${policy.roleOrUser}, access_level = ${policy.accessLevel},
          masking_technique = ${policy.maskingTechnique} WHERE id = ${policy.id}"""
      .update.run.transact(xa).void

  override def delete(id: UUID): IO[Unit] =
    sql"""DELETE FROM column_policies WHERE id = $id"""
      .update.run.transact(xa).void
}
