package io.gbmm.udps.catalog.discovery

import io.gbmm.udps.core.domain.DataType

sealed trait CompatibilityLevel extends Product with Serializable

object CompatibilityLevel {
  case object FullyCompatible extends CompatibilityLevel
  case object BackwardCompatible extends CompatibilityLevel
  case object Incompatible extends CompatibilityLevel
}

final case class CompatibilityIssue(
  columnName: String,
  changeType: String,
  description: String,
  level: CompatibilityLevel
)

final case class EvolutionValidation(
  issues: Seq[CompatibilityIssue],
  overallLevel: CompatibilityLevel
)

object SchemaEvolution {

  def validate(changes: Seq[SchemaChangeEvent]): EvolutionValidation = {
    val issues = changes.flatMap(validateEvent)
    val overallLevel = if (issues.isEmpty) {
      CompatibilityLevel.FullyCompatible
    } else {
      val hasIncompatible = issues.exists(_.level == CompatibilityLevel.Incompatible)
      if (hasIncompatible) CompatibilityLevel.Incompatible
      else CompatibilityLevel.BackwardCompatible
    }
    EvolutionValidation(issues = issues, overallLevel = overallLevel)
  }

  private def validateEvent(event: SchemaChangeEvent): Seq[CompatibilityIssue] =
    event match {
      case SchemaChangeEvent.SchemaAdded(_) =>
        Seq.empty
      case SchemaChangeEvent.SchemaRemoved(tableName) =>
        Seq(CompatibilityIssue(
          columnName = "*",
          changeType = "table_removed",
          description = s"Table '$tableName' was removed",
          level = CompatibilityLevel.Incompatible
        ))
      case SchemaChangeEvent.SchemaModified(_, columnChanges) =>
        columnChanges.flatMap(validateColumnChange)
    }

  private def validateColumnChange(change: ColumnChange): Seq[CompatibilityIssue] =
    change match {
      case ColumnChange.ColumnAdded(col) =>
        if (col.nullable) {
          Seq(CompatibilityIssue(
            columnName = col.name,
            changeType = "column_added_nullable",
            description = s"Nullable column '${col.name}' added",
            level = CompatibilityLevel.FullyCompatible
          ))
        } else {
          Seq(CompatibilityIssue(
            columnName = col.name,
            changeType = "column_added_required",
            description = s"Required column '${col.name}' added - existing rows will not have this value",
            level = CompatibilityLevel.Incompatible
          ))
        }

      case ColumnChange.ColumnRemoved(name) =>
        Seq(CompatibilityIssue(
          columnName = name,
          changeType = "column_removed",
          description = s"Column '$name' was removed",
          level = CompatibilityLevel.Incompatible
        ))

      case ColumnChange.ColumnTypeChanged(name, oldType, newType) =>
        if (isWideningConversion(oldType, newType)) {
          Seq(CompatibilityIssue(
            columnName = name,
            changeType = "type_widened",
            description = s"Column '$name' type widened from $oldType to $newType",
            level = CompatibilityLevel.BackwardCompatible
          ))
        } else {
          Seq(CompatibilityIssue(
            columnName = name,
            changeType = "type_narrowed",
            description = s"Column '$name' type changed from $oldType to $newType (potentially incompatible)",
            level = CompatibilityLevel.Incompatible
          ))
        }

      case ColumnChange.NullabilityChanged(name, wasNullable, isNullable) =>
        if (!wasNullable && isNullable) {
          Seq(CompatibilityIssue(
            columnName = name,
            changeType = "became_nullable",
            description = s"Column '$name' became nullable",
            level = CompatibilityLevel.BackwardCompatible
          ))
        } else {
          Seq(CompatibilityIssue(
            columnName = name,
            changeType = "became_required",
            description = s"Column '$name' became required",
            level = CompatibilityLevel.Incompatible
          ))
        }
    }

  private val typeWidthOrder: Map[DataType, Int] = Map(
    DataType.Int8 -> 1,
    DataType.Int16 -> 2,
    DataType.Int32 -> 3,
    DataType.Int64 -> 4,
    DataType.UInt8 -> 1,
    DataType.UInt16 -> 2,
    DataType.UInt32 -> 3,
    DataType.UInt64 -> 4,
    DataType.Float32 -> 5,
    DataType.Float64 -> 6,
    DataType.Date32 -> 10,
    DataType.Date64 -> 11,
    DataType.TimestampSec -> 20,
    DataType.TimestampMillis -> 21,
    DataType.TimestampMicros -> 22,
    DataType.TimestampNanos -> 23
  )

  private def isWideningConversion(from: DataType, to: DataType): Boolean =
    (from, to) match {
      case (a, b) if a == b => true
      case (DataType.Decimal(p1, s1), DataType.Decimal(p2, s2)) =>
        p2 >= p1 && s2 >= s1
      case (a, b) =>
        (typeWidthOrder.get(a), typeWidthOrder.get(b)) match {
          case (Some(wa), Some(wb)) => wb >= wa && sameTypeFamily(a, b)
          case _ => false
        }
    }

  private val intTypes: Set[DataType] = Set(DataType.Int8, DataType.Int16, DataType.Int32, DataType.Int64)
  private val uintTypes: Set[DataType] = Set(DataType.UInt8, DataType.UInt16, DataType.UInt32, DataType.UInt64)
  private val floatTypes: Set[DataType] = Set(DataType.Float32, DataType.Float64)
  private val dateTypes: Set[DataType] = Set(DataType.Date32, DataType.Date64)
  private val tsTypes: Set[DataType] = Set(DataType.TimestampSec, DataType.TimestampMillis, DataType.TimestampMicros, DataType.TimestampNanos)

  private val typeFamilies: Seq[Set[DataType]] = Seq(intTypes, uintTypes, floatTypes, dateTypes, tsTypes)

  private def sameTypeFamily(a: DataType, b: DataType): Boolean =
    typeFamilies.exists(family => family.contains(a) && family.contains(b))
}
