package io.gbmm.udps.catalog.discovery

import io.gbmm.udps.core.domain.DataType

sealed trait SchemaChangeEvent extends Product with Serializable {
  def tableName: String
}

object SchemaChangeEvent {
  final case class SchemaAdded(table: DiscoveredTable) extends SchemaChangeEvent {
    override def tableName: String = table.name
  }

  final case class SchemaRemoved(tableName: String) extends SchemaChangeEvent

  final case class SchemaModified(
    tableName: String,
    changes: Seq[ColumnChange]
  ) extends SchemaChangeEvent
}

sealed trait ColumnChange extends Product with Serializable {
  def columnName: String
}

object ColumnChange {
  final case class ColumnAdded(column: DiscoveredColumn) extends ColumnChange {
    override def columnName: String = column.name
  }

  final case class ColumnRemoved(columnName: String) extends ColumnChange

  final case class ColumnTypeChanged(
    columnName: String,
    oldType: DataType,
    newType: DataType
  ) extends ColumnChange

  final case class NullabilityChanged(
    columnName: String,
    wasNullable: Boolean,
    isNullable: Boolean
  ) extends ColumnChange
}

object ChangeDetector {

  def detectChanges(
    previous: DiscoveryResult,
    current: DiscoveryResult
  ): Seq[SchemaChangeEvent] = {
    val previousByName = previous.tables.map(t => t.name -> t).toMap
    val currentByName = current.tables.map(t => t.name -> t).toMap

    val addedTables = currentByName.keySet.diff(previousByName.keySet).toSeq.sorted.map { name =>
      SchemaChangeEvent.SchemaAdded(currentByName(name))
    }

    val removedTables = previousByName.keySet.diff(currentByName.keySet).toSeq.sorted.map { name =>
      SchemaChangeEvent.SchemaRemoved(name)
    }

    val commonTables = previousByName.keySet.intersect(currentByName.keySet)
    val modifiedTables = commonTables.toSeq.sorted.flatMap { name =>
      val changes = detectColumnChanges(previousByName(name), currentByName(name))
      if (changes.nonEmpty) Seq(SchemaChangeEvent.SchemaModified(name, changes))
      else Seq.empty
    }

    addedTables ++ removedTables ++ modifiedTables
  }

  private def detectColumnChanges(
    previous: DiscoveredTable,
    current: DiscoveredTable
  ): Seq[ColumnChange] = {
    val prevByName = previous.columns.map(c => c.name -> c).toMap
    val currByName = current.columns.map(c => c.name -> c).toMap

    val added = currByName.keySet.diff(prevByName.keySet).toSeq.sorted.map { name =>
      ColumnChange.ColumnAdded(currByName(name))
    }

    val removed = prevByName.keySet.diff(currByName.keySet).toSeq.sorted.map { name =>
      ColumnChange.ColumnRemoved(name)
    }

    val common = prevByName.keySet.intersect(currByName.keySet)
    val typeChanges = common.toSeq.sorted.flatMap { name =>
      val prev = prevByName(name)
      val curr = currByName(name)
      val changes = Seq.newBuilder[ColumnChange]

      if (prev.dataType != curr.dataType) {
        changes += ColumnChange.ColumnTypeChanged(name, prev.dataType, curr.dataType)
      }
      if (prev.nullable != curr.nullable) {
        changes += ColumnChange.NullabilityChanged(name, prev.nullable, curr.nullable)
      }

      changes.result()
    }

    added ++ removed ++ typeChanges
  }
}
