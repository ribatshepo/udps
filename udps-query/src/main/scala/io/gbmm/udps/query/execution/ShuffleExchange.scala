package io.gbmm.udps.query.execution

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.util.Using

/** Strategy for distributing data across partitions during a shuffle exchange. */
sealed trait ShuffleStrategy extends Product with Serializable

object ShuffleStrategy {

  /** Hash-partition data by the given key columns into the specified number of partitions. */
  final case class HashPartition(keys: Seq[String], numPartitions: Int) extends ShuffleStrategy

  /** Broadcast the full dataset to every partition (for small dimension tables). */
  case object Broadcast extends ShuffleStrategy

  /** Distribute rows evenly in round-robin fashion. */
  final case class RoundRobin(numPartitions: Int) extends ShuffleStrategy

  /** Collect all data into a single partition (partition 0). */
  case object SinglePartition extends ShuffleStrategy
}

/** Metrics collected during a shuffle exchange. */
final case class ShuffleMetrics(
  inputRowCount: Long,
  outputPartitionSizes: Map[Int, Long],
  serializedBytesTotal: Long
)

/** Manages data exchange between execution stages by partitioning rows
  * according to a specified shuffle strategy.
  *
  * This component is used by the coordinator to redistribute data between
  * stages that require repartitioning (e.g. before a hash join or aggregate).
  */
final class ShuffleExchange extends LazyLogging {

  /** Partition input rows according to the given strategy.
    *
    * @param data     input rows to partition
    * @param strategy how to distribute the rows
    * @return partition ID to rows mapping
    */
  def exchange(
    data: Seq[Map[String, Any]],
    strategy: ShuffleStrategy
  ): IO[Map[Int, Seq[Map[String, Any]]]] =
    IO {
      strategy match {
        case ShuffleStrategy.HashPartition(keys, numPartitions) =>
          hashPartition(data, keys, numPartitions)

        case ShuffleStrategy.Broadcast =>
          broadcastPartition(data)

        case ShuffleStrategy.RoundRobin(numPartitions) =>
          roundRobinPartition(data, numPartitions)

        case ShuffleStrategy.SinglePartition =>
          singlePartition(data)
      }
    }

  /** Partition rows and collect shuffle metrics.
    *
    * @param data     input rows to partition
    * @param strategy how to distribute the rows
    * @return a tuple of (partitioned data, metrics)
    */
  def exchangeWithMetrics(
    data: Seq[Map[String, Any]],
    strategy: ShuffleStrategy
  ): IO[(Map[Int, Seq[Map[String, Any]]], ShuffleMetrics)] =
    for {
      partitioned <- exchange(data, strategy)
      serializedSize <- estimateSerializedSize(partitioned)
      metrics = ShuffleMetrics(
        inputRowCount = data.size.toLong,
        outputPartitionSizes = partitioned.map { case (pid, rows) =>
          pid -> rows.size.toLong
        },
        serializedBytesTotal = serializedSize
      )
      _ <- IO(logger.debug(
        "Shuffle exchange: {} input rows -> {} partitions, ~{} bytes",
        data.size.toString,
        partitioned.size.toString,
        serializedSize.toString
      ))
    } yield (partitioned, metrics)

  /** Serialize partition data to bytes for network transfer.
    *
    * @param partitionData rows in a single partition
    * @return serialized byte array
    */
  def serializePartition(partitionData: Seq[Map[String, Any]]): IO[Array[Byte]] =
    IO {
      val bos = new ByteArrayOutputStream()
      Using.resource(new ObjectOutputStream(bos)) { oos =>
        oos.writeInt(partitionData.size)
        partitionData.foreach { row =>
          oos.writeObject(new java.util.HashMap[String, Any](
            scala.jdk.CollectionConverters.MapHasAsJava(row).asJava
          ))
        }
      }
      bos.toByteArray
    }

  /** Deserialize partition data from bytes received over the network.
    *
    * @param bytes serialized partition data
    * @return deserialized rows
    */
  def deserializePartition(bytes: Array[Byte]): IO[Seq[Map[String, Any]]] =
    IO {
      val bis = new ByteArrayInputStream(bytes)
      Using.resource(new ObjectInputStream(bis)) { ois =>
        val size = ois.readInt()
        val rows = new scala.collection.mutable.ArrayBuffer[Map[String, Any]](size)
        var i = 0
        while (i < size) {
          val javaMap = ois.readObject().asInstanceOf[java.util.HashMap[String, Any]]
          rows += scala.jdk.CollectionConverters.MapHasAsScala(javaMap).asScala.toMap
          i += 1
        }
        rows.toSeq
      }
    }

  private def hashPartition(
    data: Seq[Map[String, Any]],
    keys: Seq[String],
    numPartitions: Int
  ): Map[Int, Seq[Map[String, Any]]] = {
    if (numPartitions <= 0) {
      throw new IllegalArgumentException(
        s"Number of partitions must be positive, got $numPartitions"
      )
    }
    val partitions = Array.fill(numPartitions)(
      scala.collection.mutable.ArrayBuffer.empty[Map[String, Any]]
    )

    data.foreach { row =>
      val keyHash = murmurHash(keys.map(k => row.getOrElse(k, null)))
      val partition = math.abs(keyHash % numPartitions)
      partitions(partition) += row
    }

    partitions.zipWithIndex
      .map { case (buf, idx) => idx -> buf.toSeq }
      .filter { case (_, rows) => rows.nonEmpty }
      .toMap
  }

  private def broadcastPartition(
    data: Seq[Map[String, Any]]
  ): Map[Int, Seq[Map[String, Any]]] =
    Map(0 -> data)

  private def roundRobinPartition(
    data: Seq[Map[String, Any]],
    numPartitions: Int
  ): Map[Int, Seq[Map[String, Any]]] = {
    if (numPartitions <= 0) {
      throw new IllegalArgumentException(
        s"Number of partitions must be positive, got $numPartitions"
      )
    }
    val partitions = Array.fill(numPartitions)(
      scala.collection.mutable.ArrayBuffer.empty[Map[String, Any]]
    )

    data.zipWithIndex.foreach { case (row, idx) =>
      partitions(idx % numPartitions) += row
    }

    partitions.zipWithIndex
      .map { case (buf, idx) => idx -> buf.toSeq }
      .filter { case (_, rows) => rows.nonEmpty }
      .toMap
  }

  private def singlePartition(
    data: Seq[Map[String, Any]]
  ): Map[Int, Seq[Map[String, Any]]] =
    if (data.isEmpty) Map.empty
    else Map(0 -> data)

  /** Murmur-hash inspired hash for partition key values.
    *
    * Uses a FNV-1a style mixing with murmur-like finalization to produce
    * well-distributed partition assignments.
    */
  private def murmurHash(values: Seq[Any]): Int = {
    val FNV_OFFSET_BASIS = 0x811c9dc5
    val FNV_PRIME = 0x01000193

    var hash = FNV_OFFSET_BASIS
    values.foreach { value =>
      val h = if (value == null) 0 else value.hashCode()
      hash ^= h
      hash *= FNV_PRIME
    }

    // Murmur-like finalization
    hash ^= hash >>> 16
    hash *= 0x85ebca6b
    hash ^= hash >>> 13
    hash *= 0xc2b2ae35
    hash ^= hash >>> 16
    hash
  }

  private def estimateSerializedSize(
    partitioned: Map[Int, Seq[Map[String, Any]]]
  ): IO[Long] =
    IO {
      val OVERHEAD_PER_ROW_BYTES = 64L
      val OVERHEAD_PER_VALUE_BYTES = 32L
      partitioned.values.foldLeft(0L) { (acc, rows) =>
        acc + rows.foldLeft(0L) { (rowAcc, row) =>
          rowAcc + OVERHEAD_PER_ROW_BYTES + row.size * OVERHEAD_PER_VALUE_BYTES +
            row.values.foldLeft(0L) { (valAcc, v) =>
              valAcc + estimateValueSize(v)
            }
        }
      }
    }

  private def estimateValueSize(value: Any): Long =
    value match {
      case null          => 0L
      case _: Boolean    => 1L
      case _: Byte       => 1L
      case _: Short      => 2L
      case _: Int        => 4L
      case _: Long       => 8L
      case _: Float      => 4L
      case _: Double     => 8L
      case s: String     => s.length.toLong * 2L
      case a: Array[_]   => a.length.toLong * 8L
      case _             => 16L
    }
}

object ShuffleExchange {

  /** Create a new ShuffleExchange instance. */
  def apply(): ShuffleExchange = new ShuffleExchange()
}
