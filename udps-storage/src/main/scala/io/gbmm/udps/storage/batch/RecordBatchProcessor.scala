package io.gbmm.udps.storage.batch

import cats.effect.{IO, Resource}
import com.typesafe.scalalogging.LazyLogging
import fs2.{Pipe, Stream}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.VectorSchemaRoot

import java.nio.file.Path

/**
 * Trait defining the interface for reading Parquet files into Arrow batches.
 * Implementations are provided by the parquet module (UDPS-009).
 */
trait ParquetBatchReader {

  /**
   * Return the total number of row groups in the Parquet file.
   */
  def rowGroupCount(path: Path): IO[Int]

  /**
   * Read a single row group from a Parquet file into a VectorSchemaRoot.
   * The caller owns the returned VectorSchemaRoot and must close it.
   *
   * @param path         path to the Parquet file
   * @param rowGroupIndex zero-based row group index
   * @param allocator    Arrow buffer allocator to use
   * @return the batch as a VectorSchemaRoot
   */
  def readRowGroup(path: Path, rowGroupIndex: Int, allocator: BufferAllocator): IO[VectorSchemaRoot]
}

/**
 * Trait defining the interface for writing Arrow batches to Parquet files.
 * Implementations are provided by the parquet module (UDPS-010).
 */
trait ParquetBatchWriter {

  /**
   * Initialize a new Parquet file for writing. Must be called before
   * writeBatch. Returns a writer handle resource that will finalize
   * the file on close.
   */
  def open(path: Path, schema: org.apache.arrow.vector.types.pojo.Schema): Resource[IO, ParquetWriteHandle]

  /**
   * Write a single batch to the file.
   */
  def writeBatch(handle: ParquetWriteHandle, batch: VectorSchemaRoot): IO[Unit]
}

/**
 * Opaque handle representing an open Parquet file being written.
 * Concrete implementation lives in the parquet module.
 */
trait ParquetWriteHandle extends AutoCloseable

/**
 * A streaming batch processor that reads Parquet files as fs2 Streams
 * of Arrow VectorSchemaRoot, applies vectorized transformations, and
 * writes results back to Parquet.
 *
 * Back-pressure is handled naturally by fs2. Resource management for
 * the Arrow allocator is handled via cats-effect Resource/bracket.
 */
final class RecordBatchProcessor(
  reader: ParquetBatchReader,
  writer: ParquetBatchWriter,
  allocatorCapacity: Long
) extends LazyLogging {

  /**
   * Create a managed RootAllocator resource that is closed when the
   * stream or resource scope completes.
   */
  private[batch] def managedAllocator: Resource[IO, BufferAllocator] =
    Resource.make(
      IO {
        val alloc = new RootAllocator(allocatorCapacity)
        alloc.asInstanceOf[BufferAllocator]
      }
    )(alloc => IO(alloc.close()))

  /**
   * Create a child allocator scoped to a single batch operation.
   */
  private[batch] def childAllocator(
    parent: BufferAllocator,
    name: String
  ): Resource[IO, BufferAllocator] =
    Resource.make(
      IO(parent.newChildAllocator(name, 0, parent.getLimit))
    )(child => IO(child.close()))

  // -------------------------------------------------------------------------
  // Stream: read a Parquet file as a stream of Arrow batches
  // -------------------------------------------------------------------------

  /**
   * Read a Parquet file as an fs2 Stream of VectorSchemaRoot batches.
   * Each element in the stream corresponds to one row group from the
   * Parquet file. Batches are bracketed so that each VectorSchemaRoot
   * is closed after downstream processing completes for that element.
   *
   * @param path      path to the Parquet file
   * @param allocator Arrow allocator to use for reading
   * @return a stream of VectorSchemaRoot, each representing one row group
   */
  def stream(
    path: Path,
    allocator: BufferAllocator
  ): Stream[IO, VectorSchemaRoot] = {
    Stream.eval(reader.rowGroupCount(path)).flatMap { totalGroups =>
      logger.info("Opening Parquet file {} with {} row groups", path, totalGroups.toString)
      Stream.range(0, totalGroups).evalMap { groupIdx =>
        reader.readRowGroup(path, groupIdx, allocator)
      }
    }
  }

  /**
   * Overload that manages its own allocator lifetime via Resource.
   */
  def stream(path: Path): Stream[IO, VectorSchemaRoot] =
    Stream.resource(managedAllocator).flatMap(alloc => stream(path, alloc))

  // -------------------------------------------------------------------------
  // Transform: apply a chain of transformations to each batch
  // -------------------------------------------------------------------------

  /**
   * An fs2 Pipe that applies a sequence of transformation functions to each
   * VectorSchemaRoot batch flowing through the stream.
   *
   * Each transformation receives the current batch and allocator, and returns
   * a new batch. The previous intermediate batch is closed after each step
   * to avoid memory leaks.
   *
   * @param transforms sequence of (VectorSchemaRoot, BufferAllocator) => VectorSchemaRoot
   * @param allocator  Arrow allocator for new batch allocations
   * @return an fs2 Pipe transforming VectorSchemaRoot elements
   */
  def transform(
    transforms: Seq[(VectorSchemaRoot, BufferAllocator) => VectorSchemaRoot],
    allocator: BufferAllocator
  ): Pipe[IO, VectorSchemaRoot, VectorSchemaRoot] = { inStream =>
    if (transforms.isEmpty) {
      inStream
    } else {
      inStream.evalMap { batch =>
        IO {
          var current = batch
          var stepIndex = 0
          transforms.foreach { fn =>
            val next = fn(current, allocator)
            // Close the intermediate batch (but not the original if it is the first step,
            // because the caller may still need a reference — however, since we produce
            // a new VectorSchemaRoot we should close intermediates)
            if (stepIndex > 0) {
              current.close()
            }
            current = next
            stepIndex += 1
          }
          // Close the original input batch since we have produced a new output
          if (transforms.nonEmpty) {
            batch.close()
          }
          current
        }
      }
    }
  }

  // -------------------------------------------------------------------------
  // Write: write a stream of batches to a Parquet file
  // -------------------------------------------------------------------------

  /**
   * An fs2 Pipe that writes all VectorSchemaRoot batches to a Parquet file.
   * The first batch's schema is used to initialize the Parquet writer.
   * The writer resource is held open for the lifetime of the stream.
   *
   * @param path path to the output Parquet file
   * @return an fs2 Pipe that consumes VectorSchemaRoot batches and emits Unit
   */
  def write(path: Path): Pipe[IO, VectorSchemaRoot, Unit] = { inStream =>
    // Peek at the first batch to get the schema, then process all batches
    // within the writer resource scope
    inStream.pull.uncons1.flatMap {
      case None =>
        fs2.Pull.done
      case Some((firstBatch, rest)) =>
        val schema = firstBatch.getSchema
        val allBatches = Stream.emit(firstBatch) ++ rest
        val writeStream = Stream.resource(writer.open(path, schema)).flatMap { handle =>
          logger.info("Initializing Parquet writer for {}", path)
          allBatches.evalMap { batch =>
            writer.writeBatch(handle, batch) *>
              IO(logger.debug("Wrote batch with {} rows", batch.getRowCount.toString))
          }
        }
        writeStream.pull.echo
    }.stream
  }

  /**
   * Writes a stream of batches to a Parquet file, properly managing the
   * writer resource lifecycle. Convenience method wrapping the write Pipe.
   *
   * @param path        output Parquet file path
   * @param batchStream the stream of batches to write
   * @return IO that completes when all batches are written
   */
  def writeAll(path: Path, batchStream: Stream[IO, VectorSchemaRoot]): IO[Unit] =
    batchStream.through(write(path)).compile.drain

  // -------------------------------------------------------------------------
  // Pipeline: compose read -> transform -> write
  // -------------------------------------------------------------------------

  /**
   * Full pipeline: read a Parquet file, apply transformations, write output.
   *
   * @param inputPath   source Parquet file
   * @param outputPath  destination Parquet file
   * @param transforms  transformation chain to apply to each batch
   * @return IO that completes when the full pipeline has run
   */
  def pipeline(
    inputPath: Path,
    outputPath: Path,
    transforms: Seq[(VectorSchemaRoot, BufferAllocator) => VectorSchemaRoot]
  ): IO[Unit] = {
    managedAllocator.use { allocator =>
      val batchStream = stream(inputPath, allocator)
        .through(transform(transforms, allocator))
      writeAll(outputPath, batchStream)
    }
  }

  /**
   * Pipeline variant that produces a stream of transformed batches
   * without writing, useful for query execution.
   *
   * @param inputPath  source Parquet file
   * @param transforms transformation chain to apply to each batch
   * @param allocator  Arrow allocator
   * @return stream of transformed VectorSchemaRoot batches
   */
  def pipelineStream(
    inputPath: Path,
    transforms: Seq[(VectorSchemaRoot, BufferAllocator) => VectorSchemaRoot],
    allocator: BufferAllocator
  ): Stream[IO, VectorSchemaRoot] =
    stream(inputPath, allocator).through(transform(transforms, allocator))
}

object RecordBatchProcessor {

  private val DefaultAllocatorCapacity: Long = 1073741824L // 1 GiB

  /**
   * Create a RecordBatchProcessor as a cats-effect Resource, ensuring
   * proper cleanup of internal resources.
   */
  def resource(
    reader: ParquetBatchReader,
    writer: ParquetBatchWriter,
    allocatorCapacity: Long = DefaultAllocatorCapacity
  ): Resource[IO, RecordBatchProcessor] =
    Resource.pure(new RecordBatchProcessor(reader, writer, allocatorCapacity))

  /**
   * Convenience constructors for common transformation functions that
   * can be passed to `pipeline` or `transform`.
   */
  object TransformFunctions {

    /** Create a filter transformation function. */
    def filterFn(predicate: Int => Boolean): (VectorSchemaRoot, BufferAllocator) => VectorSchemaRoot =
      (root, alloc) => Transformations.filter(root, predicate, alloc)

    /** Create a projection transformation function. */
    def projectFn(columns: Seq[String]): (VectorSchemaRoot, BufferAllocator) => VectorSchemaRoot =
      (root, alloc) => Transformations.project(root, columns, alloc)

    /** Create a sort transformation function. */
    def sortFn(sortColumns: Seq[Transformations.SortColumn]): (VectorSchemaRoot, BufferAllocator) => VectorSchemaRoot =
      (root, alloc) => Transformations.sort(root, sortColumns, alloc)

    /** Create an aggregation transformation function. */
    def aggregateFn(
      aggregations: Seq[Transformations.Aggregation]
    ): (VectorSchemaRoot, BufferAllocator) => VectorSchemaRoot =
      (root, alloc) => Transformations.aggregate(root, aggregations, alloc)
  }
}
