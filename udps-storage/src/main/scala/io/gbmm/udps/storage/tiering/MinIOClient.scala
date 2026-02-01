package io.gbmm.udps.storage.tiering

import cats.effect.{IO, Resource}
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.core.config.MinioConfig
import io.minio._

import java.io.InputStream
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

/** Wrapper around the MinIO Java SDK providing cats-effect IO operations. */
final class MinIOClient private (client: MinioClient, bucket: String) extends LazyLogging {

  private val PresignedUrlExpirySeconds: Int = 3600

  /** Upload a local file to MinIO under the given object key. */
  def upload(localPath: Path, objectKey: String): IO[Unit] =
    IO.blocking {
      logger.info("Uploading {} to bucket={} key={}", localPath, bucket, objectKey)
      client.uploadObject(
        UploadObjectArgs.builder()
          .bucket(bucket)
          .`object`(objectKey)
          .filename(localPath.toString)
          .build()
      )
    }.void

  /** Download an object from MinIO to the given local path. */
  def download(objectKey: String, localPath: Path): IO[Unit] =
    IO.blocking {
      logger.info("Downloading bucket={} key={} to {}", bucket, objectKey, localPath)
      Option(localPath.getParent).foreach(parent => Files.createDirectories(parent))
      client.downloadObject(
        DownloadObjectArgs.builder()
          .bucket(bucket)
          .`object`(objectKey)
          .filename(localPath.toString)
          .build()
      )
    }.void

  /** Delete an object from MinIO. */
  def delete(objectKey: String): IO[Unit] =
    IO.blocking {
      logger.info("Deleting bucket={} key={}", bucket, objectKey)
      client.removeObject(
        RemoveObjectArgs.builder()
          .bucket(bucket)
          .`object`(objectKey)
          .build()
      )
    }.void

  /** Generate a presigned GET URL valid for 1 hour. */
  def presignedGetUrl(objectKey: String): IO[String] =
    IO.blocking {
      client.getPresignedObjectUrl(
        GetPresignedObjectUrlArgs.builder()
          .method(io.minio.http.Method.GET)
          .bucket(bucket)
          .`object`(objectKey)
          .expiry(PresignedUrlExpirySeconds, TimeUnit.SECONDS)
          .build()
      )
    }

  /** List object keys in the bucket matching the given prefix. */
  def listObjects(prefix: String): IO[List[String]] =
    IO.blocking {
      val results = client.listObjects(
        ListObjectsArgs.builder()
          .bucket(bucket)
          .prefix(prefix)
          .recursive(true)
          .build()
      )
      results.asScala.toList.map(_.get().objectName())
    }

  /** Check whether an object exists in the bucket. */
  def exists(objectKey: String): IO[Boolean] =
    IO.blocking {
      try {
        client.statObject(
          StatObjectArgs.builder()
            .bucket(bucket)
            .`object`(objectKey)
            .build()
        )
        true
      } catch {
        case _: io.minio.errors.ErrorResponseException => false
      }
    }

  /** Get an InputStream for the object as a cats-effect Resource. */
  def getObjectStream(objectKey: String): Resource[IO, InputStream] =
    Resource.make(
      IO.blocking {
        client.getObject(
          GetObjectArgs.builder()
            .bucket(bucket)
            .`object`(objectKey)
            .build()
        )
      }
    )(stream => IO.blocking(stream.close()))
}

object MinIOClient extends LazyLogging {

  /** Build a MinIOClient as a Resource, ensuring the target bucket exists. */
  def resource(config: MinioConfig): Resource[IO, MinIOClient] =
    Resource.make(
      IO.blocking {
        val client = MinioClient.builder()
          .endpoint(config.endpoint)
          .credentials(config.accessKey, config.secretKey)
          .build()

        ensureBucketExists(client, config.bucket)
        new MinIOClient(client, config.bucket)
      }
    )(_ => IO.unit)

  private def ensureBucketExists(client: MinioClient, bucket: String): Unit = {
    val bucketExists = client.bucketExists(
      BucketExistsArgs.builder().bucket(bucket).build()
    )
    if (!bucketExists) {
      logger.info("Creating bucket={}", bucket)
      client.makeBucket(
        MakeBucketArgs.builder().bucket(bucket).build()
      )
    }
  }
}
